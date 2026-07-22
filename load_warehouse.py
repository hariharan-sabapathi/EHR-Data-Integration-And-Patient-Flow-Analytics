"""
load_warehouse.py

Loads FHIR Parquet files into a Snowflake star schema.

SNOWFLAKE (production):
    Set environment variables and run with --mode snowflake:
        export SNOWFLAKE_ACCOUNT=xy12345.us-east-1
        export SNOWFLAKE_USER=ehr_loader
        export SNOWFLAKE_PASSWORD=...
        export SNOWFLAKE_WAREHOUSE=EHR_WH
        export SNOWFLAKE_DATABASE=EHR_DB
        export SNOWFLAKE_SCHEMA=ANALYTICS
        python load_warehouse.py --parquet data/parquet --mode snowflake

LOCAL (simulation, same DDL / SQL dialect):
    python load_warehouse.py --parquet data/parquet --mode local --db data/ehr_warehouse.duckdb

The DDL and all INSERT statements are identical between modes.
Switching to Snowflake requires only credentials — no SQL changes.
"""

import argparse
import logging
import os
from pathlib import Path
from typing import Protocol

import duckdb

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DDL  — Snowflake-compatible star schema
# Identical in both Snowflake and DuckDB (same SQL dialect).
# ---------------------------------------------------------------------------

DDL = """
CREATE TABLE IF NOT EXISTS Dim_Patients (
    patient_id     VARCHAR PRIMARY KEY,
    gender         VARCHAR,
    birth_date     VARCHAR,
    name_family    VARCHAR,
    name_given     VARCHAR,
    city           VARCHAR,
    state          VARCHAR,
    country        VARCHAR,
    marital_status VARCHAR
);

-- Cohort-derived normal ranges (P2.5 / P97.5).
-- In production, join to the official LOINC normal-ranges table instead.
CREATE TABLE IF NOT EXISTS Dim_LOINC_Reference (
    loinc_code       VARCHAR PRIMARY KEY,
    observation_name VARCHAR,
    obs_category     VARCHAR,
    result_unit      VARCHAR,
    normal_low       DOUBLE,
    normal_high      DOUBLE
);

CREATE TABLE IF NOT EXISTS Dim_Diagnoses (
    condition_id          VARCHAR,
    patient_id            VARCHAR,
    encounter_id          VARCHAR,
    coding_system         VARCHAR,
    diagnosis_code        VARCHAR,
    diagnosis_description VARCHAR,
    clinical_status       VARCHAR,
    verification_status   VARCHAR,
    category              VARCHAR,
    onset_date            TIMESTAMPTZ,
    abatement_date        TIMESTAMPTZ,
    recorded_date         DATE,
    is_resolved           BOOLEAN,
    PRIMARY KEY (condition_id, coding_system)
);

CREATE TABLE IF NOT EXISTS Dim_Clinical_Observations (
    observation_id     VARCHAR,
    patient_id         VARCHAR,
    encounter_id       VARCHAR,
    status             VARCHAR,
    loinc_code         VARCHAR,
    observation_name   VARCHAR,
    obs_category       VARCHAR,
    effective_datetime TIMESTAMPTZ,
    result_value       DOUBLE,
    result_unit        VARCHAR,
    result_text        VARCHAR,
    is_abnormal        BOOLEAN,
    PRIMARY KEY (observation_id, loinc_code)
);

CREATE TABLE IF NOT EXISTS Dim_Episodes (
    episode_id        VARCHAR PRIMARY KEY,
    patient_id        VARCHAR,
    episode_status    VARCHAR,
    episode_type      VARCHAR,
    episode_start     TIMESTAMPTZ,
    episode_end       TIMESTAMPTZ,
    episode_los_hours DOUBLE
);

CREATE TABLE IF NOT EXISTS Fact_Patient_Encounters (
    encounter_id          VARCHAR PRIMARY KEY,
    patient_id            VARCHAR REFERENCES Dim_Patients(patient_id),
    status                VARCHAR,
    encounter_class       VARCHAR,   -- AMB / IMP / EMER / HH / VR
    encounter_type        VARCHAR,
    encounter_snomed_code VARCHAR,
    episode_of_care_ref   VARCHAR,
    admit_timestamp       TIMESTAMPTZ,
    discharge_timestamp   TIMESTAMPTZ,
    los_hours             DOUBLE,
    los_days              DOUBLE,
    admit_year            INTEGER,
    admit_month           INTEGER,
    admit_hour            INTEGER,
    admit_dow             VARCHAR,
    service_provider      VARCHAR,
    location_display      VARCHAR,
    reason_for_visit      VARCHAR,
    is_inpatient          BOOLEAN,
    is_emergency          BOOLEAN,
    is_readmission_30d    BOOLEAN DEFAULT FALSE,
    days_since_last_enc   DOUBLE
);
"""

# ---------------------------------------------------------------------------
# Connection factory
# ---------------------------------------------------------------------------

def connect_local(db_path: Path) -> duckdb.DuckDBPyConnection:
    """DuckDB connection used for local simulation."""
    if db_path.exists():
        db_path.unlink()
    return duckdb.connect(str(db_path))


def connect_snowflake():
    """
    Snowflake connection using environment variables.

    Required env vars:
        SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
        SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA

    Returns a snowflake.connector connection with the same
    .execute() interface used throughout this script.
    """
    import snowflake.connector  # pip install snowflake-connector-python

    required = [
        "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA",
    ]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        raise EnvironmentError(f"Missing Snowflake env vars: {', '.join(missing)}")

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def row_count(con, table: str) -> int:
    return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def create_schema(con) -> None:
    for statement in DDL.strip().split(";"):
        stmt = statement.strip()
        if stmt:
            con.execute(stmt)
    log.info("schema ready (6 tables)")


def load_patients(con, parquet_dir: Path) -> None:
    src = parquet_dir / "dim_patients"
    con.execute(f"""
        INSERT INTO Dim_Patients
            (patient_id, gender, birth_date, name_family, name_given,
             city, state, country, marital_status)
        SELECT
            patient_id, gender, birth_date, name_family, name_given,
            city, state, country, marital_status
        FROM read_parquet('{src}/*.parquet')
    """)
    log.info("Dim_Patients               %7s rows", f"{row_count(con, 'Dim_Patients'):,}")


def load_loinc_reference(con, parquet_dir: Path) -> None:
    src = parquet_dir / "dim_observations"
    con.execute(f"""
        INSERT INTO Dim_LOINC_Reference
        SELECT
            loinc_code,
            MODE(observation_name)                                              AS observation_name,
            MODE(obs_category)                                                  AS obs_category,
            MODE(result_unit)                                                   AS result_unit,
            PERCENTILE_CONT(0.025) WITHIN GROUP (ORDER BY result_value)         AS normal_low,
            PERCENTILE_CONT(0.975) WITHIN GROUP (ORDER BY result_value)         AS normal_high
        FROM read_parquet('{src}/*.parquet')
        WHERE loinc_code IS NOT NULL
          AND result_value IS NOT NULL
        GROUP BY loinc_code
    """)
    log.info("Dim_LOINC_Reference        %7s codes", f"{row_count(con, 'Dim_LOINC_Reference'):,}")


def load_diagnoses(con, parquet_dir: Path) -> None:
    src = parquet_dir / "dim_diagnoses"
    con.execute(f"INSERT INTO Dim_Diagnoses SELECT * FROM read_parquet('{src}/*.parquet')")
    log.info("Dim_Diagnoses              %7s rows", f"{row_count(con, 'Dim_Diagnoses'):,}")


def load_observations(con, parquet_dir: Path) -> None:
    src = parquet_dir / "dim_observations"
    con.execute(f"""
        INSERT INTO Dim_Clinical_Observations
        SELECT
            o.observation_id,
            o.patient_id,
            o.encounter_id,
            o.status,
            o.loinc_code,
            o.observation_name,
            o.obs_category,
            o.effective_datetime,
            o.result_value,
            o.result_unit,
            o.result_text,
            CASE
                WHEN o.result_value IS NOT NULL
                 AND lr.normal_low  IS NOT NULL
                 AND lr.normal_high IS NOT NULL
                THEN o.result_value < lr.normal_low
                  OR o.result_value > lr.normal_high
                ELSE NULL
            END AS is_abnormal
        FROM read_parquet('{src}/*.parquet') o
        LEFT JOIN Dim_LOINC_Reference lr ON o.loinc_code = lr.loinc_code
    """)
    log.info("Dim_Clinical_Observations  %7s rows",
             f"{row_count(con, 'Dim_Clinical_Observations'):,}")


def load_episodes(con, parquet_dir: Path) -> None:
    src = parquet_dir / "dim_episodes"
    con.execute(f"""
        INSERT INTO Dim_Episodes
        SELECT
            episode_id, patient_id, episode_status, episode_type,
            episode_start, episode_end,
            ROUND(EPOCH(episode_end) - EPOCH(episode_start)) / 3600.0 AS episode_los_hours
        FROM read_parquet('{src}/*.parquet')
    """)
    log.info("Dim_Episodes               %7s rows", f"{row_count(con, 'Dim_Episodes'):,}")


def load_encounters(con, parquet_dir: Path) -> None:
    src = parquet_dir / "fact_encounters"
    con.execute(f"""
        INSERT INTO Fact_Patient_Encounters
        SELECT
            encounter_id,
            patient_id,
            status,
            encounter_class,
            encounter_type,
            encounter_snomed_code,
            episode_of_care_ref,
            admit_timestamp,
            discharge_timestamp,
            los_hours,
            ROUND(los_hours / 24.0, 2)                   AS los_days,
            EXTRACT(YEAR  FROM admit_timestamp)::INTEGER  AS admit_year,
            EXTRACT(MONTH FROM admit_timestamp)::INTEGER  AS admit_month,
            EXTRACT(HOUR  FROM admit_timestamp)::INTEGER  AS admit_hour,
            STRFTIME(admit_timestamp, '%A')               AS admit_dow,
            service_provider,
            location_display,
            reason_for_visit,
            encounter_class = 'IMP'                       AS is_inpatient,
            encounter_class = 'EMER'                      AS is_emergency,
            FALSE                                         AS is_readmission_30d,
            NULL::DOUBLE                                  AS days_since_last_enc
        FROM read_parquet('{src}/*.parquet')
        WHERE patient_id IN (SELECT patient_id FROM Dim_Patients)
    """)
    log.info("Fact_Patient_Encounters    %7s rows",
             f"{row_count(con, 'Fact_Patient_Encounters'):,}")


def validate(con) -> None:
    orphans = con.execute("""
        SELECT COUNT(*) FROM Fact_Patient_Encounters f
        WHERE NOT EXISTS (
            SELECT 1 FROM Dim_Patients p WHERE p.patient_id = f.patient_id
        )
    """).fetchone()[0]

    linked = con.execute("""
        SELECT COUNT(*) FROM Dim_Clinical_Observations o
        JOIN Fact_Patient_Encounters f ON o.encounter_id = f.encounter_id
    """).fetchone()[0]

    if orphans:
        log.warning("Referential integrity: %d orphaned encounters", orphans)
    else:
        log.info("Referential integrity: OK")
    log.info("Observations joined to encounters: %s", f"{linked:,}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load FHIR Parquet files into Snowflake (or local DuckDB) star schema."
    )
    parser.add_argument("--parquet", default="data/parquet",
                        help="Directory containing Parquet subdirectories from ingest_fhir.py")
    parser.add_argument("--mode",    default="local", choices=["local", "snowflake"],
                        help="'local' uses DuckDB; 'snowflake' uses env-var credentials")
    parser.add_argument("--db",      default="data/ehr_warehouse.duckdb",
                        help="DuckDB path (local mode only)")
    args = parser.parse_args()

    parquet_dir = Path(args.parquet)

    if args.mode == "snowflake":
        log.info("Connecting to Snowflake (%s)", os.environ.get("SNOWFLAKE_ACCOUNT", "?"))
        con = connect_snowflake()
    else:
        db_path = Path(args.db)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        log.info("Local mode — DuckDB at %s", db_path)
        con = connect_local(db_path)

    create_schema(con)
    load_patients(con, parquet_dir)
    load_loinc_reference(con, parquet_dir)
    load_diagnoses(con, parquet_dir)
    load_observations(con, parquet_dir)
    load_episodes(con, parquet_dir)
    load_encounters(con, parquet_dir)
    validate(con)
    con.close()

    log.info("Done.")


if __name__ == "__main__":
    main()
