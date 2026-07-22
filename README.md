# EHR Interoperability & Patient Flow Analytics Engine

End-to-end pipeline for clinical EHR data: FHIR R4 bulk ingestion via PySpark,
a Snowflake star schema, dbt analytics models, and a Power BI operational dashboard.

Developed against the `smart-on-fhir/sample-bulk-fhir-datasets` 100-patient Synthea export.

---

## Stack

| Layer | Tool | Purpose |
|---|---|---|
| Ingestion | PySpark 3+ | FHIR NDJSON parsing, schema flattening, array explosions |
| Warehouse | Snowflake (local: DuckDB) | Star schema — same DDL works on both |
| Transforms | dbt-core | Staging, LOS KPIs, readmissions, throughput, lab stats |
| Dashboard | Power BI Desktop | Five-page operational control tower |

---

## Project layout

```
.
├── ingest_fhir.py          # PySpark: NDJSON → Parquet
├── load_warehouse.py       # Parquet → Snowflake / DuckDB star schema
├── export_for_powerbi.py   # Exports CSVs for local Power BI testing
├── POWERBI_GUIDE.md        # Full Power BI build guide + DAX measures
├── README.md
├── dbt_models/
│   ├── dbt_project.yml
│   └── models/
│       ├── staging/
│       │   └── stg_encounters.sql
│       ├── marts/
│       │   └── mart_los_kpis.sql
│       └── analytics/
│           ├── analytics_readmissions.sql
│           ├── analytics_throughput.sql
│           └── analytics_lab_kpis.sql
└── data/
    ├── raw/                # NDJSON source files
    ├── parquet/            # PySpark output
    └── powerbi/            # CSV exports for Power BI local dev
```

---

## Setup

```bash
pip install pyspark duckdb dbt-core dbt-duckdb

# For Snowflake production:
pip install snowflake-connector-python dbt-snowflake
```

**dbt profile** (`~/.dbt/profiles.yml`):

```yaml
# Local development
ehr_duckdb:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: data/ehr_warehouse.duckdb

# Snowflake production
ehr_snowflake:
  target: prod
  outputs:
    prod:
      type: snowflake
      account: xy12345.us-east-1
      user: ehr_loader
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      database: EHR_DB
      warehouse: EHR_WH
      schema: ANALYTICS
```

---

## Running the pipeline

```bash
# 1. Ingest — flatten FHIR NDJSON to Parquet
spark-submit ingest_fhir.py --src data/raw --out data/parquet

# 2. Load — build the star schema
#    Local:
python load_warehouse.py --parquet data/parquet --mode local
#    Snowflake:
export SNOWFLAKE_ACCOUNT=... SNOWFLAKE_USER=... SNOWFLAKE_PASSWORD=...
export SNOWFLAKE_WAREHOUSE=EHR_WH SNOWFLAKE_DATABASE=EHR_DB SNOWFLAKE_SCHEMA=ANALYTICS
python load_warehouse.py --parquet data/parquet --mode snowflake

# 3. Transform — run dbt models
cd dbt_models && dbt run

# 4. Export CSVs for Power BI (local dev only)
python export_for_powerbi.py

# 5. Dashboard — follow POWERBI_GUIDE.md
```

---

## Data model

```
Fact_Patient_Encounters          (7,761 rows)
    ├── Dim_Patients             (120 rows)
    ├── Dim_Diagnoses            (4,294 rows — SNOMED CT coded)
    ├── Dim_Clinical_Observations(70,704 rows — LOINC coded)
    ├── Dim_Episodes             (7,761 rows — EpisodeOfCare linkage)
    └── Dim_LOINC_Reference      (159 codes — P2.5/P97.5 normal ranges)
```

---

## dbt models

| Model | Grain | Output |
|---|---|---|
| `stg_encounters` | Per encounter | Filters LOS outliers, adds shift / LOS tier / weekend flag |
| `mart_los_kpis` | Per encounter class | P25/P75/P90, outlier count, weekday vs weekend split |
| `analytics_readmissions` | Per encounter | LAG/LEAD over patient history — 7/30/90-day readmission flags |
| `analytics_throughput` | Per encounter per episode | Episode→encounter linkage, inter-encounter gap |
| `analytics_lab_kpis` | Per LOINC code | Population stats, abnormal rate |

---

## PySpark ingestion — key transformations

| Resource | Transformation |
|---|---|
| `Encounter` | `explode(type)` to flatten encounter type array; struct path `class.code` for AMB/IMP/EMER |
| `Condition` | `explode(code.coding)` to get one row per coding system (SNOMED, ICD-10) |
| `Observation` | `explode(code.coding)` then filter `system LIKE '%loinc%'` |
| All | `regexp_replace(subject.reference, '^Patient/', '')` to strip FHIR reference prefixes |

---

## Notes

**Readmission rate** is high (~65%) because the Synthea cohort covers decades of
longitudinal care. For a clinically valid denominator, filter `analytics_readmissions`
to `encounter_class = 'IMP'` index discharges only.

**SNOMED vs ICD-10** — Synthea encodes conditions in SNOMED CT. Apply a
SNOMED→ICD-10 crosswalk table if ICD-10 grouping is needed downstream.

**Snowflake COPY INTO** — for large-scale production loads, replace the DuckDB
`read_parquet()` inserts with Snowflake `COPY INTO` from a staged S3 location.
The DDL and all dbt SQL remain unchanged.
