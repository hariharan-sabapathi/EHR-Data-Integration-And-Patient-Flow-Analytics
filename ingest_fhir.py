"""
ingest_fhir.py

PySpark job that reads FHIR R4 NDJSON bulk exports, flattens nested
structs and arrays using explode() / select(), and writes Parquet files
for downstream warehouse loading.

Handles five resource types:
    Patient, Encounter, Condition, Observation, EpisodeOfCare

Usage:
    spark-submit ingest_fhir.py --src data/raw --out data/parquet

    # or plain python if running locally without spark-submit:
    python ingest_fhir.py --src data/raw --out data/parquet
"""

import argparse
import logging
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def build_spark(app_name: str = "fhir_ingest") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Extraction functions — one per FHIR resource type
# ---------------------------------------------------------------------------

def extract_patients(spark: SparkSession, src: Path) -> DataFrame:
    raw = spark.read.json(str(src / "Patient_000.ndjson"))

    return raw.select(
        F.col("id").alias("patient_id"),
        F.col("gender"),
        F.col("birthDate").alias("birth_date"),
        F.col("name").getItem(0).getField("family").alias("name_family"),
        F.col("name").getItem(0).getField("given").getItem(0).alias("name_given"),
        F.col("address").getItem(0).getField("city").alias("city"),
        F.col("address").getItem(0).getField("state").alias("state"),
        F.col("address").getItem(0).getField("country").alias("country"),
        F.col("maritalStatus.text").alias("marital_status"),
    )


def extract_encounters(spark: SparkSession, src: Path) -> DataFrame:
    raw = spark.read.json(str(src / "Encounter_000.ndjson"))

    # type is array<struct<coding:array, text:string>> — explode then dedup
    exploded = raw.withColumn("type_entry", F.explode_outer(F.col("type")))

    return (
        exploded.select(
            F.col("id").alias("encounter_id"),
            F.regexp_replace(F.col("subject.reference"), "^Patient/", "").alias("patient_id"),
            F.col("status"),
            F.col("class.code").alias("encounter_class"),
            F.col("type_entry").getField("text").alias("encounter_type"),
            F.col("type_entry").getField("coding").getItem(0).getField("code")
                .alias("encounter_snomed_code"),
            F.col("episodeOfCare").getItem(0).getField("reference").alias("episode_of_care_ref"),
            F.to_timestamp(F.col("period.start")).alias("admit_timestamp"),
            F.to_timestamp(F.col("period.end")).alias("discharge_timestamp"),
            F.round(
                (F.unix_timestamp(F.to_timestamp(F.col("period.end"))) -
                 F.unix_timestamp(F.to_timestamp(F.col("period.start")))) / 3600.0, 2
            ).alias("los_hours"),
            F.col("serviceProvider.display").alias("service_provider"),
            F.col("location").getItem(0).getField("location").getField("display")
                .alias("location_display"),
            F.col("reasonCode").getItem(0).getField("coding").getItem(0).getField("display")
                .alias("reason_for_visit"),
        )
        .dropDuplicates(["encounter_id"])
    )


def extract_conditions(spark: SparkSession, src: Path) -> DataFrame:
    raw = spark.read.json(str(src / "Condition_000.ndjson"))

    # code.coding is an array — explode to get one row per coding system
    exploded = raw.withColumn("coding_entry", F.explode_outer(F.col("code.coding")))

    return (
        exploded.select(
            F.col("id").alias("condition_id"),
            F.regexp_replace(F.col("subject.reference"), "^Patient/", "").alias("patient_id"),
            F.regexp_replace(F.col("encounter.reference"), "^Encounter/", "").alias("encounter_id"),
            F.col("coding_entry.system").alias("coding_system"),
            F.col("coding_entry.code").alias("diagnosis_code"),
            F.col("coding_entry.display").alias("diagnosis_description"),
            F.col("clinicalStatus.coding").getItem(0).getField("code").alias("clinical_status"),
            F.col("verificationStatus.coding").getItem(0).getField("code")
                .alias("verification_status"),
            F.col("category").getItem(0).getField("coding").getItem(0).getField("display")
                .alias("category"),
            F.to_timestamp(F.col("onsetDateTime")).alias("onset_date"),
            F.to_timestamp(F.col("abatementDateTime")).alias("abatement_date"),
            F.to_date(F.col("recordedDate")).alias("recorded_date"),
            F.col("abatementDateTime").isNotNull().alias("is_resolved"),
        )
        .dropDuplicates(["condition_id", "coding_system"])
    )


def extract_observations(spark: SparkSession, src: Path) -> DataFrame:
    obs_paths = [
        str(src / "Observation_000.ndjson"),
        str(src / "Observation_001.ndjson"),
    ]
    raw = spark.read.json(obs_paths)

    # code.coding is an array — explode then filter to LOINC only
    exploded = raw.withColumn("coding_entry", F.explode_outer(F.col("code.coding")))
    loinc_only = exploded.filter(F.col("coding_entry.system").contains("loinc.org"))

    return (
        loinc_only.select(
            F.col("id").alias("observation_id"),
            F.regexp_replace(F.col("subject.reference"), "^Patient/", "").alias("patient_id"),
            F.regexp_replace(F.col("encounter.reference"), "^Encounter/", "").alias("encounter_id"),
            F.col("status"),
            F.col("coding_entry.code").alias("loinc_code"),
            F.col("coding_entry.display").alias("observation_name"),
            F.col("category").getItem(0).getField("coding").getItem(0).getField("code")
                .alias("obs_category"),
            F.to_timestamp(F.col("effectiveDateTime")).alias("effective_datetime"),
            F.col("valueQuantity.value").cast(DoubleType()).alias("result_value"),
            F.col("valueQuantity.unit").alias("result_unit"),
            F.col("valueCodeableConcept.text").alias("result_text"),
        )
        .dropDuplicates(["observation_id", "loinc_code"])
    )


def extract_episodes(spark: SparkSession, src: Path) -> DataFrame:
    raw = spark.read.json(str(src / "EpisodeOfCare_000.ndjson"))

    return raw.select(
        F.col("id").alias("episode_id"),
        F.regexp_replace(F.col("patient.reference"), "^Patient/", "").alias("patient_id"),
        F.col("status").alias("episode_status"),
        F.col("type").getItem(0).getField("coding").getItem(0).getField("display")
            .alias("episode_type"),
        F.to_timestamp(F.col("period.start")).alias("episode_start"),
        F.to_timestamp(F.col("period.end")).alias("episode_end"),
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Flatten FHIR R4 NDJSON exports to Parquet using PySpark."
    )
    parser.add_argument("--src", default="data/raw",    help="Directory containing NDJSON files")
    parser.add_argument("--out", default="data/parquet", help="Output directory for Parquet files")
    args = parser.parse_args()

    src = Path(args.src)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")

    extractions: list[tuple[str, DataFrame]] = [
        ("dim_patients",     extract_patients(spark, src)),
        ("fact_encounters",  extract_encounters(spark, src)),
        ("dim_diagnoses",    extract_conditions(spark, src)),
        ("dim_observations", extract_observations(spark, src)),
        ("dim_episodes",     extract_episodes(spark, src)),
    ]

    for name, df in extractions:
        output_path = str(out / name)
        df.write.mode("overwrite").parquet(output_path)
        count = spark.read.parquet(output_path).count()
        log.info("%-20s  %7s rows  →  %s", name, f"{count:,}", output_path)

    spark.stop()
    log.info("Parquet files written to %s", out)


if __name__ == "__main__":
    main()
