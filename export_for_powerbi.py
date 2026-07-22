"""
export_for_powerbi.py

Exports dbt analytical tables from the local DuckDB warehouse to CSV
for import into Power BI Desktop during local development.

When connected to Snowflake, Power BI imports directly — this script
is only needed for the local simulation mode.

Usage:
    python export_for_powerbi.py --db data/ehr_warehouse.duckdb --out data/powerbi
"""

import argparse
import logging
from pathlib import Path

import duckdb

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

TABLES = [
    "Fact_Patient_Encounters",
    "Dim_Patients",
    "Dim_Diagnoses",
    "Dim_Clinical_Observations",
    "Dim_LOINC_Reference",
    "analytics_readmissions",
    "analytics_throughput",
    "analytics_lab_kpis",
    "mart_los_kpis",
]


def main() -> None:
    parser = argparse.ArgumentParser(description="Export warehouse tables to CSV for Power BI.")
    parser.add_argument("--db",  default="data/ehr_warehouse.duckdb", help="DuckDB path")
    parser.add_argument("--out", default="data/powerbi", help="Output directory for CSVs")
    args = parser.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(args.db, read_only=True)

    for table in TABLES:
        csv_path = out / f"{table.lower()}.csv"
        con.execute(f"COPY (SELECT * FROM {table}) TO '{csv_path}' (HEADER, DELIMITER ',')")
        n = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        log.info("%-35s  %7s rows  →  %s", table, f"{n:,}", csv_path.name)

    con.close()
    log.info("CSVs written to %s — import into Power BI via Get Data → Text/CSV", out)


if __name__ == "__main__":
    main()
