# Databricks notebook source
# ------------------------------------
# 01_bronze_job.py
#
# Notebook-Entry fuer Bronze Ingest im Databricks Job.
# Nutzt die gleichen Kernmodule wie lokale Runs.
#
# Usage
# ------------------------------------
# - Databricks Job Task: notebook_path auf dieses Notebook setzen
# ------------------------------------
# ------------------------------------

from datetime import UTC, datetime
from pathlib import Path

from databricks.jobs.bronze_ingest.run_bronze_ingest import REQUIRED_DATASETS, run_bronze_ingest
from databricks.jobs.common_io import latest_timestamp_common_flat
from local_ingestion.core.bronze_writer import run_demo_ingestion

bronze_dir = Path("data/bronze")
lakehouse_bronze_dir = Path("data/lakehouse/bronze")
demo_dir = Path("demo/data")

try:
    latest_timestamp_common_flat(bronze_dir, REQUIRED_DATASETS)
except FileNotFoundError:
    bootstrap = run_demo_ingestion(
        demo_dir,
        bronze_dir,
        now=datetime.now(UTC),
        source_version="demo-databricks",
    )
    print({"bootstrap": "demo_ingestion", "summary": bootstrap})

summary = run_bronze_ingest(bronze_dir, lakehouse_bronze_dir)
print(summary)
