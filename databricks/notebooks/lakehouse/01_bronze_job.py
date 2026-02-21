# Databricks notebook source
# ------------------------------------
# 01_bronze_job.py
#
# Notebook-Entry fuer Bronze Ingest im Databricks Job.
# Nutzt die gleichen Kernmodule wie lokale Runs.
# ------------------------------------

from databricks.jobs.bronze_ingest.run_bronze_ingest import run_bronze_ingest
from pathlib import Path

bronze_dir = Path("data/bronze")
lakehouse_bronze_dir = Path("data/lakehouse/bronze")

summary = run_bronze_ingest(bronze_dir, lakehouse_bronze_dir)
print(summary)
