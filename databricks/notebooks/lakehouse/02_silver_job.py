# Databricks notebook source
# ------------------------------------
# 02_silver_job.py
#
# Notebook-Entry fuer Silver Sync im Databricks Job.
# ------------------------------------

from databricks.jobs.silver_sync.run_silver_sync import run_silver_sync
from pathlib import Path

lakehouse_bronze_dir = Path("data/lakehouse/bronze")
lakehouse_silver_dir = Path("data/lakehouse/silver")

summary = run_silver_sync(lakehouse_bronze_dir, lakehouse_silver_dir)
print(summary)
