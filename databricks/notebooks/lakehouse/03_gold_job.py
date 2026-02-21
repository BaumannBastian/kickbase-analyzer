# Databricks notebook source
# ------------------------------------
# 03_gold_job.py
#
# Notebook-Entry fuer Gold Feature Build im Databricks Job.
# ------------------------------------

from databricks.jobs.gold_features.run_gold_features import run_gold_features
from pathlib import Path

lakehouse_silver_dir = Path("data/lakehouse/silver")
lakehouse_gold_dir = Path("data/lakehouse/gold")

summary = run_gold_features(lakehouse_silver_dir, lakehouse_gold_dir)
print(summary)
