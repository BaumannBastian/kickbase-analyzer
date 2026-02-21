# ------------------------------------
# Makefile
#
# Optionales Kommando-Interface fuer lokale Standardaufgaben.
# ------------------------------------

PYTHON ?= python3

.PHONY: test lint format run-demo-ingestion run-private-ingestion run-scheduler run-databricks-jobs-demo run-build-marts-local run-pipeline-demo

test:
	./scripts/test.sh

lint:
	./scripts/lint.sh

format:
	@echo "No formatter configured yet. Add ruff format or black once dev deps are installed."

run-demo-ingestion:
	./scripts/run_demo_ingestion.sh

run-private-ingestion:
	./scripts/run_private_ingestion.sh

run-scheduler:
	./scripts/run_scheduler.sh

run-databricks-jobs-demo:
	./scripts/run_databricks_jobs_demo.sh

run-build-marts-local:
	./scripts/run_build_marts_local.sh

run-pipeline-demo:
	./scripts/run_pipeline_demo.sh
