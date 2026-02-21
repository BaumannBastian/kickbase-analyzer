# ------------------------------------
# Makefile
#
# Optionales Kommando-Interface fuer lokale Standardaufgaben.
# ------------------------------------

PYTHON ?= python3

.PHONY: test lint format run-demo-ingestion run-private-ingestion run-scheduler run-kickbase-auth-check run-ligainsider-scrape-check run-databricks-jobs-demo run-build-marts-local run-pipeline-demo run-backtesting install-gcloud-cli configure-gcloud-auth check-bq-setup run-bq-raw-export run-bq-load run-bq-views run-bq-pipeline run-powerbi-api

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

run-kickbase-auth-check:
	./scripts/run_kickbase_auth_check.sh --env-file .env --verify-snapshots

run-ligainsider-scrape-check:
	./scripts/run_ligainsider_scrape_check.sh --env-file .env

run-databricks-jobs-demo:
	./scripts/run_databricks_jobs_demo.sh

run-build-marts-local:
	./scripts/run_build_marts_local.sh

run-pipeline-demo:
	./scripts/run_pipeline_demo.sh

run-backtesting:
	./scripts/run_backtesting.sh

install-gcloud-cli:
	./scripts/bigquery/install_gcloud_cli_wsl.sh

configure-gcloud-auth:
	./scripts/bigquery/configure_gcloud_auth.sh --project $(BQ_PROJECT_ID)

check-bq-setup:
	./scripts/bigquery/check_bq_setup.sh

run-bq-raw-export:
	./scripts/bigquery/run_prepare_raw_exports.sh

run-bq-load:
	./scripts/bigquery/run_load_raw_bq.sh

run-bq-views:
	./scripts/bigquery/run_apply_views_bq.sh

run-bq-pipeline:
	./scripts/bigquery/run_bigquery_pipeline.sh

run-powerbi-api:
	./scripts/powerbi/run_powerbi_api.sh list-workspaces
