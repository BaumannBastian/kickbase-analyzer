# ------------------------------------
# Makefile
#
# Optionales Kommando-Interface fuer lokale Standardaufgaben.
# ------------------------------------

PYTHON ?= python3

.PHONY: test lint format run-demo-ingestion run-private-ingestion run-scheduler

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
