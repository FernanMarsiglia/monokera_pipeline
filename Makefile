PYTHON ?= python
PIP ?= $(PYTHON) -m pip
VENV ?= .venv

.PHONY: install test lint format upload-glue-scripts upload-dags airflow-up airflow-down airflow-logs airflow-init airflow-restart

install:
	$(PYTHON) -m venv $(VENV)
	$(VENV)/Scripts/pip install -r requirements.txt -r requirements-dev.txt || \
	$(VENV)/bin/pip install -r requirements.txt -r requirements-dev.txt

lint:
	$(VENV)/Scripts/ruff check src dags glue || $(VENV)/bin/ruff check src dags glue

format:
	$(VENV)/Scripts/black src dags glue || $(VENV)/bin/black src dags glue

test:
	$(VENV)/Scripts/pytest || $(VENV)/bin/pytest

# ===== AWS Deployment =====
upload-glue-scripts:
	aws s3 sync glue/scripts/ s3://<your-glue-scripts-bucket>/glue/scripts/

upload-dags:
	aws s3 sync dags/ s3://<your-airflow-dags-bucket>/dags/

# ===== Local Airflow =====
airflow-init:
	chmod +x local-setup.sh
	./local-setup.sh

airflow-up:
	docker compose up -d

airflow-down:
	docker compose down

airflow-restart:
	docker compose restart

airflow-logs:
	docker compose logs -f airflow-scheduler airflow-webserver

airflow-clean:
	docker compose down -v
	rm -rf logs/*
