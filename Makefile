VENV=.venv
PY=$(VENV)/bin/python
PIP=$(VENV)/bin/pip
DBT=$(VENV)/bin/dbt

.PHONY: setup deps seed build test demo_ai

setup:
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	$(PIP) install dbt-duckdb

deps:
	DBT_PROFILES_DIR=. $(DBT) deps

seed:
	DBT_PROFILES_DIR=. $(DBT) seed --full-refresh

build:
	DBT_PROFILES_DIR=. $(DBT) build

test:
	DBT_PROFILES_DIR=. $(DBT) test

demo_ai:
	$(PY) scripts/ai_query_runner.py
