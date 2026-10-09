# Developer entrypoints. `make help` lists targets. Settings come from .env when present.
-include .env
export

UV ?= uv
TF ?= terraform
MONTH ?= 2026-06
REPLAY_START ?= 2026-06-12T17:00
REPLAY_END ?= 2026-06-12T18:00
REPLAY_SAMPLE ?= 0.02
FIXTURE_DIR := data/fixture-run
FIXTURES := --data-dir $(FIXTURE_DIR) --source-dir tests/fixtures/tlc --weather-dir tests/fixtures/weather

.DEFAULT_GOAL := help
.PHONY: help install lint format test test-unit test-e2e test-integration fixtures-pipeline pipeline \
        ingest load stream load-events dq transform dbt-athena-compile verify report latest-month \
        tf-fmt tf-validate tf-lint checkov infra-check precommit clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

install: ## Create the virtualenv from uv.lock (dev tools included)
	$(UV) sync --frozen

lint: ## Ruff lint + format check
	$(UV) run ruff check .
	$(UV) run ruff format --check .

format: ## Auto-fix lint and format
	$(UV) run ruff check --fix .
	$(UV) run ruff format .

test-unit: ## Unit tests (moto in-process, no network)
	$(UV) run pytest tests/unit

test-e2e: ## End-to-end state machine + dbt on the committed real fixtures, plus rerun checks
	$(UV) run pytest tests/e2e

test-integration: ## Emulator endpoint tests (needs UBER_AWS_ENDPOINT_URL, e.g. LocalStack)
	$(UV) run pytest -m integration tests/integration

test: ## Unit + e2e (integration tests skip without an endpoint)
	$(UV) run pytest --cov

fixtures-pipeline: ## Whole batch on fixtures (into data/fixture-run), then verify + report
	$(UV) run uber-rides $(FIXTURES) batch --month 2026-06 --replay-start 2026-06-12T18:00 --replay-end 2026-06-12T18:01 --duplicate-every 7 --malformed-every 11 --replays 2
	$(UV) run uber-rides $(FIXTURES) verify --month 2026-06 --offline-dir tests/fixtures/published
	$(UV) run uber-rides $(FIXTURES) report --out site

pipeline: ## Real data: daily batch for MONTH with a sampled replay window (downloads ~500 MB)
	$(UV) run uber-rides batch --month $(MONTH) --replay-start $(REPLAY_START) --replay-end $(REPLAY_END) --replay-sample-rate $(REPLAY_SAMPLE) --duplicate-every 7 --malformed-every 11 --replays 2

latest-month: ## Newest month with a published HVFHV file
	$(UV) run uber-rides latest-month

ingest: ## Land MONTH (TLC file, zones, weather)
	$(UV) run uber-rides ingest --month $(MONTH)

load: ## Load MONTH into Iceberg (idempotent)
	$(UV) run uber-rides load --month $(MONTH)

stream: ## Replay REPLAY_START..REPLAY_END through emulated Kinesis -> Lambda -> DynamoDB
	$(UV) run uber-rides stream --start $(REPLAY_START) --end $(REPLAY_END) --sample-rate $(REPLAY_SAMPLE) --duplicate-every 7 --malformed-every 11

load-events: ## Landing zone -> Iceberg ride_events (incremental)
	$(UV) run uber-rides load-events

dq: ## All data-quality gates for MONTH (exit 3 on error-level failures)
	$(UV) run uber-rides dq --month $(MONTH)

transform: ## dbt build on DuckDB
	$(UV) run uber-rides transform build

dbt-athena-compile: ## Compile the dbt project for Athena (placeholder creds, no AWS calls)
	AWS_ACCESS_KEY_ID=testing AWS_SECRET_ACCESS_KEY=testing AWS_REGION=us-east-1 AWS_ENDPOINT_URL=http://127.0.0.1:9 \
	  $(UV) run --extra athena uber-rides transform --target athena compile --no-introspect --no-populate-cache

verify: ## Reconcile MONTH with TLC-published figures
	$(UV) run uber-rides verify --month $(MONTH)

report: ## Write the static results site to ./site
	$(UV) run uber-rides report --out site

tf-fmt: ## terraform fmt -check
	cd infra && $(TF) fmt -check -recursive

tf-validate: ## terraform init -backend=false + validate
	cd infra && $(TF) init -backend=false -input=false && $(TF) validate

tf-lint: ## tflint (terraform + aws rulesets)
	cd infra && tflint --init && tflint --recursive

checkov: ## checkov on infra/ (skips are justified inline)
	uvx --from checkov checkov -d infra --framework terraform --compact --quiet

infra-check: tf-fmt tf-validate tf-lint checkov ## All infrastructure checks

precommit: ## pre-commit on every file
	$(UV) run pre-commit run --all-files

clean: ## Remove caches and local build output (not data)
	rm -rf .pytest_cache .ruff_cache .coverage coverage.xml htmlcov site dbt/target dbt/logs infra/modules/streaming/.build
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
