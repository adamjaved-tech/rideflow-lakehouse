# RideFlow Lakehouse

An end-to-end data-engineering project: a ride-hailing **event pipeline** that ingests
micro-batches (file replay or Kafka), lands them in a **bronze / silver / gold** layout,
enforces **data-quality gates**, loads a **star-schema warehouse** with idempotent SQL
upserts, and records **run audits, health checks and analytics queries**.
All data is synthetic and generated deterministically - no credentials, no paid services.

![CI](https://github.com/adamjaved-tech/rideflow-lakehouse/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/Python-3.11%2B-blue)
![License: MIT](https://img.shields.io/badge/License-MIT-green)

**Author:** [Adam Javed](https://github.com/adamjaved-tech) · **Focus:** Data Engineering, Analytics Engineering, Data Quality, and Pipeline Reliability

## Features

* Deterministic generator with injected defects (duplicates, malformed JSON, bad/future timestamps, negative fares, unknown types, out-of-order delivery)
* Offset-checkpointed, idempotent, incremental bronze ingestion
* Validation + quarantine with reason codes and pointers back to the raw line
* Quality framework: critical vs warning checks, explicit PASS/FAIL, Markdown + JSON run reports, persisted in `dq_results`
* Versioned SQL migrations, staging tables, constraints, FKs, indexes, a view, upsert merges
* Analytics SQL (`sql/analytics/`) runnable via CLI against a read-only connection
* Structured JSON logging with run-id correlation and secret redaction
* Health check command (exit code 1 when unhealthy), SLA check, audit tables
* Restartable stages shared by the CLI and an Airflow DAG (retries, backoff, timeouts)
* Dockerfile, Compose (incl. optional Kafka profile), GitHub Actions CI

## Architecture

See [docs/architecture.md](docs/architecture.md) for the diagram, design decisions and failure modes.

```
source (file | Kafka) -> bronze (raw JSONL) -> silver (clean CSV) -> quality gate
      -> trips -> stg_trips -> dim_* / fact_trips (SQLite) -> quality checks -> report
```

## Tech stack

Python 3.11+, pandas, PyYAML, SQLite (stdlib), SQL, Airflow (DAG), Kafka (optional source),
Docker / Compose, GitHub Actions, pytest / unittest.

## Quick start

Prerequisites: Python 3.11+ and git.

**Linux / macOS**
```bash
git clone https://github.com/adamjaved-tech/rideflow-lakehouse.git && cd rideflow-lakehouse
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
pip install -e .            # provides the `rideflow` command (or use: export PYTHONPATH=src and `python -m rideflow`)
rideflow run
rideflow health
rideflow query revenue_by_city
```

**Windows (PowerShell)**
```powershell
git clone https://github.com/adamjaved-tech/rideflow-lakehouse.git; cd rideflow-lakehouse
python -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt
pip install -e .
rideflow run
rideflow health
rideflow query revenue_by_city
```
Without installing: `$env:PYTHONPATH="src"; python -m rideflow run` (PowerShell) or `PYTHONPATH=src python -m rideflow run`.

`make demo` runs the pipeline, the health check and every analytics query (Linux/macOS).

### Commands

| Command | Purpose |
|---|---|
| `rideflow generate --trips 500 --seed 1 --out data/sample/events.jsonl` | Regenerate sample events |
| `rideflow run [--run-id ID]` | Full pipeline |
| `rideflow stage <init_db\|ingest\|silver\|load\|gold_quality\|report> --run-id ID` | Single stage (what Airflow calls) |
| `rideflow health` | Health check (JSON, exit 1 if unhealthy) |
| `rideflow query [name]` | List / run analytics queries |

Configuration lives in `config/pipeline.yaml`; any key can be overridden with `RIDEFLOW_<KEY>` environment variables (see `.env.example`).

## Inspecting outputs

| What | Where |
|---|---|
| Raw batches / checkpoint | `data/lake/bronze/` |
| Clean events, stats | `data/lake/silver/` |
| Rejected records | `data/lake/quarantine/rejects.jsonl` |
| Warehouse | `data/warehouse/rideflow.db` (open with any SQLite client) |
| Run + DQ reports | `reports/run_report_<run_id>.{md,json}`, tables `pipeline_runs`, `dq_results` |

A real report from a verified run is in [docs/sample_run_report.md](docs/sample_run_report.md).

## Testing

```bash
python -m pytest -q                       # pytest
python -m unittest discover -s tests -v   # same tests, no pytest needed (PYTHONPATH=src)
python scripts/validate_repo.py           # SQL/YAML/DAG syntax, secrets scan, junk-file check
```
Coverage includes: config validation, generator determinism, record validation (every reject reason), bronze idempotency / incremental / lost or corrupt checkpoint, trip building (order independence), warehouse migrations, constraints and upserts, quality gate blocking, missing source, garbage input, quarantine recovery, health, every analytics query, log redaction, DAG structure.

## Data quality and recovery

1. Invalid records are written to `data/lake/quarantine/rejects.jsonl` (`reasons`, `source_file`, `source_line`, `raw`).
2. Critical failures (e.g. reject rate above `max_reject_rate`) stop the run **before** the warehouse load; the failure is stored in `pipeline_runs`/`dq_results` and the report.
3. To recover: fix the cause upstream and re-run (`rideflow run`) - bronze/silver/warehouse stages are idempotent. To repair specific bronze records, edit the batch file named in the quarantine entry, then run `rideflow stage silver --run-id fix-1` and `rideflow stage load --run-id fix-1` (exercised by `test_quarantine_recovery_flow`).
4. A lost checkpoint replays identically; a corrupt one raises an error instead of silently restarting.

## Orchestration (Airflow)

`dags/rideflow_dag.py` defines hourly tasks `init_db >> ingest >> silver >> load >> gold_quality >> report` with 3 retries (exponential backoff), execution/DAG-run timeouts, `max_active_runs=1` and a failure callback. Local path (**not executed in the build environment, see below**):

```bash
pip install "apache-airflow==2.9.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.12.txt"
export RIDEFLOW_CONFIG=$PWD/config/pipeline.yaml PYTHONPATH=$PWD/src AIRFLOW__CORE__DAGS_FOLDER=$PWD/dags
airflow standalone
```

## Docker and Kafka

```bash
docker compose run --rm pipeline                                  # file-based run
docker compose --profile streaming up -d kafka
docker compose --profile streaming run --rm pipeline-kafka        # produce sample events, then ingest from Kafka
```

## Verification status

| Component | Status |
|---|---|
| Pipeline (generate -> bronze -> silver -> warehouse -> reports), CLI, SQL, health, queries | **Executed and tested** (53 tests, plus mutation checks that the tests fail when behaviour is broken) |
| `scripts/validate_repo.py` (SQL/YAML/DAG syntax, secrets scan) | **Executed** |
| Pytest runner / CI workflow | **Unverified** - pytest could not be installed offline; the same tests were run with `unittest`. The workflow YAML is syntax-checked only. |
| Airflow DAG | **Structure checked, scheduler never started** |
| Kafka source/producer, Docker image, Compose | **Unverified** (no broker, no Docker daemon, no network in the build environment). Reproduce with the commands above. |

## Limitations

* SQLite is a single-writer local warehouse; SQL uses SQLite upsert syntax and needs porting for PostgreSQL / BigQuery.
* pandas transforms are sized for the sample; very large volumes need a distributed engine (e.g. Spark).
* Kafka source supports a single partition and external offset management only.
* Dimensions are SCD Type 1; rows are never hard-deleted when upstream corrects history.
* No FX conversion: revenue is reported per currency.
* Security: no authentication or encryption at rest; the sample data is synthetic and pseudonymous. Do not point this at real personal data without adding access control and retention rules. SQL values are always parameterised; query names are validated and run on a read-only connection.

## Project layout

```
config/    pipeline.yaml            dags/     Airflow DAG
src/rideflow/                       sql/      schema/ transform/ analytics/
data/      reference + sample       tests/    unit + integration tests
scripts/   kafka_produce, validate_repo
docs/      architecture + sample report
```

## Author

**Adam Javed**  
GitHub: [@adamjaved-tech](https://github.com/adamjaved-tech)

This repository is a portfolio project demonstrating data-engineering design, implementation, testing, and documentation. The included sample data is synthetic and does not contain customer records.

## Contributing

Run `python scripts/validate_repo.py` and the test suite before opening a PR; add a test for every new validation rule or SQL change.

## Data and licence

Synthetic data only (generated by `src/rideflow/generator.py`); no third-party datasets are used. Code is released under the [MIT License](LICENSE).
