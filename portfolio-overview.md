# RideFlow Lakehouse — Portfolio Overview

**Author:** Adam Javed  
**Category:** Data Engineering / Analytics Engineering  
**Repository:** https://github.com/adamjaved-tech/rideflow-lakehouse

## One-line summary

A reproducible event-driven data platform that ingests synthetic ride-hailing events, applies bronze/silver/gold processing and data-quality gates, loads a dimensional warehouse, and publishes audited analytics outputs.

## Problem it addresses

Raw event streams commonly contain duplicates, malformed records, missing identifiers, invalid timestamps, and inconsistent business values. Downstream analytics need trusted data plus a way to explain failures and recover safely.

## What the implementation does

1. Generates deterministic synthetic trip events, including configurable data defects.
2. Ingests batches into a bronze layer with checkpoints and replay-safe behavior.
3. Validates and normalizes records into a silver layer; rejected records are quarantined with reason codes and source-line references.
4. Applies critical and warning-level quality checks before loading trusted data.
5. Builds trip records and upserts them into a SQL dimensional model.
6. Stores run audits and data-quality outcomes, creates reports, and exposes analytics queries.
7. Provides CLI execution and an Airflow DAG; Kafka input and Docker/Compose are optional execution paths.

## Engineering decisions worth discussing

- **Idempotency:** rerunning stages should converge rather than duplicate warehouse records.
- **Traceability:** rejected rows retain reason codes and raw-source pointers.
- **Quality gates:** critical failures prevent questionable data from reaching the warehouse.
- **Schema evolution:** versioned SQL migrations create and update warehouse structures.
- **Operational visibility:** structured logs, health checks, SLA checks, run IDs, and persisted audit tables support troubleshooting.
- **Scope clarity:** SQLite and pandas keep the demo easy to run locally; larger deployments would need a production warehouse and distributed processing strategy.

## Evidence in the repository

- 53 unit/integration tests run successfully with `python -m unittest discover -s tests -v` in the preparation environment.
- `python scripts/validate_repo.py` completes successfully.
- A sample run report is checked into `docs/sample_run_report.md`.
- GitHub Actions is configured to run tests and a Docker build on pushes and pull requests. Hosted CI results should be described as verified only after the workflow has actually run in the published repository.

## Resume alignment

**Primary match: Data Engineer / Senior Data Engineer.** The strongest evidence is pipeline design, Python/SQL, data quality, dimensional modelling, orchestration, observability, and operational reliability.

**Secondary match: Data Platform Engineer.** Checkpointing, idempotency, audits, CI, containers, and orchestration are relevant platform-engineering signals.

**Not the primary match:** AI/ML Engineer. The repository does not implement model training, evaluation, or serving; present it as a data platform that could support downstream AI workloads, not as an ML project.

## Suggested resume bullet

Built RideFlow Lakehouse, a reproducible Python/SQL data pipeline with bronze/silver/gold layers, quarantine and quality gates, idempotent warehouse upserts, audit reports, analytics queries, and optional Kafka/Airflow orchestration; validated with 53 automated tests.

Use this bullet only if you can confidently explain the implementation and personally substantiate the work in interviews.
