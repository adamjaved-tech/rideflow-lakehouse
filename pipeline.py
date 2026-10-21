"""Pipeline orchestration: independent, restartable stages plus an in-process runner.

Stages communicate only via the lake, the warehouse and the per-run state file, so the
same functions are used by the CLI (`rideflow run`) and by the Airflow DAG tasks.
"""

from __future__ import annotations

import logging
import time
import uuid
from contextlib import contextmanager
from datetime import timedelta

from rideflow import quality
from rideflow.bronze import ingest
from rideflow.config import Config, load_cities
from rideflow.errors import DataQualityError, PipelineError
from rideflow.fsutil import atomic_write_json, atomic_write_text
from rideflow.gold import build_trips
from rideflow.logging_setup import get_logger, log_event, set_run_id
from rideflow.silver import build_silver, read_silver, resolve_as_of, write_silver
from rideflow.sources import build_source
from rideflow.state import load_state, update_state, validate_run_id
from rideflow.timeutil import format_ts, parse_ts, utc_now
from rideflow import warehouse

log = get_logger("pipeline")
STAGES = ("init_db", "ingest", "silver", "load", "gold_quality", "report")


def new_run_id() -> str:
    return f"run-{utc_now().strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:6]}"


@contextmanager
def _stage(name: str, run_id: str):
    set_run_id(run_id)
    started = time.monotonic()
    log_event(log, logging.INFO, "stage started", stage=name)
    try:
        yield
    except Exception as exc:
        log_event(log, logging.ERROR, "stage failed", stage=name, error_type=type(exc).__name__, error=str(exc),
                  seconds=round(time.monotonic() - started, 3))
        raise
    log_event(log, logging.INFO, "stage finished", stage=name, seconds=round(time.monotonic() - started, 3))


def _mark_started(cfg: Config, run_id: str) -> None:
    if "started_at" not in load_state(cfg, run_id):
        update_state(cfg, run_id, started_at=format_ts(utc_now()))


def stage_init_db(cfg: Config, run_id: str) -> None:
    with _stage("init_db", run_id):
        _mark_started(cfg, run_id)
        conn = warehouse.connect(cfg.warehouse_path)
        try:
            applied = warehouse.migrate(conn, cfg.sql_dir)
        finally:
            conn.close()
        update_state(cfg, run_id, migrations_applied=applied)


def stage_ingest(cfg: Config, run_id: str, source=None) -> None:
    with _stage("ingest", run_id):
        _mark_started(cfg, run_id)
        result = ingest(cfg, source or build_source(cfg))
        update_state(cfg, run_id, ingest=result.__dict__)


def stage_silver(cfg: Config, run_id: str) -> None:
    with _stage("silver", run_id):
        cities = {c["city_id"].strip().lower() for c in load_cities(cfg)}
        df, rejects, stats = build_silver(cfg, cities)
        write_silver(cfg, df, rejects, stats)
        results = quality.silver_checks(cfg, stats, df, resolve_as_of(cfg))
        update_state(cfg, run_id, silver=stats.to_dict(), dq_silver=[c.to_dict() for c in results])
        quality.raise_on_critical(results)  # quality gate: nothing reaches the warehouse if this fails


def stage_load(cfg: Config, run_id: str) -> None:
    with _stage("load", run_id):
        trips = build_trips(read_silver(cfg))
        conn = warehouse.connect(cfg.warehouse_path)
        try:
            summary = warehouse.load_gold(conn, cfg, load_cities(cfg), trips, run_id)
        finally:
            conn.close()
        update_state(cfg, run_id, gold=summary, trips_loaded=len(trips))


def stage_gold_quality(cfg: Config, run_id: str) -> None:
    with _stage("gold_quality", run_id):
        conn = warehouse.connect(cfg.warehouse_path)
        try:
            results = quality.gold_checks(conn, cfg)
        finally:
            conn.close()
        update_state(cfg, run_id, dq_gold=[c.to_dict() for c in results])
        quality.raise_on_critical(results)


def _record_run(cfg: Config, run_id: str, status: str, report: dict) -> None:
    conn = warehouse.connect(cfg.warehouse_path)
    try:
        warehouse.migrate(conn, cfg.sql_dir)
        conn.execute("DELETE FROM dq_results WHERE run_id = ?", (run_id,))
        conn.execute(
            """INSERT OR REPLACE INTO pipeline_runs (run_id, status, started_at, finished_at, duration_seconds,
                   records_total, records_rejected, trips_loaded, sla_breached, error_message)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (run_id, status, report["started_at"], report["finished_at"], report["duration_seconds"],
             report.get("records_total"), report.get("records_rejected"), report.get("trips_loaded"),
             1 if report.get("sla_breached") else 0, report.get("error_message")),
        )
        conn.executemany(
            "INSERT INTO dq_results (run_id, check_name, layer, severity, passed, observed, expected, message) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [(run_id, c["name"], c["layer"], c["severity"], 1 if c["passed"] else 0, c["observed"], c["expected"], c["message"])
             for c in report["checks"]],
        )
        conn.commit()
    finally:
        conn.close()


def _build_report(cfg: Config, run_id: str, status: str, error: str | None = None) -> dict:
    state = load_state(cfg, run_id)
    now = utc_now()
    started = parse_ts(state.get("started_at", format_ts(now)))
    duration = max((now - started).total_seconds(), 0.0)
    checks = list(state.get("dq_silver", [])) + list(state.get("dq_gold", []))
    silver = state.get("silver", {})
    sla_breached = duration > cfg.sla_seconds
    if sla_breached:
        checks.append(quality.CheckResult("run_sla", "ops", "warning", False, f"{duration:.1f}s", f"<= {cfg.sla_seconds}s",
                                          f"run took {duration:.1f}s, over the {cfg.sla_seconds}s SLA").to_dict())
    else:
        checks.append(quality.CheckResult("run_sla", "ops", "warning", True, f"{duration:.1f}s", f"<= {cfg.sla_seconds}s",
                                          "run within SLA").to_dict())
    return {
        "run_id": run_id, "status": status, "started_at": format_ts(started), "finished_at": format_ts(now),
        "duration_seconds": round(duration, 3), "sla_breached": sla_breached, "error_message": error,
        "records_total": silver.get("total_records"), "records_rejected": silver.get("rejected_records"),
        "records_duplicate": silver.get("duplicate_records"), "reject_reasons": silver.get("reject_reasons", {}),
        "trips_loaded": state.get("trips_loaded"), "ingest": state.get("ingest"), "gold": state.get("gold"),
        "checks": checks,
    }


def _write_report_files(cfg: Config, run_id: str, report: dict) -> None:
    atomic_write_json(cfg.reports_dir / f"run_report_{run_id}.json", report)
    md = [f"## Run report - {run_id}", "", f"- Status: **{report['status']}**",
          f"- Duration: {report['duration_seconds']}s (SLA {cfg.sla_seconds:.0f}s, breached: {report['sla_breached']})",
          f"- Records: {report['records_total']} total, {report['records_rejected']} rejected, {report.get('records_duplicate')} duplicates dropped",
          f"- Trips loaded: {report['trips_loaded']}"]
    if report.get("reject_reasons"):
        md.append(f"- Reject reasons: {report['reject_reasons']}")
    if report.get("error_message"):
        md.append(f"- Error: {report['error_message']}")
    md.append("")
    md.append(quality.render_markdown(run_id, [quality.CheckResult(**c) for c in report["checks"]]))
    atomic_write_text(cfg.reports_dir / f"run_report_{run_id}.md", "\n".join(md))


def stage_report(cfg: Config, run_id: str) -> dict:
    with _stage("report", run_id):
        report = _build_report(cfg, run_id, "success")
        _record_run(cfg, run_id, "success", report)
        _write_report_files(cfg, run_id, report)
        log_event(log, logging.INFO, "run succeeded", duration_seconds=report["duration_seconds"],
                  trips_loaded=report["trips_loaded"], sla_breached=report["sla_breached"])
        return report


def record_failure(cfg: Config, run_id: str, exc: BaseException) -> None:
    """Best-effort audit of a failed run. Never raises: the original error must surface."""
    try:
        report = _build_report(cfg, run_id, "failed", error=f"{type(exc).__name__}: {exc}"[:2000])
        _record_run(cfg, run_id, "failed", report)
        _write_report_files(cfg, run_id, report)
    except Exception as audit_exc:  # noqa: BLE001 - audit must not mask the real failure
        log_event(log, logging.ERROR, "could not record failed run", error=str(audit_exc))


def run_stage(cfg: Config, name: str, run_id: str, source=None):
    validate_run_id(run_id)
    if name not in STAGES:
        raise PipelineError(f"unknown stage '{name}'. Valid stages: {', '.join(STAGES)}")
    if name == "ingest":
        return stage_ingest(cfg, run_id, source)
    return globals()[f"stage_{name}"](cfg, run_id)


def run_pipeline(cfg: Config, run_id: str | None = None, source=None) -> dict:
    run_id = validate_run_id(run_id or new_run_id())
    set_run_id(run_id)
    log_event(log, logging.INFO, "pipeline started", source_type=cfg.source_type)
    try:
        stage_init_db(cfg, run_id)
        stage_ingest(cfg, run_id, source)
        stage_silver(cfg, run_id)
        stage_load(cfg, run_id)
        stage_gold_quality(cfg, run_id)
        return stage_report(cfg, run_id)
    except Exception as exc:
        record_failure(cfg, run_id, exc)
        raise
