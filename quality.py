"""Data-quality rules with explicit pass/fail results, severity and readable messages."""

from __future__ import annotations

import sqlite3
from dataclasses import asdict, dataclass

import pandas as pd

from rideflow.errors import DataQualityError
from rideflow.silver import SilverStats
from rideflow.timeutil import parse_ts


@dataclass
class CheckResult:
    name: str
    layer: str
    severity: str  # critical | warning
    passed: bool
    observed: str
    expected: str
    message: str

    def to_dict(self) -> dict:
        return asdict(self)


def _result(name, layer, severity, passed, observed, expected, ok_msg, fail_msg) -> CheckResult:
    return CheckResult(name, layer, severity, bool(passed), str(observed), str(expected), ok_msg if passed else fail_msg)


def silver_checks(cfg, stats: SilverStats, df: pd.DataFrame, as_of) -> list[CheckResult]:
    r = []
    r.append(_result("silver_not_empty", "silver", "critical", stats.valid_records > 0, stats.valid_records, "> 0",
                     "valid records present", "no valid records survived validation; inspect quarantine/rejects.jsonl"))
    r.append(_result("silver_reject_rate", "silver", "critical", stats.reject_rate <= cfg.max_reject_rate,
                     f"{stats.reject_rate:.4f}", f"<= {cfg.max_reject_rate}", "reject rate within threshold",
                     f"{stats.rejected_records}/{stats.total_records} records rejected; top reasons: {dict(sorted(stats.reject_reasons.items(), key=lambda kv: -kv[1])[:3])}"))
    r.append(_result("silver_duplicate_rate", "silver", "warning", stats.duplicate_rate <= cfg.max_duplicate_rate,
                     f"{stats.duplicate_rate:.4f}", f"<= {cfg.max_duplicate_rate}", "duplicate rate within threshold",
                     f"{stats.duplicate_records} duplicate event_ids dropped; check upstream delivery guarantees"))
    null_keys = int(df[["event_id", "trip_id", "event_ts", "city_id"]].isna().any(axis=1).sum()) if not df.empty else 0
    r.append(_result("silver_no_null_keys", "silver", "critical", null_keys == 0, null_keys, 0,
                     "no null business keys", f"{null_keys} rows have null event_id/trip_id/event_ts/city_id"))
    dup_ids = int(df["event_id"].duplicated().sum()) if not df.empty else 0
    r.append(_result("silver_unique_event_id", "silver", "critical", dup_ids == 0, dup_ids, 0,
                     "event_id is unique", f"{dup_ids} duplicate event_id values in silver"))
    negatives = int(((df["fare_amount"] < 0) | (df["distance_km"] < 0)).sum()) if not df.empty else 0
    r.append(_result("silver_non_negative_amounts", "silver", "critical", negatives == 0, negatives, 0,
                     "no negative fares/distances", f"{negatives} rows have negative amounts"))
    if stats.max_event_ts:
        lag_h = (as_of - parse_ts(stats.max_event_ts)).total_seconds() / 3600
        r.append(_result("silver_freshness", "silver", "warning", lag_h <= cfg.freshness_max_lag_hours,
                         f"{lag_h:.1f}h", f"<= {cfg.freshness_max_lag_hours}h", "data is fresh",
                         f"newest event is {lag_h:.1f}h older than as_of; upstream may be stalled"))
    return r


def gold_checks(conn: sqlite3.Connection, cfg) -> list[CheckResult]:
    r = []
    fk = conn.execute("PRAGMA foreign_key_check").fetchall()
    r.append(_result("gold_foreign_keys", "gold", "critical", len(fk) == 0, len(fk), 0,
                     "all foreign keys resolve", f"{len(fk)} foreign key violations in the warehouse"))
    missing = conn.execute(
        "SELECT COUNT(*) FROM stg_trips s LEFT JOIN fact_trips f ON f.trip_id = s.trip_id WHERE f.trip_id IS NULL"
    ).fetchone()[0]
    r.append(_result("gold_reconciliation", "gold", "critical", missing == 0, missing, 0,
                     "every staged trip is present in fact_trips", f"{missing} staged trips missing from fact_trips (e.g. unknown city)"))
    bad_completed = conn.execute(
        "SELECT COUNT(*) FROM fact_trips WHERE status = 'completed' AND (fare_amount IS NULL OR completed_ts IS NULL)"
    ).fetchone()[0]
    r.append(_result("gold_completed_have_fare", "gold", "critical", bad_completed == 0, bad_completed, 0,
                     "completed trips carry fare and completion time", f"{bad_completed} completed trips lack fare/completion time"))
    disorder = conn.execute(
        "SELECT COUNT(*) FROM fact_trips WHERE completed_ts IS NOT NULL AND requested_ts IS NOT NULL AND completed_ts < requested_ts"
    ).fetchone()[0]
    r.append(_result("gold_timestamp_order", "gold", "warning", disorder == 0, disorder, 0,
                     "completion never precedes request", f"{disorder} trips complete before they were requested"))
    total, cancelled = conn.execute(
        "SELECT COUNT(*), COALESCE(SUM(status = 'cancelled'), 0) FROM fact_trips"
    ).fetchone()
    rate = cancelled / total if total else 0.0
    r.append(_result("gold_cancel_rate_anomaly", "gold", "warning", rate <= cfg.max_cancel_rate, f"{rate:.4f}",
                     f"<= {cfg.max_cancel_rate}", "cancellation rate in expected range",
                     f"cancellation rate {rate:.1%} exceeds {cfg.max_cancel_rate:.0%}; investigate supply or upstream status bugs"))
    return r


def critical_failures(results: list[CheckResult]) -> list[CheckResult]:
    return [c for c in results if c.severity == "critical" and not c.passed]


def raise_on_critical(results: list[CheckResult]) -> None:
    failed = critical_failures(results)
    if failed:
        detail = "; ".join(f"{c.name}: {c.message}" for c in failed)
        raise DataQualityError(f"{len(failed)} critical data-quality check(s) failed: {detail}", failed)


def render_markdown(run_id: str, results: list[CheckResult]) -> str:
    lines = [f"### Data-quality report - {run_id}", "", "| Check | Layer | Severity | Result | Observed | Expected |", "|---|---|---|---|---|---|"]
    for c in results:
        lines.append(f"| {c.name} | {c.layer} | {c.severity} | {'PASS' if c.passed else 'FAIL'} | {c.observed} | {c.expected} |")
    failed = [c for c in results if not c.passed]
    if failed:
        lines += ["", "**Failures**", ""] + [f"- `{c.name}` ({c.severity}): {c.message}" for c in failed]
    return "\n".join(lines) + "\n"
