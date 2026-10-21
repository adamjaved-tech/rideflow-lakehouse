"""Silver layer: parse, validate, standardise and de-duplicate bronze records.

Invalid records are never dropped silently: they go to a quarantine file with the
reason(s) and a pointer to the exact bronze file/line, so they can be fixed and replayed
(see README 'Recovery'). The silver build is a full, deterministic rebuild from bronze,
therefore idempotent.
"""

from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path

import pandas as pd

from rideflow.bronze import list_batches
from rideflow.fsutil import atomic_write_json, atomic_write_text
from rideflow.logging_setup import get_logger, log_event
from rideflow.timeutil import format_ts, parse_ts

log = get_logger("silver")

EVENT_TYPES = ("requested", "accepted", "started", "completed", "cancelled")
REQUIRED_FIELDS = ("event_id", "event_type", "trip_id", "event_ts", "city_id")
SILVER_COLUMNS = [
    "event_id", "event_type", "trip_id", "rider_id", "driver_id", "city_id", "event_ts",
    "fare_amount", "currency", "distance_km", "vehicle_type", "cancel_reason",
    "source_file", "source_line",
]
FUTURE_TOLERANCE = timedelta(minutes=5)


@dataclass
class SilverStats:
    total_records: int = 0
    valid_records: int = 0
    rejected_records: int = 0
    duplicate_records: int = 0
    reject_reasons: dict = field(default_factory=dict)
    min_event_ts: str | None = None
    max_event_ts: str | None = None

    @property
    def reject_rate(self) -> float:
        return self.rejected_records / self.total_records if self.total_records else 0.0

    @property
    def duplicate_rate(self) -> float:
        return self.duplicate_records / self.total_records if self.total_records else 0.0

    def to_dict(self) -> dict:
        d = dict(self.__dict__)
        d["reject_rate"] = round(self.reject_rate, 6)
        d["duplicate_rate"] = round(self.duplicate_rate, 6)
        return d


def _text(value):
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return str(value) if not (isinstance(value, float) and not math.isfinite(value)) else None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return None


def _number(value):
    """Return a finite float or None; raise ValueError for present-but-invalid values."""
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise ValueError("boolean is not numeric")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("not numeric") from exc
    if not math.isfinite(number):
        raise ValueError("not finite")
    return number


def validate_line(raw: str, as_of, allowed_cities: set[str]):
    """Return (normalised record | None, [reason codes])."""
    if raw is None or not raw.strip():
        return None, ["empty_record"]
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        return None, ["malformed_json"]
    if not isinstance(obj, dict):
        return None, ["not_a_json_object"]

    reasons: list[str] = []
    rec = {
        "event_id": _text(obj.get("event_id")),
        "event_type": (_text(obj.get("event_type")) or "").lower() or None,
        "trip_id": _text(obj.get("trip_id")),
        "rider_id": _text(obj.get("rider_id")),
        "driver_id": _text(obj.get("driver_id")),
        "city_id": (_text(obj.get("city_id")) or "").lower() or None,
        "currency": (_text(obj.get("currency")) or "").upper() or None,
        "vehicle_type": (_text(obj.get("vehicle_type")) or "").lower() or None,
        "cancel_reason": _text(obj.get("cancel_reason")),
        "fare_amount": None,
        "distance_km": None,
        "event_ts": None,
    }
    for name in REQUIRED_FIELDS:
        if name != "event_ts" and rec[name] is None:
            reasons.append(f"missing_{name}")
    if rec["event_type"] and rec["event_type"] not in EVENT_TYPES:
        reasons.append("unknown_event_type")
    if rec["city_id"] and rec["city_id"] not in allowed_cities:
        reasons.append("unknown_city_id")

    raw_ts = obj.get("event_ts")
    if raw_ts is None or raw_ts == "":
        reasons.append("missing_event_ts")
    else:
        try:
            ts = parse_ts(raw_ts)
            if ts > as_of + FUTURE_TOLERANCE:
                reasons.append("event_ts_in_future")
            rec["event_ts"] = format_ts(ts)
        except (ValueError, OverflowError):
            reasons.append("invalid_event_ts")

    for name in ("fare_amount", "distance_km"):
        try:
            number = _number(obj.get(name))
        except ValueError:
            reasons.append(f"invalid_{name}")
            continue
        if number is not None and number < 0:
            reasons.append(f"negative_{name}")
        rec[name] = number

    if rec["event_type"] == "completed":
        if rec["fare_amount"] is None and "invalid_fare_amount" not in reasons:
            reasons.append("completed_missing_fare_amount")
        if rec["distance_km"] is None and "invalid_distance_km" not in reasons:
            reasons.append("completed_missing_distance_km")
    if rec["event_type"] == "accepted" and rec["driver_id"] is None:
        reasons.append("accepted_missing_driver_id")

    return (None, reasons) if reasons else (rec, [])


def resolve_as_of(cfg):
    from rideflow.timeutil import utc_now

    return parse_ts(cfg.as_of) if cfg.as_of else utc_now()


def build_silver(cfg, allowed_cities: set[str]):
    """Return (DataFrame, quarantine rows, SilverStats). Pure function of bronze + config."""
    as_of = resolve_as_of(cfg)
    stats = SilverStats()
    rows: list[dict] = []
    rejects: list[dict] = []
    seen: set[str] = set()

    for batch in list_batches(cfg.bronze_dir):
        text = batch.read_text(encoding="utf-8")
        for line_no, raw in enumerate(text.splitlines(), start=1):
            stats.total_records += 1
            rec, reasons = validate_line(raw, as_of, allowed_cities)
            if reasons:
                stats.rejected_records += 1
                primary = reasons[0]
                stats.reject_reasons[primary] = stats.reject_reasons.get(primary, 0) + 1
                rejects.append({"source_file": batch.name, "source_line": line_no, "reasons": reasons, "raw": raw[:2000]})
                continue
            if rec["event_id"] in seen:
                stats.duplicate_records += 1
                rejects.append({"source_file": batch.name, "source_line": line_no, "reasons": ["duplicate_event_id"], "raw": raw[:2000]})
                continue
            seen.add(rec["event_id"])
            rec["source_file"] = batch.name
            rec["source_line"] = line_no
            rows.append(rec)

    stats.valid_records = len(rows)
    df = pd.DataFrame(rows, columns=SILVER_COLUMNS)
    if not df.empty:
        df = df.sort_values(["event_ts", "event_id"], kind="stable").reset_index(drop=True)
        stats.min_event_ts = str(df["event_ts"].iloc[0])
        stats.max_event_ts = str(df["event_ts"].iloc[-1])
    log_event(log, logging.INFO, "silver built", **stats.to_dict())
    return df, rejects, stats


def write_silver(cfg, df: pd.DataFrame, rejects: list[dict], stats: SilverStats) -> None:
    atomic_write_text(cfg.silver_dir / "events.csv", df.to_csv(index=False, lineterminator="\n"))
    atomic_write_json(cfg.silver_dir / "_stats.json", stats.to_dict())
    body = "".join(json.dumps(r, sort_keys=True) + "\n" for r in rejects)
    atomic_write_text(cfg.quarantine_dir / "rejects.jsonl", body)


def read_silver(cfg) -> pd.DataFrame:
    path = cfg.silver_dir / "events.csv"
    if not path.is_file():
        raise FileNotFoundError(f"silver events not found: {path}. Run the silver stage first.")
    df = pd.read_csv(path, dtype=str, keep_default_na=True)
    for col in ("fare_amount", "distance_km"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in SILVER_COLUMNS:
        if col not in df.columns:
            df[col] = None
    return df[SILVER_COLUMNS]
