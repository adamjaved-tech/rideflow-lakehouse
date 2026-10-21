"""Deterministic synthetic ride-event generator.

The same (n_trips, seed, start, ...) always yields byte-identical output, so the
project runs without credentials or external APIs. Realistic defects are injected on
purpose (duplicates, malformed JSON, bad timestamps, negative fares, ...) to exercise
validation, quarantine and data-quality reporting.
"""

from __future__ import annotations

import json
import random
from datetime import timedelta
from pathlib import Path

from rideflow.timeutil import format_ts, parse_ts

CITIES = [
    ("lahore", "PKR"),
    ("karachi", "PKR"),
    ("dubai", "AED"),
    ("riyadh", "SAR"),
    ("cairo", "EGP"),
]
# (base fare, per-km rate) in local currency
FARE_MODEL = {"PKR": (150.0, 55.0), "AED": (8.0, 2.2), "SAR": (7.0, 1.9), "EGP": (25.0, 7.5)}
VEHICLES = ("economy", "comfort", "bike")


def generate_events(
    n_trips: int = 200,
    seed: int = 42,
    start: str = "2026-01-01T00:00:00Z",
    span_hours: int = 72,
    n_drivers: int = 40,
    defect_rate: float = 0.03,
) -> list[str]:
    """Return raw JSON lines (some intentionally malformed)."""
    if n_trips <= 0:
        raise ValueError("n_trips must be > 0")
    if not 0 <= defect_rate <= 0.5:
        raise ValueError("defect_rate must be between 0 and 0.5")
    rng = random.Random(seed)
    start_dt = parse_ts(start)
    drivers = [(f"drv-{i:04d}", rng.choice(VEHICLES)) for i in range(1, n_drivers + 1)]

    events: list[dict] = []
    for i in range(1, n_trips + 1):
        trip_id = f"trip-{i:06d}"
        rider_id = f"rid-{rng.randint(1, n_trips // 2 + 1):05d}"
        city_id, currency = rng.choice(CITIES)
        t0 = start_dt + timedelta(seconds=rng.randint(0, span_hours * 3600))
        seq = 0

        def emit(event_type: str, ts, **extra) -> None:
            nonlocal seq
            seq += 1
            events.append(
                {
                    "event_id": f"evt-{i:06d}-{seq}",
                    "event_type": event_type,
                    "trip_id": trip_id,
                    "rider_id": rider_id,
                    "city_id": city_id,
                    "currency": currency,
                    "event_ts": format_ts(ts),
                    "schema_version": 1,
                    **extra,
                }
            )

        emit("requested", t0)
        if rng.random() < 0.06:  # nobody accepted
            emit("cancelled", t0 + timedelta(seconds=rng.randint(20, 300)), cancel_reason="no_driver_found")
            continue
        driver_id, vehicle = rng.choice(drivers)
        t_acc = t0 + timedelta(seconds=rng.randint(5, 120))
        emit("accepted", t_acc, driver_id=driver_id, vehicle_type=vehicle)
        if rng.random() < 0.08:  # cancelled after accept
            reason = rng.choice(["driver_cancelled", "rider_cancelled"])
            emit("cancelled", t_acc + timedelta(seconds=rng.randint(30, 300)), driver_id=driver_id, cancel_reason=reason)
            continue
        t_start = t_acc + timedelta(seconds=rng.randint(60, 420))
        emit("started", t_start, driver_id=driver_id)
        if rng.random() < 0.04:  # still open (no terminal event yet)
            continue
        duration = rng.randint(300, 3000)
        distance = round(duration / 60 * rng.uniform(0.35, 0.7), 2)
        base, per_km = FARE_MODEL[currency]
        fare = round(base + distance * per_km + rng.uniform(0, base * 0.2), 2)
        emit(
            "completed",
            t_start + timedelta(seconds=duration),
            driver_id=driver_id,
            fare_amount=fare,
            distance_km=distance,
        )

    # out-of-order arrival: sort by event time plus bounded jitter
    events.sort(key=lambda e: (parse_ts(e["event_ts"]).timestamp() + rng.uniform(-300, 300), e["event_id"]))

    lines: list[str] = []
    d = defect_rate
    for ev in events:
        r = rng.random()
        copy = dict(ev)
        raw: str | None = None
        if r < d * 0.10:
            raw = json.dumps(copy, sort_keys=True)[: rng.randint(10, 40)]  # truncated JSON
        elif r < d * 0.30:
            copy["trip_id"] = None
        elif r < d * 0.50:
            copy["event_ts"] = "not-a-timestamp"
        elif r < d * 0.60:
            copy["event_ts"] = "2099-01-01T00:00:00Z"  # impossible future event
        elif r < d * 0.75 and "fare_amount" in copy:
            copy["fare_amount"] = -abs(copy["fare_amount"])
        elif r < d * 0.85:
            copy["event_type"] = "teleported"
        lines.append(raw if raw is not None else json.dumps(copy, sort_keys=True))
        if rng.random() < d * 0.8:  # at-least-once delivery duplicate
            lines.append(json.dumps(ev, sort_keys=True))
    return lines


def write_events(path: str | Path, lines: list[str]) -> Path:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")
    return out
