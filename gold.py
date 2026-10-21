"""Gold transformations: collapse the silver event stream into one row per trip and
build the date dimension. Pure functions (no I/O) so they are trivially unit-testable."""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from rideflow.silver import EVENT_TYPES
from rideflow.timeutil import parse_ts

TRIP_COLUMNS = [
    "trip_id", "rider_id", "driver_id", "city_id", "vehicle_type", "status",
    "first_event_ts", "requested_ts", "accepted_ts", "started_ts", "completed_ts", "cancelled_ts",
    "fare_amount", "currency", "distance_km", "wait_seconds", "duration_seconds", "cancel_reason",
]
_TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def _seconds_between(start: pd.Series, end: pd.Series) -> pd.Series:
    a = pd.to_datetime(start, utc=True, format=_TS_FORMAT)
    b = pd.to_datetime(end, utc=True, format=_TS_FORMAT)
    return (b - a).dt.total_seconds()


def build_trips(events: pd.DataFrame) -> pd.DataFrame:
    """One row per trip_id. Order-independent: late/out-of-order events are handled by
    using the earliest timestamp per event type."""
    if events.empty:
        return pd.DataFrame(columns=TRIP_COLUMNS)

    ev = events.sort_values(["event_ts", "event_id"], kind="stable")
    stamps = ev.groupby(["trip_id", "event_type"])["event_ts"].min().unstack()
    for etype in EVENT_TYPES:
        if etype not in stamps.columns:
            stamps[etype] = None

    by_trip = ev.groupby("trip_id")
    trips = pd.DataFrame(
        {
            "rider_id": by_trip["rider_id"].first(),
            "driver_id": by_trip["driver_id"].last(),
            "city_id": by_trip["city_id"].first(),
            "vehicle_type": by_trip["vehicle_type"].last(),
            "currency": by_trip["currency"].last(),
            "first_event_ts": by_trip["event_ts"].min(),
            "requested_ts": stamps["requested"],
            "accepted_ts": stamps["accepted"],
            "started_ts": stamps["started"],
            "completed_ts": stamps["completed"],
            "cancelled_ts": stamps["cancelled"],
        }
    )
    completed = ev[ev["event_type"] == "completed"].groupby("trip_id")[["fare_amount", "distance_km"]].last()
    cancelled = ev[ev["event_type"] == "cancelled"].groupby("trip_id")["cancel_reason"].last()
    trips = trips.join(completed).join(cancelled)

    done, cancel = trips["completed_ts"], trips["cancelled_ts"]
    is_completed = done.notna() & (cancel.isna() | (done >= cancel))
    is_cancelled = cancel.notna() & ~is_completed
    trips["status"] = "open"
    trips.loc[is_cancelled, "status"] = "cancelled"
    trips.loc[is_completed, "status"] = "completed"

    trips["wait_seconds"] = _seconds_between(trips["requested_ts"], trips["accepted_ts"])
    trips["duration_seconds"] = _seconds_between(trips["started_ts"], trips["completed_ts"])
    # fare/distance only describe completed trips
    trips.loc[trips["status"] != "completed", ["fare_amount", "distance_km"]] = None
    trips.loc[trips["status"] != "cancelled", "cancel_reason"] = None
    return trips.reset_index()[TRIP_COLUMNS]


def build_dim_date(trips: pd.DataFrame) -> list[tuple]:
    """Rows for dim_date covering every day between the first and last trip."""
    if trips.empty:
        return []
    first = parse_ts(str(trips["first_event_ts"].min())).date()
    last = parse_ts(str(trips["first_event_ts"].max())).date()
    rows, day = [], first
    while day <= last:
        rows.append(_date_row(day))
        day += timedelta(days=1)
    return rows


def _date_row(day: date) -> tuple:
    return (
        int(day.strftime("%Y%m%d")), day.isoformat(), day.year, day.month, day.day,
        day.isoweekday(), 1 if day.isoweekday() >= 6 else 0,
    )


def to_records(df: pd.DataFrame, columns: list[str]) -> list[tuple]:
    """DataFrame -> list of tuples with NaN/NaT -> None and whole-number floats -> int
    for second columns (safe for parameterised SQL inserts)."""
    out = []
    for row in df[columns].itertuples(index=False, name=None):
        clean = []
        for col, value in zip(columns, row):
            if value is None or (not isinstance(value, str) and pd.isna(value)):
                clean.append(None)
            elif col.endswith("_seconds"):
                clean.append(int(round(float(value))))
            elif hasattr(value, "item"):
                clean.append(value.item())
            else:
                clean.append(value)
        out.append(tuple(clean))
    return out
