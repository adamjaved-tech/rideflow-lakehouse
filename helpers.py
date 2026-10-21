"""Shared test helpers: build an isolated Config in a temp directory."""

from __future__ import annotations

import shutil
from pathlib import Path

import yaml

from rideflow.config import load_config
from rideflow.generator import generate_events, write_events

REPO = Path(__file__).resolve().parents[1]


def make_project(tmp: Path, n_trips: int = 120, seed: int = 7, **overrides):
    """Copy sql/reference data into tmp, generate events, return a loaded Config."""
    shutil.copytree(REPO / "sql", tmp / "sql")
    shutil.copytree(REPO / "data" / "reference", tmp / "data" / "reference")
    (tmp / "config").mkdir()
    write_events(tmp / "data" / "events.jsonl", generate_events(n_trips=n_trips, seed=seed))
    base = yaml.safe_load((REPO / "config" / "pipeline.yaml").read_text())
    base.update({"source_path": "data/events.jsonl", "batch_size": 100})
    base.update(overrides)
    (tmp / "config" / "pipeline.yaml").write_text(yaml.safe_dump(base))
    return load_config(tmp / "config" / "pipeline.yaml", env={})


def event(**kw):
    """A valid raw event dict; override fields via kwargs."""
    base = {
        "event_id": "e1", "event_type": "requested", "trip_id": "t1", "rider_id": "r1",
        "city_id": "lahore", "currency": "PKR", "event_ts": "2026-01-01T10:00:00Z", "schema_version": 1,
    }
    base.update(kw)
    return base
