"""Per-run state file so independently scheduled stages (e.g. Airflow tasks) can hand
metrics to each other without sharing process memory."""

from __future__ import annotations

import json
import re
from pathlib import Path

from rideflow.errors import ConfigError
from rideflow.fsutil import atomic_write_json

_RUN_ID = re.compile(r"^[A-Za-z0-9_.-]{1,80}$")


def validate_run_id(run_id: str) -> str:
    if not isinstance(run_id, str) or not _RUN_ID.match(run_id) or run_id in (".", ".."):
        raise ConfigError("run_id may only contain letters, digits, '_', '.', '-' (max 80 chars)")
    return run_id


def sanitize_run_id(raw: str) -> str:
    """Make an arbitrary scheduler run id (e.g. 'scheduled__2026-01-01T00:00:00+00:00') path-safe."""
    return re.sub(r"[^A-Za-z0-9_.-]", "_", raw)[:80]


def state_path(cfg, run_id: str) -> Path:
    return Path(cfg.reports_dir) / "runs" / validate_run_id(run_id) / "state.json"


def load_state(cfg, run_id: str) -> dict:
    path = state_path(cfg, run_id)
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def update_state(cfg, run_id: str, **entries) -> dict:
    state = load_state(cfg, run_id)
    state.update(entries)
    atomic_write_json(state_path(cfg, run_id), state)
    return state
