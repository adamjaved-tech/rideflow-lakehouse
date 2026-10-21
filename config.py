"""Configuration: YAML file + RIDEFLOW_<KEY> environment overrides, validated on load."""

from __future__ import annotations

import csv
import os
from dataclasses import dataclass
from pathlib import Path

import yaml

from rideflow.errors import ConfigError
from rideflow.timeutil import parse_ts

ENV_PREFIX = "RIDEFLOW_"
DEFAULT_CONFIG_PATH = "config/pipeline.yaml"

# key -> (type, required)
SPEC = {
    "lake_dir": (str, True),
    "warehouse_path": (str, True),
    "reports_dir": (str, True),
    "sql_dir": (str, True),
    "reference_dir": (str, True),
    "source_type": (str, True),
    "source_path": (str, False),
    "batch_size": (int, True),
    "max_reject_rate": (float, True),
    "max_duplicate_rate": (float, True),
    "freshness_max_lag_hours": (float, True),
    "max_cancel_rate": (float, True),
    "sla_seconds": (float, True),
    "health_max_run_age_hours": (float, True),
    "as_of": (str, False),
    "kafka_bootstrap": (str, False),
    "kafka_topic": (str, False),
    "kafka_group": (str, False),
}


@dataclass(frozen=True)
class Config:
    project_root: Path
    lake_dir: Path
    warehouse_path: Path
    reports_dir: Path
    sql_dir: Path
    reference_dir: Path
    source_type: str
    source_path: Path | None
    batch_size: int
    max_reject_rate: float
    max_duplicate_rate: float
    freshness_max_lag_hours: float
    max_cancel_rate: float
    sla_seconds: float
    health_max_run_age_hours: float
    as_of: str | None
    kafka_bootstrap: str | None
    kafka_topic: str
    kafka_group: str

    @property
    def bronze_dir(self) -> Path:
        return self.lake_dir / "bronze"

    @property
    def silver_dir(self) -> Path:
        return self.lake_dir / "silver"

    @property
    def quarantine_dir(self) -> Path:
        return self.lake_dir / "quarantine"


def _cast(key: str, raw, typ):
    try:
        if typ is int:
            if isinstance(raw, bool):
                raise ValueError("bool is not an int")
            return int(raw)
        if typ is float:
            if isinstance(raw, bool):
                raise ValueError("bool is not a float")
            return float(raw)
        return str(raw)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"config key '{key}' must be {typ.__name__}, got {raw!r}") from exc


def load_config(path: str | os.PathLike | None = None, env=None) -> Config:
    env = os.environ if env is None else env
    cfg_path = Path(path or env.get(ENV_PREFIX + "CONFIG") or DEFAULT_CONFIG_PATH)
    if not cfg_path.is_file():
        raise ConfigError(f"config file not found: {cfg_path}")
    try:
        data = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"config file is not valid YAML: {cfg_path}") from exc
    if not isinstance(data, dict):
        raise ConfigError("config file must contain a YAML mapping")

    unknown = sorted(set(data) - set(SPEC))
    if unknown:
        raise ConfigError(f"unknown config keys: {', '.join(unknown)}")

    values: dict = {}
    for key, (typ, required) in SPEC.items():
        env_key = ENV_PREFIX + key.upper()
        raw = env[env_key] if env_key in env and env[env_key] != "" else data.get(key)
        if raw is None:
            if required:
                raise ConfigError(f"missing required config key '{key}'")
            values[key] = None
            continue
        values[key] = _cast(key, raw, typ)

    root = cfg_path.resolve().parent.parent

    def _path(value):
        p = Path(value)
        return p if p.is_absolute() else root / p

    if values["source_type"] not in ("file", "kafka"):
        raise ConfigError("source_type must be 'file' or 'kafka'")
    if values["source_type"] == "file" and not values["source_path"]:
        raise ConfigError("source_path is required when source_type is 'file'")
    if values["source_type"] == "kafka" and not values["kafka_bootstrap"]:
        raise ConfigError("kafka_bootstrap (env RIDEFLOW_KAFKA_BOOTSTRAP) is required when source_type is 'kafka'")
    if values["batch_size"] <= 0:
        raise ConfigError("batch_size must be > 0")
    for rate in ("max_reject_rate", "max_duplicate_rate", "max_cancel_rate"):
        if not 0.0 <= values[rate] <= 1.0:
            raise ConfigError(f"{rate} must be between 0 and 1")
    for positive in ("sla_seconds", "freshness_max_lag_hours", "health_max_run_age_hours"):
        if values[positive] <= 0:
            raise ConfigError(f"{positive} must be > 0")
    if values["as_of"]:
        try:
            parse_ts(values["as_of"])
        except ValueError as exc:
            raise ConfigError(f"as_of is not a valid ISO-8601 timestamp: {values['as_of']!r}") from exc

    return Config(
        project_root=root,
        lake_dir=_path(values["lake_dir"]),
        warehouse_path=_path(values["warehouse_path"]),
        reports_dir=_path(values["reports_dir"]),
        sql_dir=_path(values["sql_dir"]),
        reference_dir=_path(values["reference_dir"]),
        source_type=values["source_type"],
        source_path=_path(values["source_path"]) if values["source_path"] else None,
        batch_size=values["batch_size"],
        max_reject_rate=values["max_reject_rate"],
        max_duplicate_rate=values["max_duplicate_rate"],
        freshness_max_lag_hours=values["freshness_max_lag_hours"],
        max_cancel_rate=values["max_cancel_rate"],
        sla_seconds=values["sla_seconds"],
        health_max_run_age_hours=values["health_max_run_age_hours"],
        as_of=values["as_of"],
        kafka_bootstrap=values["kafka_bootstrap"],
        kafka_topic=values["kafka_topic"] or "rideflow.events",
        kafka_group=values["kafka_group"] or "rideflow-bronze",
    )


def load_cities(cfg: Config) -> list[dict]:
    """Reference data for dim_city. Also defines the set of valid city ids."""
    path = cfg.reference_dir / "cities.csv"
    if not path.is_file():
        raise ConfigError(f"reference file not found: {path}")
    with path.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    if not rows:
        raise ConfigError(f"reference file is empty: {path}")
    return rows
