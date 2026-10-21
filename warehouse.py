"""SQLite warehouse: versioned migrations, staging load and SQL-based merges.

SQLite keeps the project runnable anywhere with zero services. The SQL is plain
ANSI-style DDL/DML (upserts, constraints, FKs), but it has only been executed on SQLite;
porting to PostgreSQL/BigQuery would need dialect changes (see README limitations).
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

from rideflow.errors import WarehouseError
from rideflow.gold import build_dim_date, to_records
from rideflow.logging_setup import get_logger, log_event

log = get_logger("warehouse")

STG_COLUMNS = [
    "trip_id", "rider_id", "driver_id", "city_id", "vehicle_type", "status", "first_event_ts",
    "requested_ts", "accepted_ts", "started_ts", "completed_ts", "cancelled_ts",
    "fare_amount", "currency", "distance_km", "wait_seconds", "duration_seconds", "cancel_reason",
]


def connect(path: Path, read_only: bool = False) -> sqlite3.Connection:
    path = Path(path)
    try:
        if read_only:
            if not path.is_file():
                raise WarehouseError(f"warehouse not found: {path}. Run 'rideflow init-db' or 'rideflow run' first.")
            conn = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(path)
    except sqlite3.Error as exc:
        raise WarehouseError(f"cannot open warehouse {path}: {exc}") from exc
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _run_script(conn: sqlite3.Connection, script: str, name: str) -> None:
    try:
        conn.executescript(script)
    except sqlite3.Error as exc:
        if conn.in_transaction:
            conn.rollback()
        raise WarehouseError(f"SQL script failed ({name}): {exc}") from exc


def migration_files(sql_dir: Path) -> list[Path]:
    return sorted((Path(sql_dir) / "schema").glob("*.sql"))


def migrate(conn: sqlite3.Connection, sql_dir: Path) -> list[str]:
    """Apply pending schema migrations in order; returns the versions applied now."""
    files = migration_files(sql_dir)
    if not files:
        raise WarehouseError(f"no migration files found in {Path(sql_dir) / 'schema'}")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY, applied_at TEXT NOT NULL)"
    )
    conn.commit()
    applied = {r["version"] for r in conn.execute("SELECT version FROM schema_migrations")}
    newly: list[str] = []
    for f in files:
        if f.stem in applied:
            continue
        _run_script(conn, "BEGIN;\n" + f.read_text(encoding="utf-8") + "\nCOMMIT;", f.name)
        conn.execute("INSERT INTO schema_migrations (version, applied_at) VALUES (?, CURRENT_TIMESTAMP)", (f.stem,))
        conn.commit()
        newly.append(f.stem)
        log_event(log, logging.INFO, "migration applied", version=f.stem)
    return newly


def upsert_cities(conn: sqlite3.Connection, cities: list[dict]) -> None:
    conn.executemany(
        """INSERT INTO dim_city (city_id, city_name, country, timezone, currency)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT (city_id) DO UPDATE SET city_name = excluded.city_name,
               country = excluded.country, timezone = excluded.timezone, currency = excluded.currency""",
        [(c["city_id"].strip().lower(), c["city_name"], c["country"], c["timezone"], c["currency"]) for c in cities],
    )
    conn.commit()


def load_staging(conn: sqlite3.Connection, trips, run_id: str) -> int:
    rows = [r + (run_id,) for r in to_records(trips, STG_COLUMNS)]
    placeholders = ", ".join("?" for _ in range(len(STG_COLUMNS) + 1))
    try:
        conn.execute("DELETE FROM stg_trips")
        conn.executemany(f"INSERT INTO stg_trips ({', '.join(STG_COLUMNS)}, run_id) VALUES ({placeholders})", rows)
        conn.commit()
    except sqlite3.Error as exc:
        conn.rollback()
        raise WarehouseError(f"staging load failed: {exc}") from exc
    return len(rows)


def load_dim_date(conn: sqlite3.Connection, trips) -> int:
    rows = build_dim_date(trips)
    conn.executemany(
        "INSERT OR IGNORE INTO dim_date (date_key, full_date, year, month, day, day_of_week, is_weekend) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    return len(rows)


def run_transforms(conn: sqlite3.Connection, sql_dir: Path) -> list[str]:
    ran = []
    for f in sorted((Path(sql_dir) / "transform").glob("*.sql")):
        _run_script(conn, f.read_text(encoding="utf-8"), f.name)
        ran.append(f.name)
    return ran


def load_gold(conn: sqlite3.Connection, cfg, cities: list[dict], trips, run_id: str) -> dict:
    migrate(conn, cfg.sql_dir)
    upsert_cities(conn, cities)
    staged = load_staging(conn, trips, run_id)
    dates = load_dim_date(conn, trips)
    try:
        run_transforms(conn, cfg.sql_dir)
    except WarehouseError:
        raise
    fact_rows = conn.execute("SELECT COUNT(*) FROM fact_trips").fetchone()[0]
    log_event(log, logging.INFO, "gold loaded", staged_trips=staged, dim_date_rows=dates, fact_trips=fact_rows)
    return {"staged_trips": staged, "dim_date_rows": dates, "fact_trips_total": fact_rows}
