-- Dimension tables and the staging table. Idempotent (IF NOT EXISTS).

CREATE TABLE IF NOT EXISTS dim_city (
    city_key   INTEGER PRIMARY KEY AUTOINCREMENT,
    city_id    TEXT NOT NULL UNIQUE,
    city_name  TEXT NOT NULL,
    country    TEXT NOT NULL,
    timezone   TEXT NOT NULL,
    currency   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_driver (
    driver_key    INTEGER PRIMARY KEY AUTOINCREMENT,
    driver_id     TEXT NOT NULL UNIQUE,
    vehicle_type  TEXT,
    first_seen_ts TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key    INTEGER PRIMARY KEY,           -- YYYYMMDD
    full_date   TEXT NOT NULL UNIQUE,
    year        INTEGER NOT NULL,
    month       INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
    day         INTEGER NOT NULL CHECK (day BETWEEN 1 AND 31),
    day_of_week INTEGER NOT NULL CHECK (day_of_week BETWEEN 1 AND 7),  -- ISO: 1=Mon
    is_weekend  INTEGER NOT NULL CHECK (is_weekend IN (0, 1))
);

-- Staging: truncated and reloaded on every run, then merged into dims/facts by sql/transform.
CREATE TABLE IF NOT EXISTS stg_trips (
    trip_id          TEXT PRIMARY KEY,
    rider_id         TEXT,
    driver_id        TEXT,
    city_id          TEXT NOT NULL,
    vehicle_type     TEXT,
    status           TEXT NOT NULL,
    first_event_ts   TEXT NOT NULL,
    requested_ts     TEXT,
    accepted_ts      TEXT,
    started_ts       TEXT,
    completed_ts     TEXT,
    cancelled_ts     TEXT,
    fare_amount      REAL,
    currency         TEXT,
    distance_km      REAL,
    wait_seconds     INTEGER,
    duration_seconds INTEGER,
    cancel_reason    TEXT,
    run_id           TEXT NOT NULL
);
