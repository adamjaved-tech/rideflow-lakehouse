-- Fact table (grain: one row per trip) and operational audit tables.

CREATE TABLE IF NOT EXISTS fact_trips (
    trip_key         INTEGER PRIMARY KEY AUTOINCREMENT,
    trip_id          TEXT    NOT NULL UNIQUE,
    rider_id         TEXT,                                   -- degenerate dimension (pseudonymous id)
    driver_key       INTEGER REFERENCES dim_driver (driver_key),
    city_key         INTEGER NOT NULL REFERENCES dim_city (city_key),
    date_key         INTEGER NOT NULL REFERENCES dim_date (date_key),
    status           TEXT    NOT NULL CHECK (status IN ('completed', 'cancelled', 'open')),
    requested_ts     TEXT,
    accepted_ts      TEXT,
    started_ts       TEXT,
    completed_ts     TEXT,
    cancelled_ts     TEXT,
    fare_amount      REAL CHECK (fare_amount IS NULL OR fare_amount >= 0),
    currency         TEXT,
    distance_km      REAL CHECK (distance_km IS NULL OR distance_km >= 0),
    wait_seconds     INTEGER,
    duration_seconds INTEGER,
    cancel_reason    TEXT,
    loaded_at        TEXT    NOT NULL,
    updated_run_id   TEXT    NOT NULL,
    CHECK (status <> 'completed' OR fare_amount IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id           TEXT PRIMARY KEY,
    status           TEXT NOT NULL CHECK (status IN ('success', 'failed')),
    started_at       TEXT NOT NULL,
    finished_at      TEXT NOT NULL,
    duration_seconds REAL NOT NULL,
    records_total    INTEGER,
    records_rejected INTEGER,
    trips_loaded     INTEGER,
    sla_breached     INTEGER NOT NULL DEFAULT 0,
    error_message    TEXT
);

CREATE TABLE IF NOT EXISTS dq_results (
    run_id     TEXT NOT NULL REFERENCES pipeline_runs (run_id),
    check_name TEXT NOT NULL,
    layer      TEXT NOT NULL,
    severity   TEXT NOT NULL CHECK (severity IN ('critical', 'warning')),
    passed     INTEGER NOT NULL CHECK (passed IN (0, 1)),
    observed   TEXT,
    expected   TEXT,
    message    TEXT,
    PRIMARY KEY (run_id, check_name)
);
