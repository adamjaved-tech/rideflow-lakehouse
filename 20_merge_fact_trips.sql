-- Idempotent merge of staged trips into the fact table (key: trip_id).
-- The WHERE clause is required by SQLite to disambiguate INSERT..SELECT..ON CONFLICT.
BEGIN;
INSERT INTO fact_trips (
    trip_id, rider_id, driver_key, city_key, date_key, status,
    requested_ts, accepted_ts, started_ts, completed_ts, cancelled_ts,
    fare_amount, currency, distance_km, wait_seconds, duration_seconds,
    cancel_reason, loaded_at, updated_run_id)
SELECT s.trip_id, s.rider_id, d.driver_key, c.city_key,
       CAST(strftime('%Y%m%d', s.first_event_ts) AS INTEGER), s.status,
       s.requested_ts, s.accepted_ts, s.started_ts, s.completed_ts, s.cancelled_ts,
       s.fare_amount, s.currency, s.distance_km, s.wait_seconds, s.duration_seconds,
       s.cancel_reason, CURRENT_TIMESTAMP, s.run_id
FROM stg_trips s
JOIN dim_city c ON c.city_id = s.city_id
LEFT JOIN dim_driver d ON d.driver_id = s.driver_id
WHERE true
ON CONFLICT (trip_id) DO UPDATE SET
    rider_id         = excluded.rider_id,
    driver_key       = excluded.driver_key,
    city_key         = excluded.city_key,
    date_key         = excluded.date_key,
    status           = excluded.status,
    requested_ts     = excluded.requested_ts,
    accepted_ts      = excluded.accepted_ts,
    started_ts       = excluded.started_ts,
    completed_ts     = excluded.completed_ts,
    cancelled_ts     = excluded.cancelled_ts,
    fare_amount      = excluded.fare_amount,
    currency         = excluded.currency,
    distance_km      = excluded.distance_km,
    wait_seconds     = excluded.wait_seconds,
    duration_seconds = excluded.duration_seconds,
    cancel_reason    = excluded.cancel_reason,
    loaded_at        = excluded.loaded_at,
    updated_run_id   = excluded.updated_run_id;
COMMIT;
