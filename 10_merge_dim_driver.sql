-- Upsert drivers observed in staging. Re-running with the same data is a no-op.
BEGIN;
INSERT INTO dim_driver (driver_id, vehicle_type, first_seen_ts)
SELECT driver_id, MAX(vehicle_type), MIN(first_event_ts)
FROM stg_trips
WHERE driver_id IS NOT NULL
GROUP BY driver_id
ON CONFLICT (driver_id) DO UPDATE SET
    vehicle_type  = COALESCE(excluded.vehicle_type, dim_driver.vehicle_type),
    first_seen_ts = MIN(dim_driver.first_seen_ts, excluded.first_seen_ts);
COMMIT;
