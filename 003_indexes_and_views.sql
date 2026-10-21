CREATE INDEX IF NOT EXISTS ix_fact_trips_date    ON fact_trips (date_key);
CREATE INDEX IF NOT EXISTS ix_fact_trips_city    ON fact_trips (city_key, status);
CREATE INDEX IF NOT EXISTS ix_fact_trips_driver  ON fact_trips (driver_key);
CREATE INDEX IF NOT EXISTS ix_fact_trips_status  ON fact_trips (status);
CREATE INDEX IF NOT EXISTS ix_pipeline_runs_fin  ON pipeline_runs (finished_at);

CREATE VIEW IF NOT EXISTS vw_trip_detail AS
SELECT f.trip_id, f.status, dd.full_date AS trip_date, c.city_name, c.country,
       d.driver_id, d.vehicle_type, f.fare_amount, f.currency, f.distance_km,
       f.wait_seconds, f.duration_seconds, f.cancel_reason
FROM fact_trips f
JOIN dim_city c  ON c.city_key  = f.city_key
JOIN dim_date dd ON dd.date_key = f.date_key
LEFT JOIN dim_driver d ON d.driver_key = f.driver_key;
