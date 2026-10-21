-- Top drivers by completed trips.
SELECT d.driver_id, d.vehicle_type,
       COUNT(*) AS completed_trips,
       ROUND(AVG(f.duration_seconds) / 60.0, 1) AS avg_trip_minutes,
       ROUND(AVG(f.distance_km), 2) AS avg_distance_km
FROM fact_trips f
JOIN dim_driver d ON d.driver_key = f.driver_key
WHERE f.status = 'completed'
GROUP BY d.driver_id, d.vehicle_type
ORDER BY completed_trips DESC, d.driver_id
LIMIT 10;
