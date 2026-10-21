-- Completed-trip revenue per city (local currency; no FX conversion is applied).
SELECT c.city_name, f.currency,
       COUNT(*) AS completed_trips,
       ROUND(SUM(f.fare_amount), 2) AS revenue,
       ROUND(AVG(f.fare_amount), 2) AS avg_fare,
       ROUND(AVG(f.distance_km), 2) AS avg_distance_km
FROM fact_trips f
JOIN dim_city c ON c.city_key = f.city_key
WHERE f.status = 'completed'
GROUP BY c.city_name, f.currency
ORDER BY revenue DESC;
