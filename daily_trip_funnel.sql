-- Daily demand funnel: requested trips and their final status.
SELECT dd.full_date AS trip_date,
       COUNT(*) AS trips_requested,
       SUM(f.status = 'completed') AS completed,
       SUM(f.status = 'cancelled') AS cancelled,
       SUM(f.status = 'open')      AS still_open,
       ROUND(100.0 * SUM(f.status = 'completed') / COUNT(*), 1) AS completion_pct
FROM fact_trips f
JOIN dim_date dd ON dd.date_key = f.date_key
GROUP BY dd.full_date
ORDER BY dd.full_date;
