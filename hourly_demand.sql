-- Demand by hour of day (UTC) with average driver-acceptance wait.
SELECT strftime('%H', f.requested_ts) AS hour_utc,
       COUNT(*) AS requests,
       ROUND(AVG(f.wait_seconds), 0) AS avg_wait_seconds
FROM fact_trips f
WHERE f.requested_ts IS NOT NULL
GROUP BY hour_utc
ORDER BY hour_utc;
