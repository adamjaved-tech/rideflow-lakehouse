SELECT COALESCE(f.cancel_reason, 'unknown') AS cancel_reason,
       COUNT(*) AS cancelled_trips,
       ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM fact_trips WHERE status = 'cancelled'), 1) AS pct_of_cancellations
FROM fact_trips f
WHERE f.status = 'cancelled'
GROUP BY COALESCE(f.cancel_reason, 'unknown')
ORDER BY cancelled_trips DESC;
