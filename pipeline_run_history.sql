SELECT run_id, status, finished_at, ROUND(duration_seconds, 2) AS duration_s,
       records_total, records_rejected, trips_loaded, sla_breached
FROM pipeline_runs
ORDER BY finished_at DESC
LIMIT 20;
