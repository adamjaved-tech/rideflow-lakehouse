# Sample run report

Produced by `rideflow run` on the committed sample data (synthetic). Timings vary per machine.

## Run report - run-20261018T213946Z-4e1c12

- Status: **success**
- Duration: 0.753s (SLA 300s, breached: False)
- Records: 782 total, 21 rejected, 18 duplicates dropped
- Trips loaded: 200
- Reject reasons: {'event_ts_in_future': 2, 'invalid_event_ts': 4, 'malformed_json': 3, 'missing_trip_id': 7, 'unknown_event_type': 5}

### Data-quality report - run-20261018T213946Z-4e1c12

| Check | Layer | Severity | Result | Observed | Expected |
|---|---|---|---|---|---|
| silver_not_empty | silver | critical | PASS | 743 | > 0 |
| silver_reject_rate | silver | critical | PASS | 0.0269 | <= 0.08 |
| silver_duplicate_rate | silver | warning | PASS | 0.0230 | <= 0.05 |
| silver_no_null_keys | silver | critical | PASS | 0 | 0 |
| silver_unique_event_id | silver | critical | PASS | 0 | 0 |
| silver_non_negative_amounts | silver | critical | PASS | 0 | 0 |
| silver_freshness | silver | warning | PASS | 23.7h | <= 48.0h |
| gold_foreign_keys | gold | critical | PASS | 0 | 0 |
| gold_reconciliation | gold | critical | PASS | 0 | 0 |
| gold_completed_have_fare | gold | critical | PASS | 0 | 0 |
| gold_timestamp_order | gold | warning | PASS | 0 | 0 |
| gold_cancel_rate_anomaly | gold | warning | PASS | 0.1000 | <= 0.6 |
| run_sla | ops | warning | PASS | 0.8s | <= 300.0s |

## Query: revenue_by_city

```
city_name  currency  completed_trips  revenue   avg_fare  avg_distance_km
---------  --------  ---------------  --------  --------  ---------------
Karachi    PKR       39               39997.23  1025.57   15.58          
Lahore     PKR       35               32083.23  916.66    13.66          
Cairo      EGP       32               4637.88   144.93    15.69          
Dubai      AED       36               1411.43   39.21     13.85          
Riyadh     SAR       30               1025.44   34.18     13.95          
```

## Query: daily_trip_funnel

```
trip_date   trips_requested  completed  cancelled  still_open  completion_pct
----------  ---------------  ---------  ---------  ----------  --------------
2026-01-01  77               66         11         0           85.7          
2026-01-02  58               51         4          3           87.9          
2026-01-03  65               55         5          5           84.6          
```
