import unittest

import pandas as pd

from rideflow.gold import TRIP_COLUMNS, build_dim_date, build_trips, to_records
from rideflow.silver import SILVER_COLUMNS


def frame(rows):
    full = []
    for i, r in enumerate(rows):
        base = {c: None for c in SILVER_COLUMNS}
        base.update({"event_id": f"e{i}", "rider_id": "r1", "city_id": "lahore", "currency": "PKR"})
        base.update(r)
        full.append(base)
    return pd.DataFrame(full, columns=SILVER_COLUMNS)


class TripBuildTests(unittest.TestCase):
    def test_completed_cancelled_open(self):
        df = frame([
            {"trip_id": "A", "event_type": "requested", "event_ts": "2026-01-01T10:00:00Z"},
            {"trip_id": "A", "event_type": "accepted", "event_ts": "2026-01-01T10:01:00Z", "driver_id": "d1", "vehicle_type": "economy"},
            {"trip_id": "A", "event_type": "started", "event_ts": "2026-01-01T10:05:00Z", "driver_id": "d1"},
            {"trip_id": "A", "event_type": "completed", "event_ts": "2026-01-01T10:25:00Z", "driver_id": "d1", "fare_amount": 500.0, "distance_km": 9.5},
            {"trip_id": "B", "event_type": "requested", "event_ts": "2026-01-01T11:00:00Z"},
            {"trip_id": "B", "event_type": "cancelled", "event_ts": "2026-01-01T11:02:00Z", "cancel_reason": "no_driver_found"},
            {"trip_id": "C", "event_type": "requested", "event_ts": "2026-01-02T09:00:00Z"},
            {"trip_id": "C", "event_type": "accepted", "event_ts": "2026-01-02T09:00:30Z", "driver_id": "d2"},
        ])
        trips = build_trips(df).set_index("trip_id")
        self.assertEqual(list(build_trips(df).columns), TRIP_COLUMNS)
        self.assertEqual(trips.loc["A", "status"], "completed")
        self.assertEqual(trips.loc["A", "wait_seconds"], 60)
        self.assertEqual(trips.loc["A", "duration_seconds"], 1200)
        self.assertEqual(trips.loc["A", "fare_amount"], 500.0)
        self.assertEqual(trips.loc["A", "driver_id"], "d1")
        self.assertEqual(trips.loc["B", "status"], "cancelled")
        self.assertEqual(trips.loc["B", "cancel_reason"], "no_driver_found")
        self.assertTrue(pd.isna(trips.loc["B", "fare_amount"]))
        self.assertEqual(trips.loc["C", "status"], "open")

    def test_input_order_does_not_matter(self):
        rows = [
            {"trip_id": "A", "event_type": "requested", "event_ts": "2026-01-01T10:00:00Z"},
            {"trip_id": "A", "event_type": "started", "event_ts": "2026-01-01T10:05:00Z"},
            {"trip_id": "A", "event_type": "completed", "event_ts": "2026-01-01T10:25:00Z", "fare_amount": 90.0, "distance_km": 4.0},
        ]
        forward = build_trips(frame(rows))
        backward = build_trips(frame(list(reversed(rows))))
        pd.testing.assert_frame_equal(forward, backward)

    def test_trip_missing_request_event_still_loads(self):
        df = frame([{"trip_id": "Z", "event_type": "started", "event_ts": "2026-01-03T01:00:00Z"}])
        trips = build_trips(df)
        self.assertEqual(trips.loc[0, "first_event_ts"], "2026-01-03T01:00:00Z")
        self.assertTrue(pd.isna(trips.loc[0, "requested_ts"]))

    def test_empty_input(self):
        self.assertTrue(build_trips(frame([])).empty)

    def test_dim_date_and_records(self):
        df = frame([
            {"trip_id": "A", "event_type": "requested", "event_ts": "2026-01-01T10:00:00Z"},
            {"trip_id": "B", "event_type": "requested", "event_ts": "2026-01-03T10:00:00Z"},
        ])
        trips = build_trips(df)
        dates = build_dim_date(trips)
        self.assertEqual([d[0] for d in dates], [20260101, 20260102, 20260103])
        self.assertEqual(dates[0][5:], (4, 0))  # Thursday, not weekend
        self.assertEqual(dates[2][5:], (6, 1))  # Saturday -> weekend
        recs = to_records(trips, ["trip_id", "wait_seconds", "fare_amount"])
        self.assertIsNone(recs[0][1])
        self.assertIsNone(recs[0][2])


if __name__ == "__main__":
    unittest.main()
