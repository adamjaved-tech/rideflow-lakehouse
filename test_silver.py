import json
import unittest

from helpers import event
from rideflow.silver import validate_line
from rideflow.timeutil import parse_ts

AS_OF = parse_ts("2026-01-05T00:00:00Z")
CITIES = {"lahore", "dubai"}


def check(obj_or_raw):
    raw = obj_or_raw if isinstance(obj_or_raw, str) else json.dumps(obj_or_raw)
    return validate_line(raw, AS_OF, CITIES)


class ValidationTests(unittest.TestCase):
    def test_valid_event_is_normalised(self):
        rec, reasons = check(event(event_type=" Requested ", city_id="LAHORE", event_ts="2026-01-01T10:00:00+05:00"))
        self.assertEqual(reasons, [])
        self.assertEqual(rec["event_type"], "requested")
        self.assertEqual(rec["city_id"], "lahore")
        self.assertEqual(rec["event_ts"], "2026-01-01T05:00:00Z")  # converted to UTC

    def test_rejections(self):
        cases = {
            "malformed_json": '{"event_id": "x", ',
            "empty_record": "   ",
            "not_a_json_object": "[1, 2]",
            "missing_trip_id": event(trip_id=None),
            "missing_event_id": event(event_id=""),
            "unknown_event_type": event(event_type="teleported"),
            "unknown_city_id": event(city_id="atlantis"),
            "invalid_event_ts": event(event_ts="not-a-timestamp"),
            "missing_event_ts": event(event_ts=None),
            "event_ts_in_future": event(event_ts="2099-01-01T00:00:00Z"),
            "negative_fare_amount": event(event_type="completed", fare_amount=-5, distance_km=3),
            "invalid_fare_amount": event(event_type="completed", fare_amount="abc", distance_km=3),
            "completed_missing_fare_amount": event(event_type="completed", distance_km=3),
            "completed_missing_distance_km": event(event_type="completed", fare_amount=3),
            "accepted_missing_driver_id": event(event_type="accepted"),
        }
        for expected, payload in cases.items():
            with self.subTest(expected):
                rec, reasons = check(payload)
                self.assertIsNone(rec)
                self.assertEqual(reasons[0], expected)

    def test_boolean_and_nan_amounts_rejected(self):
        for bad in (True, "NaN", "inf"):
            with self.subTest(bad):
                rec, reasons = check(event(event_type="completed", fare_amount=bad, distance_km=2))
                self.assertIsNone(rec)
                self.assertIn("invalid_fare_amount", reasons)

    def test_numeric_ids_accepted_as_strings(self):
        rec, reasons = check(event(trip_id=12345))
        self.assertEqual(reasons, [])
        self.assertEqual(rec["trip_id"], "12345")


if __name__ == "__main__":
    unittest.main()
