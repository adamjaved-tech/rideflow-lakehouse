import json
import unittest

from rideflow.generator import generate_events


class GeneratorTests(unittest.TestCase):
    def test_deterministic(self):
        self.assertEqual(generate_events(50, seed=1), generate_events(50, seed=1))

    def test_seed_changes_output(self):
        self.assertNotEqual(generate_events(50, seed=1), generate_events(50, seed=2))

    def test_clean_when_no_defects(self):
        lines = generate_events(60, seed=3, defect_rate=0.0)
        parsed = [json.loads(x) for x in lines]
        ids = [p["event_id"] for p in parsed]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertTrue(all(p["trip_id"] for p in parsed))

    def test_defects_are_injected(self):
        lines = generate_events(300, seed=4, defect_rate=0.1)
        bad_json = 0
        for ln in lines:
            try:
                json.loads(ln)
            except json.JSONDecodeError:
                bad_json += 1
        self.assertGreater(bad_json, 0)
        self.assertGreater(len(lines), len(set(lines)))  # duplicates present

    def test_invalid_arguments(self):
        with self.assertRaises(ValueError):
            generate_events(0)
        with self.assertRaises(ValueError):
            generate_events(10, defect_rate=0.9)


if __name__ == "__main__":
    unittest.main()
