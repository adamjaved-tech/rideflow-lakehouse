import sqlite3
import tempfile
import unittest
from pathlib import Path

from helpers import make_project
from rideflow import warehouse
from rideflow.config import load_cities
from rideflow.errors import WarehouseError
from rideflow.gold import build_trips
from rideflow.silver import build_silver, SILVER_COLUMNS

import pandas as pd


def tiny_trips():
    rows = []
    for tid, status, fare in (("T1", "completed", 100.0), ("T2", "cancelled", None)):
        base = {c: None for c in SILVER_COLUMNS}
        base.update(event_id=f"{tid}-1", trip_id=tid, rider_id="r", city_id="dubai", currency="AED",
                    event_type="requested", event_ts="2026-01-01T10:00:00Z")
        rows.append(dict(base))
        if status == "completed":
            rows.append({**base, "event_id": f"{tid}-2", "event_type": "completed", "event_ts": "2026-01-01T10:20:00Z",
                         "driver_id": "d1", "vehicle_type": "economy", "fare_amount": fare, "distance_km": 5.0})
        else:
            rows.append({**base, "event_id": f"{tid}-2", "event_type": "cancelled", "event_ts": "2026-01-01T10:02:00Z",
                         "cancel_reason": "rider_cancelled"})
    return build_trips(pd.DataFrame(rows, columns=SILVER_COLUMNS))


class WarehouseTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.cfg = make_project(self.tmp)
        self.conn = warehouse.connect(self.cfg.warehouse_path)
        self.addCleanup(self.conn.close)

    def test_migrations_idempotent(self):
        first = warehouse.migrate(self.conn, self.cfg.sql_dir)
        self.assertEqual(len(first), 3)
        self.assertEqual(warehouse.migrate(self.conn, self.cfg.sql_dir), [])

    def test_missing_migrations_dir(self):
        with self.assertRaises(WarehouseError):
            warehouse.migrate(self.conn, self.tmp / "no-sql-here")

    def test_load_is_idempotent_and_sets_keys(self):
        trips = tiny_trips()
        for _ in range(2):
            warehouse.load_gold(self.conn, self.cfg, load_cities(self.cfg), trips, "run-1")
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM fact_trips").fetchone()[0], 2)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM dim_driver").fetchone()[0], 1)
        row = self.conn.execute("SELECT * FROM vw_trip_detail WHERE trip_id='T1'").fetchone()
        self.assertEqual((row["city_name"], row["trip_date"], row["fare_amount"]), ("Dubai", "2026-01-01", 100.0))

    def test_upsert_updates_changed_status(self):
        trips = tiny_trips()
        warehouse.load_gold(self.conn, self.cfg, load_cities(self.cfg), trips, "run-1")
        key_before = self.conn.execute("SELECT trip_key FROM fact_trips WHERE trip_id='T2'").fetchone()[0]
        changed = trips.copy()
        changed.loc[changed["trip_id"] == "T2", ["status", "cancel_reason"]] = ["open", None]
        warehouse.load_gold(self.conn, self.cfg, load_cities(self.cfg), changed, "run-2")
        row = self.conn.execute("SELECT trip_key, status, updated_run_id FROM fact_trips WHERE trip_id='T2'").fetchone()
        self.assertEqual((row["trip_key"], row["status"], row["updated_run_id"]), (key_before, "open", "run-2"))

    def test_constraints_enforced(self):
        warehouse.migrate(self.conn, self.cfg.sql_dir)
        with self.assertRaises(sqlite3.IntegrityError):  # FK to missing city/date
            self.conn.execute(
                "INSERT INTO fact_trips (trip_id, city_key, date_key, status, loaded_at, updated_run_id) "
                "VALUES ('X', 999, 20260101, 'open', 'now', 'r')")
        self.conn.rollback()
        warehouse.upsert_cities(self.conn, load_cities(self.cfg))
        self.conn.execute("INSERT INTO dim_date VALUES (20260101,'2026-01-01',2026,1,1,4,0)")
        with self.assertRaises(sqlite3.IntegrityError):  # completed without fare
            self.conn.execute(
                "INSERT INTO fact_trips (trip_id, city_key, date_key, status, loaded_at, updated_run_id) "
                "VALUES ('X', 1, 20260101, 'completed', 'now', 'r')")
        with self.assertRaises(sqlite3.IntegrityError):  # bad status
            self.conn.execute(
                "INSERT INTO fact_trips (trip_id, city_key, date_key, status, loaded_at, updated_run_id) "
                "VALUES ('Y', 1, 20260101, 'weird', 'now', 'r')")

    def test_read_only_connection_rejects_writes(self):
        warehouse.migrate(self.conn, self.cfg.sql_dir)
        ro = warehouse.connect(self.cfg.warehouse_path, read_only=True)
        self.addCleanup(ro.close)
        with self.assertRaises(sqlite3.OperationalError):
            ro.execute("DELETE FROM dim_city")

    def test_read_only_missing_db(self):
        with self.assertRaises(WarehouseError):
            warehouse.connect(self.tmp / "missing.db", read_only=True)


if __name__ == "__main__":
    unittest.main()
