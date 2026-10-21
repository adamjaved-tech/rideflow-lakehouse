"""Health check: can we reach the warehouse, is the schema current, did the last run succeed?"""

from __future__ import annotations

from rideflow import warehouse
from rideflow.errors import WarehouseError
from rideflow.timeutil import parse_ts, utc_now


def health_check(cfg) -> dict:
    checks: dict[str, dict] = {}
    try:
        conn = warehouse.connect(cfg.warehouse_path, read_only=True)
    except WarehouseError as exc:
        return {"healthy": False, "checks": {"warehouse_reachable": {"ok": False, "detail": str(exc)}}}
    try:
        checks["warehouse_reachable"] = {"ok": True, "detail": str(cfg.warehouse_path.name)}
        try:
            applied = {r["version"] for r in conn.execute("SELECT version FROM schema_migrations")}
            expected = {f.stem for f in warehouse.migration_files(cfg.sql_dir)}
            missing = sorted(expected - applied)
            checks["schema_current"] = {"ok": not missing, "detail": f"missing migrations: {missing}" if missing else "up to date"}
            last = conn.execute(
                "SELECT run_id, status, finished_at FROM pipeline_runs ORDER BY finished_at DESC LIMIT 1"
            ).fetchone()
        except Exception as exc:  # noqa: BLE001 - report any DB problem as unhealthy
            checks["schema_current"] = {"ok": False, "detail": f"cannot read schema: {exc}"}
            last = None
        if last is None:
            checks["last_run_succeeded"] = {"ok": False, "detail": "no pipeline run recorded"}
        else:
            age_h = (utc_now() - parse_ts(last["finished_at"])).total_seconds() / 3600
            checks["last_run_succeeded"] = {"ok": last["status"] == "success", "detail": f"{last['run_id']} -> {last['status']}"}
            checks["last_run_recent"] = {
                "ok": age_h <= cfg.health_max_run_age_hours,
                "detail": f"{age_h:.1f}h ago (limit {cfg.health_max_run_age_hours}h)",
            }
    finally:
        conn.close()
    return {"healthy": all(c["ok"] for c in checks.values()), "checks": checks}
