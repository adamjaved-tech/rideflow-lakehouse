"""Command-line interface: `rideflow <command>` or `python -m rideflow <command>`."""

from __future__ import annotations

import argparse
import json
import os
import sys

from rideflow import __version__
from rideflow.config import load_config
from rideflow.errors import ConfigError, DataQualityError, PipelineError
from rideflow.generator import generate_events, write_events
from rideflow.logging_setup import configure_logging


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="rideflow", description="RideFlow lakehouse pipeline")
    p.add_argument("--config", help="path to pipeline.yaml (default: $RIDEFLOW_CONFIG or config/pipeline.yaml)")
    p.add_argument("--version", action="version", version=f"rideflow {__version__}")
    sub = p.add_subparsers(dest="command", required=True)

    g = sub.add_parser("generate", help="write deterministic synthetic events (JSON lines)")
    g.add_argument("--out", default="data/sample/events.jsonl")
    g.add_argument("--trips", type=int, default=200)
    g.add_argument("--seed", type=int, default=42)
    g.add_argument("--defect-rate", type=float, default=0.03)

    sub.add_parser("init-db", help="apply warehouse migrations")
    r = sub.add_parser("run", help="run the full pipeline")
    r.add_argument("--run-id")
    s = sub.add_parser("stage", help="run a single stage (used by the Airflow DAG)")
    s.add_argument("name")
    s.add_argument("--run-id", required=True)
    sub.add_parser("health", help="health check; exit code 1 when unhealthy")
    q = sub.add_parser("query", help="run an analytics query")
    q.add_argument("name", nargs="?")
    return p


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    configure_logging(os.environ.get("RIDEFLOW_LOG_LEVEL", "INFO"))
    try:
        if args.command == "generate":
            lines = generate_events(n_trips=args.trips, seed=args.seed, defect_rate=args.defect_rate)
            out = write_events(args.out, lines)
            print(f"wrote {len(lines)} lines to {out}")
            return 0

        cfg = load_config(args.config)
        if args.command == "init-db":
            from rideflow.pipeline import new_run_id, stage_init_db
            stage_init_db(cfg, new_run_id())
            print("warehouse ready")
        elif args.command == "run":
            from rideflow.pipeline import run_pipeline
            report = run_pipeline(cfg, args.run_id)
            failed_warnings = [c["name"] for c in report["checks"] if not c["passed"]]
            print(json.dumps({k: report[k] for k in ("run_id", "status", "duration_seconds", "records_total",
                                                     "records_rejected", "trips_loaded")}, indent=2))
            if failed_warnings:
                print(f"warnings: {', '.join(failed_warnings)}", file=sys.stderr)
        elif args.command == "stage":
            from rideflow.pipeline import run_stage
            run_stage(cfg, args.name, args.run_id)
        elif args.command == "health":
            from rideflow.monitoring import health_check
            result = health_check(cfg)
            print(json.dumps(result, indent=2))
            return 0 if result["healthy"] else 1
        elif args.command == "query":
            from rideflow.queries import format_table, list_queries, run_query
            if not args.name:
                print("\n".join(list_queries(cfg)))
            else:
                print(format_table(*run_query(cfg, args.name)))
        return 0
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2
    except DataQualityError as exc:
        print(f"data-quality gate failed: {exc}", file=sys.stderr)
        return 1
    except PipelineError as exc:
        print(f"pipeline error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
