"""Run the version-controlled analytics SQL files read-only against the warehouse."""

from __future__ import annotations

import re
from pathlib import Path

from rideflow import warehouse
from rideflow.errors import PipelineError

_NAME = re.compile(r"^[a-z][a-z0-9_]{0,60}$")


def list_queries(cfg) -> list[str]:
    return sorted(p.stem for p in (Path(cfg.sql_dir) / "analytics").glob("*.sql"))


def run_query(cfg, name: str) -> tuple[list[str], list[tuple]]:
    if not _NAME.match(name):
        raise PipelineError("invalid query name")
    path = Path(cfg.sql_dir) / "analytics" / f"{name}.sql"
    if not path.is_file():
        raise PipelineError(f"unknown query '{name}'. Available: {', '.join(list_queries(cfg))}")
    conn = warehouse.connect(cfg.warehouse_path, read_only=True)
    try:
        cur = conn.execute(path.read_text(encoding="utf-8"))
        columns = [d[0] for d in cur.description]
        return columns, [tuple(r) for r in cur.fetchall()]
    finally:
        conn.close()


def format_table(columns: list[str], rows: list[tuple]) -> str:
    cells = [[str(c) for c in columns]] + [["" if v is None else str(v) for v in r] for r in rows]
    widths = [max(len(r[i]) for r in cells) for i in range(len(columns))]
    fmt = lambda r: "  ".join(v.ljust(w) for v, w in zip(r, widths))  # noqa: E731
    lines = [fmt(cells[0]), "  ".join("-" * w for w in widths)] + [fmt(r) for r in cells[1:]]
    return "\n".join(lines)
