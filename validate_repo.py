#!/usr/bin/env python3
"""Repository hygiene gate: required files, SQL/YAML/DAG syntax, secrets scan, junk files.

Usage: python scripts/validate_repo.py   (exit 0 = clean, 1 = problems found)
"""
from __future__ import annotations

import ast
import re
import sqlite3
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
REQUIRED = [
    "README.md", "LICENSE", ".gitignore", ".env.example", "requirements.txt", "requirements-dev.txt",
    "pyproject.toml", "Makefile", "Dockerfile", "docker-compose.yml", "config/pipeline.yaml",
    ".github/workflows/ci.yml", "dags/rideflow_dag.py", "docs/architecture.md",
]
SECRET_PATTERNS = {
    "AWS access key": re.compile(r"AKIA[0-9A-Z]{16}"),
    "private key": re.compile(r"-----BEGIN (RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    "GitHub token": re.compile(r"gh[pousr]_[A-Za-z0-9]{30,}"),
    "hard-coded credential": re.compile(r"(?i)\b(password|passwd|secret|api[_-]?key|token)\s*[:=]\s*['\"][^'\"\s<>$]{6,}['\"]"),
}
JUNK = (".env", ".db", ".sqlite", ".pyc", ".log", ".tmp")
SKIP_DIRS = {".git", "__pycache__", ".venv", "venv", ".pytest_cache", "node_modules"}
SELF = Path(__file__).resolve()


def tracked_files() -> list[Path]:
    try:
        out = subprocess.run(["git", "ls-files"], cwd=ROOT, capture_output=True, text=True, check=True).stdout.split()
        if out:
            return [ROOT / p for p in out]
    except (OSError, subprocess.CalledProcessError):
        pass
    ignored = ("data/lake", "data/warehouse", "reports", "run.log", "build", "dist")
    return [p for p in ROOT.rglob("*") if p.is_file() and not (set(p.relative_to(ROOT).parts) & SKIP_DIRS)
            and not str(p.relative_to(ROOT).as_posix()).startswith(ignored) and not p.name.endswith((".egg-info",))
            and "egg-info" not in str(p)]


def main() -> int:
    problems: list[str] = []
    for rel in REQUIRED:
        if not (ROOT / rel).is_file():
            problems.append(f"missing required file: {rel}")

    schema = sorted((ROOT / "sql" / "schema").glob("*.sql"))
    if not schema:
        problems.append("no schema migrations found")
    mem = sqlite3.connect(":memory:")
    for f in schema:
        try:
            mem.executescript(f.read_text(encoding="utf-8"))
        except sqlite3.Error as exc:
            problems.append(f"SQL error in {f.relative_to(ROOT)}: {exc}")
    for f in sorted((ROOT / "sql" / "analytics").glob("*.sql")):
        try:
            mem.execute("EXPLAIN " + f.read_text(encoding="utf-8"))
        except sqlite3.Error as exc:
            problems.append(f"SQL error in {f.relative_to(ROOT)}: {exc}")

    for f in [*ROOT.glob("config/*.yaml"), *ROOT.glob(".github/workflows/*.yml"), ROOT / "docker-compose.yml"]:
        try:
            yaml.safe_load(f.read_text(encoding="utf-8"))
        except (yaml.YAMLError, OSError) as exc:
            problems.append(f"invalid YAML {f.relative_to(ROOT)}: {exc}")

    for f in [*ROOT.glob("src/**/*.py"), *ROOT.glob("dags/*.py"), *ROOT.glob("scripts/*.py"), *ROOT.glob("tests/*.py")]:
        try:
            ast.parse(f.read_text(encoding="utf-8"))
        except SyntaxError as exc:
            problems.append(f"syntax error {f.relative_to(ROOT)}: {exc}")

    for f in tracked_files():
        rel = f.relative_to(ROOT).as_posix()
        if f.name.endswith(JUNK) and f.name != ".env.example":
            problems.append(f"generated/secret-bearing file would be committed: {rel}")
            continue
        if f.resolve() == SELF or f.suffix in {".jsonl", ".csv"}:
            continue
        try:
            text = f.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        for label, pattern in SECRET_PATTERNS.items():
            if pattern.search(text):
                problems.append(f"possible {label} in {rel}")

    if problems:
        print("validate_repo: FAILED")
        for p in problems:
            print(f"  - {p}")
        return 1
    print(f"validate_repo: OK ({len(schema)} migrations, analytics SQL, YAML, Python syntax, secrets scan)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
