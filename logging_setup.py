"""Structured JSON logging with a per-run correlation id and secret redaction."""

from __future__ import annotations

import contextvars
import json
import logging
import re
import sys
from datetime import datetime, timezone

_run_id: contextvars.ContextVar[str] = contextvars.ContextVar("run_id", default="-")

SENSITIVE_KEYS = ("password", "passwd", "secret", "token", "api_key", "apikey", "credential", "sasl")
_URL_CREDENTIALS = re.compile(r"(://[^/\s:@]+):([^@\s/]+)@")
MASK = "***REDACTED***"


def set_run_id(run_id: str) -> None:
    _run_id.set(run_id)


def redact_text(text: str) -> str:
    """Mask credentials embedded in URLs, e.g. postgres://user:pw@host."""
    return _URL_CREDENTIALS.sub(r"\1:" + MASK + "@", text)


def redact(value):
    """Recursively mask values whose key looks sensitive; scrub URL credentials in strings."""
    if isinstance(value, dict):
        return {
            k: (MASK if any(s in str(k).lower() for s in SENSITIVE_KEYS) else redact(v))
            for k, v in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [redact(v) for v in value]
    if isinstance(value, str):
        return redact_text(value)
    return value


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "level": record.levelname,
            "logger": record.name,
            "run_id": _run_id.get(),
            "message": redact_text(record.getMessage()),
        }
        ctx = getattr(record, "ctx", None)
        if ctx:
            payload.update(redact(ctx))
        if record.exc_info:
            payload["exception"] = redact_text(self.formatException(record.exc_info))
        return json.dumps(payload, default=str, sort_keys=False)


def configure_logging(level: str = "INFO") -> None:
    root = logging.getLogger("rideflow")
    root.setLevel(level.upper())
    if not any(getattr(h, "_rideflow", False) for h in root.handlers):
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(JsonFormatter())
        handler._rideflow = True  # type: ignore[attr-defined]
        root.addHandler(handler)
    root.propagate = False


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(f"rideflow.{name}")


def log_event(logger: logging.Logger, level: int, message: str, **ctx) -> None:
    logger.log(level, message, extra={"ctx": ctx})
