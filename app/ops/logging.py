"""Small structured-logging helper for pipelines and workers.

Output goes to stdout (so `docker compose logs` shows it). By default each
line is human-readable: the message followed by any context fields, e.g.

    Shadow scheduler started.  [run_id=12 provider=rentcast]

Set LOG_FORMAT=json to emit one JSON object per line instead.

Context (run_id, provider, alert_id, ...) is carried in a contextvar so
callers write `with log_context(run_id=run_id): log.info(...)` once.
Secrets are never logged: fields whose names look secret are redacted and
URLs inside messages are replaced, so webhook/heartbeat URLs cannot leak.
"""
from __future__ import annotations

import contextlib
import contextvars
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from typing import Any, Iterator

ROOT_LOGGER_NAME = "app"
_SENSITIVE_NAME = re.compile(r"url|secret|token|key|password|webhook", re.I)
_URL = re.compile(r"https?://\S+", re.I)
_RESERVED = set(logging.makeLogRecord({}).__dict__) | {"message", "asctime"}

_context: contextvars.ContextVar[dict[str, Any]] = contextvars.ContextVar(
    "log_context", default={}
)


@contextlib.contextmanager
def log_context(**fields: Any) -> Iterator[None]:
    """Attach fields (None values skipped) to every log line in the block."""

    merged = {**_context.get(), **{k: v for k, v in fields.items() if v is not None}}
    token = _context.set(merged)
    try:
        yield
    finally:
        _context.reset(token)


def current_context() -> dict[str, Any]:
    return dict(_context.get())


def _clean_message(message: str) -> str:
    return _URL.sub("<url>", message)


def _fields(record: logging.LogRecord) -> dict[str, Any]:
    fields = current_context()
    for key, value in record.__dict__.items():
        if key not in _RESERVED and not key.startswith("_"):
            fields[key] = value
    return {
        key: "<redacted>" if _SENSITIVE_NAME.search(key) else value
        for key, value in fields.items()
    }


class TextFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        line = _clean_message(record.getMessage())
        fields = _fields(record)
        if fields:
            line += "  [" + " ".join(f"{k}={v}" for k, v in fields.items()) + "]"
        if record.levelno >= logging.WARNING:
            line = f"{record.levelname}: {line}"
        if record.exc_info:
            line += "\n" + _clean_message(self.formatException(record.exc_info))
        return line


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": _clean_message(record.getMessage()),
            **_fields(record),
        }
        if record.exc_info:
            payload["exception"] = _clean_message(self.formatException(record.exc_info))
        return json.dumps(payload, default=str)


class _StdoutHandler(logging.StreamHandler):
    """Resolve sys.stdout on every emit so redirection (and capsys) works."""

    def __init__(self) -> None:
        super().__init__(sys.stdout)

    @property
    def stream(self):  # type: ignore[override]
        return sys.stdout

    @stream.setter
    def stream(self, _value) -> None:
        pass


def _formatter() -> logging.Formatter:
    if os.getenv("LOG_FORMAT", "text").strip().lower() == "json":
        return JsonFormatter()
    return TextFormatter()


def configure_logging() -> None:
    """Idempotently attach the stdout handler to the `app` logger."""

    root = logging.getLogger(ROOT_LOGGER_NAME)
    handler = next((h for h in root.handlers if isinstance(h, _StdoutHandler)), None)
    if handler is None:
        handler = _StdoutHandler()
        root.addHandler(handler)
        root.setLevel(logging.INFO)
        root.propagate = False
    handler.setFormatter(_formatter())


def get_logger(name: str) -> logging.Logger:
    """Return a logger under `app`, configured from LOG_FORMAT."""

    configure_logging()
    return logging.getLogger(name if name.startswith("app") else f"app.{name}")
