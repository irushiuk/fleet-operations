"""
Structured JSON logging shared across every pipeline component.

Every log line is a single JSON object so that, in production, this would
feed straight into Loki/ELK/CloudWatch without a custom parser. For the
mini-project it also makes it trivial to visually demonstrate "observability"
in the demo: `docker compose logs -f | jq .`
"""
import json
import logging
import os
import sys
import time


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "component": record.name,
            "message": record.getMessage(),
        }
        # allow callers to pass arbitrary structured fields via `extra={"fields": {...}}`
        fields = getattr(record, "fields", None)
        if fields:
            payload.update(fields)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def get_logger(component_name: str) -> logging.Logger:
    logger = logging.getLogger(component_name)
    if logger.handlers:
        return logger  # already configured (avoid duplicate handlers on reimport)
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logger.setLevel(level)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def log_with_fields(logger: logging.Logger, level: str, message: str, **fields):
    getattr(logger, level.lower())(message, extra={"fields": fields})
