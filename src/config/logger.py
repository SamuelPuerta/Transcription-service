import json
import logging
import sys
import traceback
from datetime import datetime, timezone
from typing import Any, Optional

SERVICE_NAME = "transcripcion-service"
DEFAULT_CORRELATION_ID = "no-correlation-id"
DEFAULT_USER_ID = "system"

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        context: dict[str, Any] = getattr(record, "context", {})
        if record.exc_info:
            context["traceback"] = traceback.format_exception(*record.exc_info)
        elif record.exc_text:
            context["traceback"] = record.exc_text
        now = datetime.now(timezone.utc)
        timestamp = now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"
        payload = {
            "timestamp": timestamp,
            "severity": record.levelname,
            "message": record.getMessage(),
            "service": SERVICE_NAME,
            "correlation_id": getattr(record, "correlation_id", DEFAULT_CORRELATION_ID),
            "user_id": getattr(record, "user_id", DEFAULT_USER_ID),
            "context": context,
        }
        return json.dumps(payload, ensure_ascii=False, default=str)

class AppLogger(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        extra = kwargs.setdefault("extra", {})
        extra.setdefault("correlation_id", self.extra.get("correlation_id", DEFAULT_CORRELATION_ID))
        extra.setdefault("user_id", self.extra.get("user_id", DEFAULT_USER_ID))
        extra.setdefault("context", self.extra.get("context", {}))
        return msg, kwargs

    def info(self, msg, *args, context: Optional[dict] = None, **kwargs):
        kwargs.setdefault("extra", {})["context"] = context or {}
        super().info(msg, *args, **kwargs)

    def warning(self, msg, *args, context: Optional[dict] = None, **kwargs):
        kwargs.setdefault("extra", {})["context"] = context or {}
        super().warning(msg, *args, **kwargs)

    def error(self, msg, *args, context: Optional[dict] = None, **kwargs):
        kwargs.setdefault("extra", {})["context"] = context or {}
        super().error(msg, *args, **kwargs)

    def critical(self, msg, *args, context: Optional[dict] = None, **kwargs):
        kwargs.setdefault("extra", {})["context"] = context or {}
        super().critical(msg, *args, **kwargs)

    def exception(self, msg, *args, context: Optional[dict] = None, **kwargs):
        kwargs.setdefault("extra", {})["context"] = context or {}
        super().exception(msg, *args, **kwargs)

    def bind(self, **kwargs) -> "AppLogger":
        merged = {**self.extra, **kwargs}
        return AppLogger(self.logger, merged)

def setup_logging(log_level: str = "INFO") -> None:
    formatter = JsonFormatter()
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(log_level)
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setFormatter(formatter)
    stderr_handler.setLevel(logging.ERROR)
    root = logging.getLogger()
    root.setLevel(log_level)
    root.handlers.clear()
    root.addHandler(console_handler)
    for name in ("uvicorn", "uvicorn.access"):
        lg = logging.getLogger(name)
        lg.handlers = [console_handler]
        lg.propagate = False
    uvicorn_error = logging.getLogger("uvicorn.error")
    uvicorn_error.handlers = [stderr_handler]
    uvicorn_error.propagate = False

setup_logging()

logger: AppLogger = AppLogger(logging.getLogger("app"), {})
