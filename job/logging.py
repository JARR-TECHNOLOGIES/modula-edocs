import sys
import time
import os
import logging
import threading
from typing import Any, Dict, Optional

_CONFIGURED = False
_LOCK = threading.Lock()
_THREAD_LOCAL = threading.local()

_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

_LOG_FORMAT = (
    "[%(asctime)s.%(msecs)03d] "
    "[+%(delta_ms)sms] "
    "[%(levelname)s] "
    "[customer=%(customer_id)s] "
    "[module=%(module_name)s] "
    "[class=%(class_name)s] "
    "[func=%(func_name)s] "
    "[file=%(pathname)s:%(lineno)d] "
    "[thread=%(threadName)s pid=%(process)d] "
    "- %(message)s"
)

NOISY_LIBRARIES = [
    "pymongo",
    "urllib3",
    "paramiko",
    "google",
    "google.auth",
    "google.cloud",
]


# Filters & Formatter

class ContextFilter(logging.Filter):
    def __init__(self, default_customer_id: str = "-") -> None:
        super().__init__()
        self.default_customer_id = default_customer_id

    def filter(self, record: logging.LogRecord) -> bool:
        record.module_name = getattr(record, "module_name", record.module)
        record.func_name = getattr(record, "func_name", record.funcName)
        record.class_name = getattr(record, "class_name", "-") or "-"
        record.customer_id = getattr(
            record,
            "customer_id",
            self.default_customer_id,
        ) or "-"
        return True


class StructuredFormatter(logging.Formatter):
    def __init__(self) -> None:
        super().__init__(_LOG_FORMAT, _DATE_FORMAT)

    def format(self, record: logging.LogRecord) -> str:
        # Ensure expected fields exist to avoid KeyError during formatting
        for field, default in (
            ("customer_id", "-"),
            ("module_name", record.module),
            ("class_name", "-"),
            ("func_name", record.funcName),
        ):
            if not getattr(record, field, None):
                setattr(record, field, default)

        now = time.perf_counter()
        last = getattr(_THREAD_LOCAL, "last_ts", None)

        if last is None:
            delta_ms = 0.0
        else:
            delta_ms = (now - last) * 1000.0

        _THREAD_LOCAL.last_ts = now
        record.delta_ms = f"{delta_ms:.3f}"

        message = super().format(record)

        if record.exc_info and not record.exc_text:
            record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            message = f"{message}\n{record.exc_text}"

        return message


class LoggerAdapter(logging.LoggerAdapter):
    def process(self, msg: Any, kwargs: Dict[str, Any]) -> Any:
        extra = kwargs.setdefault("extra", {})
        for k, v in self.extra.items():
            extra.setdefault(k, v)
        return msg, kwargs

    def error(self, msg, *args, **kwargs):
        if "exc_info" not in kwargs and sys.exc_info()[0] is not None:
            kwargs["exc_info"] = True
        return super().error(msg, *args, **kwargs)

# Setup

def setup_logging(default_customer_id: str = "-") -> None:
    global _CONFIGURED

    with _LOCK:
        if _CONFIGURED:
            return

        log_level = os.getenv("LOG_LEVEL", "DEBUG").upper()

        root = logging.getLogger()
        root.setLevel(log_level)
        root.handlers.clear()
        root.filters.clear()

        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(StructuredFormatter())
        root.addHandler(handler)

        # Ensure log records always carry contextual fields expected by formatter
        root.addFilter(ContextFilter(default_customer_id))

        for lib in NOISY_LIBRARIES:
            logging.getLogger(lib).setLevel(logging.WARNING)

        logging.captureWarnings(True)

        _CONFIGURED = True


# Public API

def get_logger(
    name: Optional[str] = None,
    *,
    customer_id: Optional[str] = None,
    module_name: Optional[str] = None,
    class_name: Optional[str] = None,
) -> logging.Logger:
    if not _CONFIGURED:
        setup_logging()

    logger = logging.getLogger(name or "modula-job")
    logger.propagate = True

    extra = {}
    if customer_id:
        extra["customer_id"] = customer_id
    if module_name:
        extra["module_name"] = module_name
    if class_name:
        extra["class_name"] = class_name

    if extra:
        return LoggerAdapter(logger, extra)

    return logger
