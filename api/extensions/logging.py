
import sys
import time
import os
import logging
import threading
from typing import Any, Dict, Optional

_CONFIGURED = False
_LOCK = threading.Lock()
_VERBOSE_FORMAT = (
    "[%(asctime)s.%(msecs)03d] "
    "[+%(delta_ms)sms] "
    "[%(levelname)s] "
    "[customer=%(customer_id)s] "
    "[module=%(module_name)s] "
    "[class=%(class_name)s] "
    "[func=%(func_name)s] "
    "[file=%(pathname)s:%(lineno)d] "
    "[user=%(user_identity)s] "
    "[thread=%(threadName)s pid=%(process)d] "
    "- %(message)s"
)
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

_THREAD_LOCAL = threading.local()

NOISY_LIBRARIES = [
    "fontTools",
    "fontTools.ttLib",
    "PIL",
    "PIL.PngImagePlugin",
    "weasyprint",
    "cairocffi",
    "cairo",
    "pango",
    "harfbuzz",
]

def silence_noisy_loggers(level=logging.WARNING):
    """
    Force-silence noisy third-party libraries, including already-created child loggers.
    """
    for logger_name in NOISY_LIBRARIES:
        logger = logging.getLogger(logger_name)

        logger.setLevel(level)
        logger.propagate = False

        logger.handlers.clear()

    # Catch already-instantiated deep children
    for name, logger in logging.root.manager.loggerDict.items():
        if any(name.startswith(prefix) for prefix in NOISY_LIBRARIES):
            if isinstance(logger, logging.Logger):
                logger.setLevel(level)
                logger.propagate = False
                logger.handlers.clear()

class _ContextFilter(logging.Filter):
    """Populate optional logging fields with sensible defaults."""
    def __init__(self, customer_id: Optional[str] = None) -> None:
        super().__init__()
        self._customer_id: Optional[str] = customer_id

    def filter(self, record: logging.LogRecord) -> bool:
        record.module_name = getattr(record, "module_name", record.module)
        record.func_name = getattr(record, "func_name", record.funcName)
        record.class_name = getattr(record, "class_name", "-") or "-"
        record.user_identity = getattr(record, "user_identity", "-") or "-"
        record.customer_id = getattr(record, "customer_id", self._customer_id) or "-"
        return True


class StructuredFormatter(logging.Formatter):
    def __init__(self, fmt: str = _VERBOSE_FORMAT, datefmt: str = _DATE_FORMAT, customer_id: Optional[str] = None):
        super().__init__(fmt=fmt, datefmt=datefmt, style="%")
        self._customer_id = customer_id

    def format(self, record: logging.LogRecord) -> str:
        # Populate structured fields
        record.module_name = getattr(record, "module_name", record.module)
        record.func_name = getattr(record, "func_name", record.funcName)
        record.class_name = getattr(record, "class_name", "-") or "-"
        record.user_identity = getattr(record, "user_identity", "-") or "-"
        record.customer_id = getattr(record, "customer_id", self._customer_id) or "-"

        # Time delta calculation
        now = time.perf_counter()
        last = getattr(_THREAD_LOCAL, "last_log_ts", None)

        if last is None:
            delta_ms = 0.0
        else:
            delta_ms = (now - last) * 1000.0

        _THREAD_LOCAL.last_log_ts = now
        record.delta_ms = f"{delta_ms:.3f}"

        # Standard formatting
        record.message = record.getMessage()
        if record.exc_info and not record.exc_text:
            record.exc_text = self.formatException(record.exc_info)
        formatted = super().format(record)
        if record.exc_text:
            formatted = f"{formatted}\n{record.exc_text}"
        return formatted

class _LoggerAdapter(logging.LoggerAdapter):
    def process(self, msg: Any, kwargs: Dict[str, Any]) -> Any:
        extra = kwargs.setdefault("extra", {})
        for key, value in self.extra.items():
            extra.setdefault(key, value)
        return msg, kwargs

    def error(self, msg, *args, **kwargs):
        if "exc_info" not in kwargs and sys.exc_info()[0] is not None:
            kwargs["exc_info"] = True
        return super().error(msg, *args, **kwargs)

def setup_logging() -> None:
    """Configure root logger with structured logging handlers."""

    global _CONFIGURED
    with _LOCK:
        if _CONFIGURED:
            return

        log_level = os.getenv("LOG_LEVEL", "DEBUG").upper()
        customer_id = os.getenv("CUSTOMER_ID", "unknown")

        root = logging.getLogger()
        root.setLevel(log_level)
        root.handlers.clear()
        root.filters.clear()

        formatter = StructuredFormatter(customer_id=customer_id)
        context_filter = _ContextFilter(customer_id=customer_id)
        root.addFilter(context_filter)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        root.addHandler(stream_handler)

        logging.captureWarnings(True)
        logging.getLogger("filelock").setLevel(logging.WARNING)
        logging.getLogger("paramiko").setLevel(logging.WARNING)

        silence_noisy_loggers()

        _CONFIGURED = True


def get_logger(name: Optional[str] = None, **context: Any) -> logging.Logger:
    """Return a structured logger optionally pre-bound with context."""
    if not _CONFIGURED:
        setup_logging()

    base_logger = logging.getLogger(name or "modula")
    base_logger.propagate = True
    if context:
        return _LoggerAdapter(base_logger, context)
    return base_logger