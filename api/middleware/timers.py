import time
from flask import g, request
from extensions.logging import get_logger

logger = get_logger(__name__, class_name="RequestTimingMiddleware")


def add_request_timing_middleware(app):
    @app.before_request
    def start_timer():
        g.start_ts = time.time()

    @app.after_request
    def end_timer(response):
        duration_ms = (time.time() - g.start_ts) * 1000
        response.headers["X-Response-Time-ms"] = f"{duration_ms:.2f}"
        logger.debug(
            "[TIMING][%s] %s %s took %.2fms",
            getattr(g, "request_id", "N/A"),
            request.method,
            request.path,
            duration_ms,
        )
        return response
