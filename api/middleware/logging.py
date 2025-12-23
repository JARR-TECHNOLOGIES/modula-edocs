from flask import request, g
from extensions.logging import get_logger

logger = get_logger(__name__, class_name="RequestLoggingMiddleware")


def add_logging_middleware(app):
    @app.before_request
    def log_before():
        logger.info(
            "[REQ][%s] %s %s IP=%s",
            getattr(g, "request_id", "N/A"),
            request.method,
            request.path,
            getattr(g, "client_ip", request.remote_addr),
        )

    @app.after_request
    def log_after(response):
        logger.info(
            "[RES][%s] %s %s Status=%s",
            getattr(g, "request_id", "N/A"),
            request.method,
            request.path,
            response.status_code,
        )
        return response
