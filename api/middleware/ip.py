from flask import request, g
from extensions.logging import get_logger

logger = get_logger(__name__, class_name="IpExtractionMiddleware")


def add_ip_extraction_middleware(app):
    @app.before_request
    def extract_ip():
        xff = request.headers.get("X-Forwarded-For", request.remote_addr)
        g.client_ip = xff.split(",")[0].strip()
        logger.debug(
            "[IP][%s] Client IP: %s",
            getattr(g, "request_id", "N/A"),
            g.client_ip,
        )
