import uuid
from flask import g, request
from extensions.logging import get_logger

logger = get_logger(__name__, class_name="RequestIdMiddleware")


def add_request_id_middleware(app):
    @app.before_request
    def generate_request_id():
        request_id = uuid.uuid4().hex
        g.request_id = request_id
        logger.debug(
            "[REQ_ID] Generated request ID: %s for %s %s",
            request_id,
            request.method,
            request.path,
        )
