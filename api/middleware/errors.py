from flask import jsonify, g
from werkzeug.exceptions import HTTPException, Unauthorized
from extensions.logging import get_logger

logger = get_logger(__name__, class_name="ErrorHandler")


def add_error_handlers_middleware(app):
    @app.errorhandler(Exception)
    def handle_errors(e):
        request_id = getattr(g, "request_id", "N/A")

        # Handle Unauthorized explicitly first to avoid being swallowed by the generic
        # HTTPException branch and accidentally converted into a 500.
        if isinstance(e, Unauthorized):
            logger.error(f"[ERROR][{request_id}] 401 - Unauthorized: {str(e)}")
            return jsonify({
                "ok": False,
                "code": "UNAUTHORIZED",
                "message": "Unauthorized",
                "request_id": request_id
            }), 401

        if isinstance(e, HTTPException):
            logger.error(f"[ERROR][{request_id}] {e.code} - {e.description}")
            return jsonify({
                "ok": False,
                "code": e.name.upper().replace(" ", "_"),
                "message": e.description,
                "request_id": request_id
            }), e.code

        # Non-HTTP (crash)
        logger.critical(f"[CRITICAL][{request_id}] Unhandled error: {e}", exc_info=True)
        return jsonify({
            "ok": False,
            "code": "INTERNAL_ERROR",
            "message": "Internal Server Error",
            "request_id": request_id
        }), 500
