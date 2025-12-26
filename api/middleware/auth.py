from flask import request
from flask_smorest import abort

from config import Config


def add_api_key_auth_middleware(app):
    @app.before_request
    def _check_api_key():
        # Skip health check endpoint
        if request.path == "/healthz":
            return
        
        expected_key = Config.API_KEY
        expected_secret = Config.API_SECRET

        # If not configured, allow all
        if not expected_key and not expected_secret:
            return

        provided_key = request.headers.get("X-M-Api-Key", "")
        provided_secret = request.headers.get("X-M-Api-Secret", "")

        if provided_key != expected_key or provided_secret != expected_secret:
            abort(401, message="Unauthorized")
