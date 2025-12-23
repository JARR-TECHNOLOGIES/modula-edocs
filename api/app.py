from flask import Flask
from flask_session import Session

from config import Config
from extensions import (
    logging as log_ext
)
from routes import init_routes
from middleware import init_middleware


def create_app() -> Flask:
    """
    Factory to create Flask app instance
    """
    app = Flask(__name__)
    app.config.from_object(Config)

    # Extensions
    Session(app)

    # Logging
    log_ext.setup_logging()
    logger = log_ext.get_logger(__name__)
    app.logger = logger

    # Middleware
    init_middleware(app)

    # Routes
    init_routes(app)
    
    # Ensure OpenAPI metadata fallbacks are set to satisfy smorest
    app.config.setdefault("API_TITLE", app.config.get("APP_NAME", "Modula API"))
    app.config.setdefault("API_VERSION", app.config.get("APP_VERSION", "0.0.0"))

    logger.info(f"[INIT] Modula Edoc Files API")

    return app


# Gunicorn entry point
app = create_app()

# Run locally for testing
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
