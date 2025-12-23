from flask.views import MethodView
from flask_smorest import Blueprint
from extensions.logging import get_logger

logger = get_logger(__name__, class_name="HealthzController")


blp = Blueprint(
    "Healthz",
    __name__,
    url_prefix="/healthz",
    description="Health check endpoint",
)

@blp.route("", strict_slashes=False)
class HealthzController(MethodView):
    def get(self):
        """
        Health check endpoint for monitoring and uptime verification.
        """
        return {
            "ok": True,
            "message": "Service is healthy",
            "code": "HEALTHY",
            "data": {}
        }