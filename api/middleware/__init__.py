from flask import Flask

# Import all middleware installers
from middleware.request_id import add_request_id_middleware
from middleware.ip import add_ip_extraction_middleware
from middleware.timers import add_request_timing_middleware
from middleware.logging import add_logging_middleware
from middleware.security import add_security_headers_middleware
from middleware.errors import add_error_handlers_middleware
from middleware.response_wrapper import add_response_wrapper_middleware


def init_middleware(app: Flask) -> None:
    """
    Register ALL global middlewares in the correct order.

    The order matters greatly:
        1. request_id → must happen as early as possible
        2. ip extraction → used by rate limiter
        3. timers → for profiling
        4. logging → uses request_id + ip
        5. metrics → increments counters early
        6. errors → central exception normalization
        7. response wrapper → last step to unify output
        8. security headers → after response body is ready
    """

    # BEFORE REQUEST middlewares
    add_request_id_middleware(app)
    add_ip_extraction_middleware(app)
    add_request_timing_middleware(app)
    add_logging_middleware(app)

    # AFTER REQUEST middlewares
    add_response_wrapper_middleware(app)
    add_security_headers_middleware(app)

    # Error handlers (global)
    add_error_handlers_middleware(app)
