import os


class Config:
    """Base application configuration."""

    # Application Settings
    PROPAGATE_EXCEPTIONS = True
    SESSION_TYPE = "filesystem"
    FILES_ROOT = os.getenv("FILES_ROOT", "/gcp-bucket")
    STAGE_FILES_ROOT = os.getenv("STAGE_FILES_ROOT", "/tmp/stage")

    # API Settings
    API_TITLE = "Modula Files API"
    API_VERSION = "1.0.0"
    API_DESCRIPTION = "Modula Internal Files Management API"
    OPENAPI_VERSION = "3.0.3"

    # Upload Settings
    MAX_CONTENT_LENGTH = 2 * 1024 * 1024 * 1024  # 2GB

    # Simple header-based auth
    API_KEY = os.getenv("FILES_API_KEY", "")
    API_SECRET = os.getenv("FILES_API_SECRET", "")
