import os


class Config:
    """Base application configuration."""

    # Application Settings
    PROPAGATE_EXCEPTIONS = True
    SESSION_TYPE = "filesystem"
    ALLOWED_FILE_TYPES = {"pdf", "xml", "pos", "mh_xml", "mr_xml"}
    FILES_ROOT = os.getenv("FILES_ROOT", "/gcp-bucket")

    # API Settings
    API_TITLE = "Modula Files API"
    API_VERSION = "1.0.0"
    API_DESCRIPTION = "Modula Internal Files Management API"
    OPENAPI_VERSION = "3.0.3"

    # Upload Settings
    MAX_CONTENT_LENGTH = 2 * 1024 * 1024 * 1024  # 2GB