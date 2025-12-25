import os
import threading
from typing import Optional

from pymongo import MongoClient, errors
from pymongo.server_api import ServerApi

from job.logging import get_logger
logger = get_logger(__name__)

_CLIENT: Optional[MongoClient] = None
_CLIENT_LOCK = threading.Lock()


def _build_uri() -> str:
    username = os.getenv("MONGO_USERNAME")
    password = os.getenv("MONGO_PASSWORD")
    cluster = os.getenv("MONGO_CLUSTER")

    if not username or not password or not cluster:
        raise RuntimeError("Mongo credentials or cluster not configured properly")

    return f"mongodb+srv://{username}:{password}@{cluster}/?retryWrites=true"


def get_client() -> MongoClient:
    """Singleton MongoClient for Atlas"""
    global _CLIENT
    if _CLIENT is not None:
        return _CLIENT

    with _CLIENT_LOCK:
        if _CLIENT is not None:
            return _CLIENT

        uri = _build_uri()

        logger.info(f"Connecting to MongoDB Atlas using URI...")
        try:
            client = MongoClient(uri, server_api=ServerApi("1"))
            client.admin.command("ping")
        except errors.PyMongoError as exc:
            logger.exception("Failed to connect to Mongo Atlas")
            raise RuntimeError("MongoDB connection failed") from exc

        _CLIENT = client
        logger.info("MongoClient initialised successfully (Atlas connection active)")
        return _CLIENT