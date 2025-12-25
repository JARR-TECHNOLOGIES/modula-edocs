from datetime import datetime, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

from dateutil import parser

from extensions.logging import get_logger

logger = get_logger(__name__)

TZ = ZoneInfo("America/Costa_Rica")

def utc_now() -> datetime:
    """Return a timezone-aware datetime in the 'America/Costa_Rica' timezone."""
    return datetime.now(TZ)

def get_timestamp() -> Optional[datetime]:
    """
    Returns the current datetime in configured timezone.
    """
    try:
        return utc_now()
    except Exception as e:
        logger.error(f"Error in get_timestamp: {str(e)}")
        return None


def from_utc_to_local(time_value: Any, *, as_iso: bool = False) -> Optional[Any]:
    """
    Convert a UTC datetime (MongoDB) to local timezone.
    Assumes input is UTC or UTC-naive.
    """
    try:
        if time_value is None:
            return None

        if isinstance(time_value, datetime):
            dt = time_value
        else:
            dt = parser.parse(str(time_value))

        # Force UTC interpretation
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)

        local_dt = dt.astimezone(TZ)
        return local_dt.isoformat() if as_iso else local_dt

    except Exception as e:
        logger.error(f"UTC → local conversion failed for '{time_value}': {e}")
        return None
    
def from_local_to_utc(time_value: Any, *, as_iso: bool = False) -> Optional[Any]:
    """
    Convert a local timezone datetime to UTC.
    Assumes input is in local timezone or naive.
    """
    try:
        if time_value is None:
            return None

        if isinstance(time_value, datetime):
            dt = time_value
        else:
            dt = parser.parse(str(time_value))

        # Force local timezone interpretation
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=TZ)
        else:
            dt = dt.astimezone(TZ)

        utc_dt = dt.astimezone(timezone.utc)
        return utc_dt.isoformat() if as_iso else utc_dt

    except Exception as e:
        logger.error(f"Local → UTC conversion failed for '{time_value}': {e}")
        return None