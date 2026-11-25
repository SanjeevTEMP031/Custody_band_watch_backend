"""
Date and Time Utilities
Conversions for UTC, IST (Indian Standard Time), and Huaten C05 format parser.
"""

import re
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, Any

IST_OFFSET = timedelta(hours=5, minutes=30)
IST_TZ = timezone(IST_OFFSET, name="IST")


def current_iso_timestamp() -> str:
    """Returns current UTC ISO 8601 timestamp."""
    return datetime.now(timezone.utc).isoformat()


def current_utc_iso() -> str:
    """Returns current UTC ISO 8601 timestamp."""
    return datetime.now(timezone.utc).isoformat()


def current_ist_time() -> datetime:
    """Returns current datetime in Indian Standard Time (IST)."""
    return datetime.now(timezone.utc).astimezone(IST_TZ)


def current_ist_iso() -> str:
    """Returns current IST ISO 8601 timestamp."""
    return datetime.now(timezone.utc).astimezone(IST_TZ).isoformat()


def parse_to_utc_datetime(value: Any) -> Optional[datetime]:
    """
    Robustly parses any timestamp representation into a timezone-aware UTC datetime.
    Supports:
    - datetime instances (aware or naive -> UTC)
    - ISO 8601 with Z or timezone offset (e.g. +05:30)
    - Text timestamps with 'IST' or 'UTC'
    - Space-separated date/time strings
    - Numeric unix timestamps (seconds or milliseconds)
    """
    if value is None:
        return None

    if isinstance(value, (int, float)):
        val = float(value)
        if val > 1e11:  # Milliseconds
            val /= 1000.0
        try:
            return datetime.fromtimestamp(val, tz=timezone.utc)
        except (ValueError, OverflowError, OSError):
            return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    s = str(value).strip()
    if not s or s.lower() in ("none", "null", "undefined", "string"):
        return None

    is_ist = False
    if "IST" in s:
        is_ist = True
        s = s.replace("IST", "").strip()

    # Normalize Z
    s = s.replace("Z", "+00:00")
    if " " in s and "T" not in s:
        s = s.replace(" ", "T")

    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            if is_ist:
                dt = dt.replace(tzinfo=IST_TZ)
            else:
                dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (TypeError, ValueError):
        pass

    # Try common formats
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y%m%d%H%M%S",
        "%y%m%d%H%M%S"
    ):
        try:
            dt = datetime.strptime(s, fmt)
            if is_ist:
                dt = dt.replace(tzinfo=IST_TZ)
            else:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except (TypeError, ValueError):
            continue

    return None


def to_utc_iso(value: Any) -> Optional[str]:
    """Converts any timestamp to standard UTC ISO-8601 string."""
    dt = parse_to_utc_datetime(value)
    return dt.isoformat() if dt else None


def to_ist_iso(value: Any) -> Optional[str]:
    """Converts any timestamp to IST ISO-8601 string (+05:30)."""
    dt = parse_to_utc_datetime(value)
    if not dt:
        return None
    return dt.astimezone(IST_TZ).isoformat()


def parse_watch_datetime(date_str: str, time_str: str) -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    Parses Huaten C05 protocol date and time.
    date_str: 'YYMMDD' (e.g. '261102' -> 2026-11-02)
    time_str: 'HHMMSS' (e.g. '092137' -> 09:21:37 UTC)

    Returns:
        (utc_datetime, ist_datetime)
    """
    try:
        if len(date_str) == 6 and len(time_str) == 6:
            year = 2000 + int(date_str[0:2])
            month = int(date_str[2:4])
            day = int(date_str[4:6])

            hour = int(time_str[0:2])
            minute = int(time_str[2:4])
            second = int(time_str[4:6])

            dt_utc = datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
            dt_ist = dt_utc.astimezone(IST_TZ)
            return dt_utc, dt_ist
    except Exception:
        pass
    return None, None


def format_utc_to_ist(utc_str: Any) -> str:
    """Converts an ISO UTC string or formatted string to human readable IST."""
    try:
        dt = parse_to_utc_datetime(utc_str)
        if not dt:
            return str(utc_str)
        ist_dt = dt.astimezone(IST_TZ)
        return ist_dt.strftime("%Y-%m-%d %I:%M:%S %p IST")
    except Exception:
        return str(utc_str)

