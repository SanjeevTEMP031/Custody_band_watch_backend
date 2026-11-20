"""
Alert Configuration Module
Re-exports from services.alert_config for clean modularity.
"""

from ..services.alert_config import (
    ALERT_CONFIG,
    ALERT_SEVERITIES,
    ALERT_TITLES,
    LEGACY_ALERT_TYPE_MAP,
)

__all__ = [
    "ALERT_CONFIG",
    "ALERT_SEVERITIES",
    "ALERT_TITLES",
    "LEGACY_ALERT_TYPE_MAP",
]
