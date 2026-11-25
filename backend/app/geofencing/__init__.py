"""
Geofencing Module
Export public interfaces.
"""

from .routes import router
from .service import geofence_service
from .evaluator import evaluate_geofence, handle_device_gps_unknown
from .models import init_geofence_database
from .alerts import acknowledge_geofence_alert, resolve_geofence_alert, list_geofence_alerts

__all__ = [
    "router",
    "geofence_service",
    "evaluate_geofence",
    "handle_device_gps_unknown",
    "init_geofence_database",
    "acknowledge_geofence_alert",
    "resolve_geofence_alert",
    "list_geofence_alerts",
]
