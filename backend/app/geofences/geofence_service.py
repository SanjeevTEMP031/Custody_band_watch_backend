"""
Geofence Service (Compatibility Bridge)
Re-exports the complete geofence service from app.geofencing.service.
"""

from ..geofencing.service import geofence_service

__all__ = ["geofence_service"]
