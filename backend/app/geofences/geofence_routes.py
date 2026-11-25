"""
Geofence Routes (Compatibility Bridge)
Re-exports the complete geofencing router from app.geofencing.routes.
"""

from ..geofencing.routes import router

__all__ = ["router"]
