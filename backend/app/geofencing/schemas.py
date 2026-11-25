"""
Geofence Schemas Module
Pydantic schemas for circle and polygon geofences, device assignments,
events, alerts, and acknowledgment workflows.
"""

from enum import Enum
from typing import List, Optional, Any, Dict
from pydantic import BaseModel, Field, field_validator, model_validator


class GeofenceType(str, Enum):
    CIRCLE = "CIRCLE"
    POLYGON = "POLYGON"


class FenceType(str, Enum):
    INCLUSION = "INCLUSION"
    EXCLUSION = "EXCLUSION"


class AlarmState(str, Enum):
    ACTIVE = "ACTIVE"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    RESOLVED = "RESOLVED"


class GeofenceEventType(str, Enum):
    GEOFENCE_ENTRY = "GEOFENCE_ENTRY"
    GEOFENCE_EXIT = "GEOFENCE_EXIT"
    GEOFENCE_OUTSIDE = "GEOFENCE_OUTSIDE"
    GEOFENCE_REENTRY = "GEOFENCE_REENTRY"
    GEOFENCE_GPS_UNKNOWN = "GEOFENCE_GPS_UNKNOWN"


class GeofenceBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=120, description="Name or label for the geofence perimeter")
    description: Optional[str] = Field(None, max_length=500, description="Optional description or operational context")
    type: str = Field("CIRCLE", description="Perimeter type: 'CIRCLE' or 'POLYGON'")
    fence_type: str = Field("INCLUSION", description="'INCLUSION' (must remain inside) or 'EXCLUSION' (forbidden zone)")
    center_latitude: Optional[float] = Field(None, description="Center latitude for circular geofences")
    center_longitude: Optional[float] = Field(None, description="Center longitude for circular geofences")
    radius_meters: Optional[float] = Field(None, ge=1.0, description="Radius in meters for circular geofences")
    polygon_coordinates: Optional[List[Any]] = Field(None, description="List of [latitude, longitude] coordinates for polygon geofences")
    active: bool = Field(True, description="Whether the geofence is currently active")
    entry_alert: bool = Field(True, description="Whether to alert when a device enters the geofence")
    exit_alert: bool = Field(True, description="Whether to alert when a device exits the geofence")

    @model_validator(mode="before")
    @classmethod
    def normalize_aliases(cls, data: Any) -> Any:
        if isinstance(data, dict):
            # Support aliases: geofence_type -> type
            if "geofence_type" in data and "type" not in data:
                data["type"] = data["geofence_type"]
            # Support alias: is_active -> active
            if "is_active" in data and "active" not in data:
                data["active"] = bool(data["is_active"])
            # Support alias: latitude/longitude for center
            if "latitude" in data and "center_latitude" not in data:
                data["center_latitude"] = data["latitude"]
            if "longitude" in data and "center_longitude" not in data:
                data["center_longitude"] = data["longitude"]
            # Support alias: radius / radius_m -> radius_meters
            if "radius" in data and "radius_meters" not in data:
                data["radius_meters"] = data["radius"]
            if "radius_m" in data and "radius_meters" not in data:
                data["radius_meters"] = data["radius_m"]
        return data


class GeofenceCreate(GeofenceBase):
    device_ids: Optional[List[str]] = Field(default_factory=list, description="Optional device IDs to assign immediately")
    assigned_device_id: Optional[str] = Field(None, description="Legacy single device ID assignment")

    @model_validator(mode="after")
    def validate_type_fields(self) -> "GeofenceCreate":
        geo_type = (self.type or "CIRCLE").upper()
        if geo_type == "CIRCLE":
            if self.center_latitude is None or self.center_longitude is None:
                raise ValueError("Circle geofences require center_latitude and center_longitude")
            if self.radius_meters is None or self.radius_meters <= 0:
                self.radius_meters = 500.0
        elif geo_type == "POLYGON":
            if not self.polygon_coordinates or len(self.polygon_coordinates) < 3:
                raise ValueError("Polygon geofences require at least 3 polygon_coordinates")
        else:
            raise ValueError(f"Unsupported geofence type '{self.type}'. Must be 'CIRCLE' or 'POLYGON'")

        # Merge assigned_device_id into device_ids if provided
        if self.assigned_device_id and self.assigned_device_id not in (self.device_ids or []):
            if self.device_ids is None:
                self.device_ids = []
            self.device_ids.append(self.assigned_device_id)

        return self


class GeofenceUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    type: Optional[str] = None
    fence_type: Optional[str] = None
    center_latitude: Optional[float] = None
    center_longitude: Optional[float] = None
    radius_meters: Optional[float] = None
    polygon_coordinates: Optional[List[Any]] = None
    active: Optional[bool] = None
    is_active: Optional[bool] = None
    entry_alert: Optional[bool] = None
    exit_alert: Optional[bool] = None
    device_ids: Optional[List[str]] = None
    assigned_device_id: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def normalize_aliases(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if "geofence_type" in data and "type" not in data:
                data["type"] = data["geofence_type"]
            if "is_active" in data and "active" not in data:
                data["active"] = bool(data["is_active"])
            if "latitude" in data and "center_latitude" not in data:
                data["center_latitude"] = data["latitude"]
            if "longitude" in data and "center_longitude" not in data:
                data["center_longitude"] = data["longitude"]
            if "radius" in data and "radius_meters" not in data:
                data["radius_meters"] = data["radius"]
            if "radius_m" in data and "radius_meters" not in data:
                data["radius_meters"] = data["radius_m"]
        return data


class AssignDevicesRequest(BaseModel):
    device_ids: Optional[List[str]] = Field(default_factory=list, description="List of device IDs to assign")
    device_id: Optional[str] = Field(None, description="Single device ID to assign")

    @model_validator(mode="after")
    def ensure_at_least_one_device(self) -> "AssignDevicesRequest":
        if self.device_id and self.device_id not in (self.device_ids or []):
            if self.device_ids is None:
                self.device_ids = []
            self.device_ids.append(self.device_id)
        if not self.device_ids:
            raise ValueError("Must provide at least one device_id or device_ids")
        return self


class GeofenceResponse(BaseModel):
    id: str = Field(..., description="Unique geofence ID")
    name: str
    description: Optional[str] = None
    type: str
    fence_type: str
    center_latitude: Optional[float] = None
    center_longitude: Optional[float] = None
    radius_meters: Optional[float] = None
    polygon_coordinates: Optional[List[List[float]]] = None
    active: bool = True
    is_active: bool = True  # Backward compatibility alias
    entry_alert: bool = True
    exit_alert: bool = True
    assigned_devices: List[str] = Field(default_factory=list)
    assigned_device_count: int = 0
    assigned_device_id: Optional[str] = None  # Backward compatibility
    created_by: Optional[str] = None
    created_at: str
    updated_at: str

    # Legacy fields for older frontends:
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    radius_m: Optional[float] = None


class GeofenceEventResponse(BaseModel):
    id: str
    device_id: str
    geofence_id: str
    geofence_name: Optional[str] = None
    event_type: str
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    event_time: str
    created_at: str


class GeofenceAlertResponse(BaseModel):
    id: str
    event_id: str
    device_id: str
    geofence_id: str
    geofence_name: Optional[str] = None
    event_type: str
    severity: str
    status: str
    requires_alarm: bool
    title: Optional[str] = None
    message: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    acknowledged_by: Optional[str] = None
    acknowledged_at: Optional[str] = None
    resolved_by: Optional[str] = None
    resolved_at: Optional[str] = None
    created_at: str
    updated_at: str


class GeofenceAlertAcknowledgeRequest(BaseModel):
    notes: Optional[str] = Field(None, description="Optional operator notes on acknowledgment")


class GeofenceValidateRequest(BaseModel):
    device_id: str
    latitude: float
    longitude: float
