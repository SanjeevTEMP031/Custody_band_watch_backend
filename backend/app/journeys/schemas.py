"""
Pydantic Schemas for Journey Management & Route Monitoring APIs
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field


class Waypoint(BaseModel):
    name: Optional[str] = Field(None, example="Via Nizampet")
    latitude: float = Field(..., example=17.5186)
    longitude: float = Field(..., example=78.3840)


class RouteAlternativesRequest(BaseModel):
    origin_latitude: float = Field(..., example=17.4435)
    origin_longitude: float = Field(..., example=78.3812)
    destination_latitude: float = Field(..., example=17.5418)
    destination_longitude: float = Field(..., example=78.3654)
    origin_name: Optional[str] = Field("Hitech City", example="Hitech City")
    destination_name: Optional[str] = Field("Bachupally", example="Bachupally")
    journey_id: Optional[str] = Field(None, example="journey_001")
    waypoints: Optional[List[Waypoint]] = None


class RouteOptionGeometry(BaseModel):
    type: str = Field("LineString", example="LineString")
    coordinates: List[List[float]] = Field(..., example=[[78.3812, 17.4435], [78.3654, 17.5418]])


class RouteOption(BaseModel):
    route_id: str = Field(..., example="route_a")
    name: str = Field(..., example="Via Nizampet")
    distance_meters: Optional[float] = Field(None, example=17215.0)
    duration_seconds: Optional[float] = Field(None, example=1367.0)
    geometry: RouteOptionGeometry
    safety_assessment: Optional[Dict[str, Any]] = None
    is_selected: Optional[bool] = False


class RouteAlternativesResponse(BaseModel):
    journey_id: Optional[str] = None
    routes: List[RouteOption]


class RouteSelectRequest(BaseModel):
    route_id: str = Field(..., example="route_a")
    journey_id: Optional[str] = Field(None, example="journey_001")
    approved_by: Optional[str] = Field("Officer", example="Officer Raman")
    notes: Optional[str] = Field(None, example="Approved based on police station density and street lighting.")


class JourneyCreate(BaseModel):
    device_id: str = Field(..., example="863758061830955")
    origin_name: Optional[str] = Field("Hitech City", example="Hitech City")
    destination_name: Optional[str] = Field("Bachupally", example="Bachupally")
    origin_latitude: Optional[float] = Field(17.4435, example=17.4435)
    origin_longitude: Optional[float] = Field(78.3812, example=78.3812)
    destination_latitude: Optional[float] = Field(17.5418, example=17.5418)
    destination_longitude: Optional[float] = Field(78.3654, example=78.3654)
    scheduled_departure: Optional[str] = Field(None, example="2026-11-24T12:00:00Z")
    scheduled_arrival: Optional[str] = Field(None, example="2026-11-24T13:30:00Z")
    selected_route_id: Optional[str] = Field(None, example="route_a")
    route_corridor_meters: Optional[float] = Field(75.0, example=75.0)
    status: Optional[str] = Field("SCHEDULED", example="SCHEDULED")


class JourneyResponse(BaseModel):
    journey_id: str
    device_id: str
    status: str
    origin_name: Optional[str] = None
    destination_name: Optional[str] = None
    scheduled_departure: Optional[str] = None
    scheduled_arrival: Optional[str] = None
    actual_departure: Optional[str] = None
    actual_arrival: Optional[str] = None
    selected_route_id: Optional[str] = None
    route_corridor_meters: Optional[float] = 75.0
    total_distance_km: Optional[float] = 0.0
    duration_formatted: Optional[str] = "00:00:00"
    points_count: Optional[int] = 0
    created_at: str
    updated_at: str


class JourneyLiveResponse(BaseModel):
    device_id: str
    journey_id: str
    latitude: Optional[float]
    longitude: Optional[float]
    timestamp: Optional[str]
    gps_valid: bool
    location_is_current: bool
    speed_kmh: float
    on_selected_route: bool
    distance_from_route_meters: float
