"""
Journey & Route Tracking API Routes
Implements comprehensive police transportation and custody journey management:
- Pre-departure route alternative calculation with OSRM
- Officer route approval and safety assessments
- Scheduled departures, corridor monitoring, and arrival tracking
- Live position feeds, travelled path polylines, event logs, and WebSocket streams
- Full backward compatibility with existing journey start/end endpoints
"""

import json
import uuid
from typing import Dict, Any, List, Optional

from fastapi import (
    APIRouter,
    HTTPException,
    Query,
    Request,
    WebSocket,
    WebSocketDisconnect,
    status,
)
from pydantic import BaseModel, Field

from ..config import settings
from ..core.dependencies import get_current_user
from ..services.alert_service import ws_manager, alert_service
from ..services.journey_service import journey_service
from ..services.osrm_service import osrm_service
from ..services.device_service import device_service
from ..utils.datetime_utils import current_iso_timestamp
from ..utils.logger import app_logger

from .models import (
    create_journey_record,
    get_journey_by_id,
    get_active_journey_for_device,
    list_journeys_records,
    update_journey_record,
    save_journey_route,
    get_journey_routes,
    select_journey_route,
    get_selected_route_for_journey,
    get_journey_gps_points,
    get_journey_events,
)
from .route_monitor import (
    route_monitor,
    calculate_perpendicular_distance_to_route,
)
from .schemas import (
    RouteAlternativesRequest,
    RouteAlternativesResponse,
    RouteSelectRequest,
    JourneyCreate,
)

router = APIRouter(prefix="/journeys", tags=["Journeys & Travel Tracking"])


async def get_officer_identity(request: Request) -> str:
    """Extracts authenticated officer identity, falling back to 'Officer'."""
    try:
        user = await get_current_user(request)
        return user.get("email") or user.get("full_name") or "Officer"
    except Exception:
        return "Officer"


# =============================================================================
# 1. LEGACY ENDPOINT COMPATIBILITY (PRESERVED)
# =============================================================================

class StartJourneyRequest(BaseModel):
    device_id: str = Field(..., description="IMEI or ID of the custody band device", example="863758061830955")
    start_latitude: Optional[float] = Field(None, description="Starting latitude")
    start_longitude: Optional[float] = Field(None, description="Starting longitude")
    latitude: Optional[float] = Field(None, description="Alias for start_latitude")
    longitude: Optional[float] = Field(None, description="Alias for start_longitude")
    wearer_name: Optional[str] = Field(None, description="Optional wearer name")
    timestamp: Optional[str] = Field(None, description="Optional start timestamp")


class EndJourneyRequest(BaseModel):
    device_id: str = Field(..., description="IMEI or ID of the custody band device", example="863758061830955")
    journey_id: Optional[str] = Field(None, description="Optional specific journey ID to end")


@router.post("/start", response_model=Dict[str, Any])
def legacy_start_journey(request: StartJourneyRequest):
    """Starts travel journey for specified custody device (legacy format)."""
    dev_id = request.device_id.strip()
    if not dev_id:
        raise HTTPException(status_code=400, detail="device_id is required")

    req_ts = request.timestamp
    if req_ts and (req_ts == "string" or not req_ts.strip()):
        req_ts = None

    lat = request.start_latitude if request.start_latitude is not None else request.latitude
    lon = request.start_longitude if request.start_longitude is not None else request.longitude
    initial_location = None
    if lat is not None and lon is not None:
        initial_location = {
            "latitude": float(lat),
            "longitude": float(lon),
            "timestamp": req_ts,
            "speed_kmh": 0.0,
        }

    journey = journey_service.start_journey(
        dev_id,
        start_time=req_ts,
        initial_location=initial_location,
    )
    return {
        "success": True,
        "message": "Journey started successfully",
        "data": journey,
        "journey": journey
    }


@router.post("/end", response_model=Dict[str, Any])
def legacy_end_journey(request: EndJourneyRequest):
    """Ends travel journey for specified custody device (legacy format)."""
    dev_id = request.device_id.strip()
    if not dev_id:
        raise HTTPException(status_code=400, detail="device_id is required")

    journey = journey_service.end_journey(dev_id, journey_id=request.journey_id)
    if not journey:
        raise HTTPException(status_code=404, detail=f"No active journey found for device {dev_id}")

    return {
        "success": True,
        "message": "Journey ended successfully",
        "data": journey,
        "journey": journey
    }


@router.get("/active/{device_id}", response_model=Dict[str, Any])
def get_legacy_active_journey(device_id: str):
    """Retrieves active journey for device (legacy format)."""
    journey = journey_service.get_active_journey(device_id)
    if not journey:
        return {
            "active": False,
            "device_id": device_id,
            "journey": None
        }

    return {
        "active": True,
        "device_id": device_id,
        "journey": journey
    }


@router.get("/device/{device_id}", response_model=List[Dict[str, Any]])
def list_legacy_device_journeys(
    device_id: str,
    limit: int = Query(50, ge=1, le=200, description="Max journeys to retrieve")
):
    """Lists historical and completed journeys for a custody band (legacy format)."""
    return journey_service.get_device_journeys(device_id, limit=limit)


# Backward-compatible function aliases for test suites and scripts
start_journey = legacy_start_journey
end_journey = legacy_end_journey
get_active_journey = get_legacy_active_journey


# =============================================================================
# 2. OSRM ROUTE ALTERNATIVES & SELECTION ENDPOINTS
# =============================================================================

@router.post("/routes/alternatives", response_model=Dict[str, Any])
def get_route_alternatives(payload: RouteAlternativesRequest):
    """
    Accepts origin and destination coordinates and queries OSRM for alternative routes.
    Returns actual road-following geometries, distances, estimated durations,
    and security safety assessments.
    """
    waypoints_data = [wp.dict() for wp in payload.waypoints] if payload.waypoints else None

    routes = osrm_service.get_alternative_routes(
        origin_lat=payload.origin_latitude,
        origin_lon=payload.origin_longitude,
        dest_lat=payload.destination_latitude,
        dest_lon=payload.destination_longitude,
        waypoints=waypoints_data,
        origin_name=payload.origin_name,
        dest_name=payload.destination_name,
    )

    jid = payload.journey_id or f"journey_preview_{uuid.uuid4().hex[:8]}"

    # If journey_id is provided, persist routes into journey_routes
    if payload.journey_id:
        existing = get_journey_by_id(payload.journey_id)
        if existing:
            for r in routes:
                save_journey_route(
                    journey_id=payload.journey_id,
                    route_name=r["name"],
                    geometry=r["geometry"],
                    distance_meters=r["distance_meters"],
                    duration_seconds=r["duration_seconds"],
                    safety_assessment=r.get("safety_assessment"),
                    route_id=r["route_id"]
                )

    return {
        "journey_id": jid,
        "routes": routes
    }


@router.post("/routes/select", response_model=Dict[str, Any])
async def select_route_global(request: Request, payload: RouteSelectRequest):
    """Approves a selected route for an active or scheduled journey."""
    if not payload.journey_id:
        raise HTTPException(status_code=400, detail="journey_id is required")

    officer = payload.approved_by or await get_officer_identity(request)
    res = select_journey_route(payload.journey_id, payload.route_id, approved_by=officer)
    if not res:
        raise HTTPException(status_code=404, detail="Route or journey not found")

    return {
        "success": True,
        "message": f"Route '{res.get('route_name')}' approved successfully",
        "route": res
    }


@router.post("/{journey_id}/route/select", response_model=Dict[str, Any])
async def select_route_for_journey(journey_id: str, request: Request, payload: RouteSelectRequest):
    """Approves a selected route for the specified journey."""
    clean_jid = (journey_id or "").strip()
    officer = payload.approved_by or await get_officer_identity(request)

    journey = get_journey_by_id(clean_jid)
    if not journey:
        raise HTTPException(status_code=404, detail=f"Journey {clean_jid} not found")

    # Prevent modification if already COMPLETED or CANCELLED
    if journey.get("status") in ("COMPLETED", "CANCELLED"):
        raise HTTPException(status_code=400, detail=f"Cannot change route for journey in status {journey.get('status')}")

    res = select_journey_route(clean_jid, payload.route_id, approved_by=officer)
    if not res:
        raise HTTPException(status_code=404, detail=f"Route {payload.route_id} not found for journey {clean_jid}")

    return {
        "success": True,
        "message": f"Route '{res.get('route_name')}' approved by {officer}",
        "route": res
    }


# =============================================================================
# 3. JOURNEY CRUD & LIFECYCLE MANAGEMENT
# =============================================================================

@router.post("", response_model=Dict[str, Any], status_code=status.HTTP_201_CREATED)
@router.post("/", response_model=Dict[str, Any], status_code=status.HTTP_201_CREATED, include_in_schema=False)
async def create_journey(request: Request, payload: JourneyCreate):
    """
    Creates or reuses a scheduled journey (e.g., Police Station to Court transport).
    Assigns a custody device, origin, destination, scheduled departure/arrival times,
    and sets initial route corridor threshold.
    Reuses existing ACTIVE or SCHEDULED journey to prevent duplicate journeys.
    """
    dev_id = payload.device_id.strip()
    if not dev_id:
        raise HTTPException(status_code=400, detail="device_id is required")

    now_iso = current_iso_timestamp()

    # Deduplication: check for active or scheduled journey for this device
    existing = get_active_journey_for_device(dev_id)
    if existing and existing.get("status") in ("ACTIVE", "SCHEDULED", "PAUSED"):
        jid = existing["journey_id"]
        updates: Dict[str, Any] = {}
        if payload.origin_name:
            updates["origin"] = payload.origin_name
            updates["origin_name"] = payload.origin_name
        if payload.destination_name:
            updates["destination"] = payload.destination_name
            updates["destination_name"] = payload.destination_name
        if payload.origin_latitude is not None:
            updates["start_latitude"] = payload.origin_latitude
        if payload.origin_longitude is not None:
            updates["start_longitude"] = payload.origin_longitude
        if payload.destination_latitude is not None:
            updates["end_latitude"] = payload.destination_latitude
        if payload.destination_longitude is not None:
            updates["end_longitude"] = payload.destination_longitude
        if payload.scheduled_departure:
            updates["scheduled_departure"] = payload.scheduled_departure
        if payload.scheduled_arrival:
            updates["scheduled_arrival"] = payload.scheduled_arrival
        if payload.selected_route_id:
            updates["selected_route_id"] = payload.selected_route_id
        if payload.route_corridor_meters:
            updates["route_corridor_meters"] = payload.route_corridor_meters
        if payload.status:
            updates["status"] = payload.status.upper()

        if updates:
            updated = update_journey_record(jid, updates)
            if updated:
                existing = updated

        # Automatically query and persist route alternatives if requested
        if payload.origin_latitude and payload.origin_longitude and payload.destination_latitude and payload.destination_longitude:
            try:
                routes = osrm_service.get_alternative_routes(
                    origin_lat=payload.origin_latitude,
                    origin_lon=payload.origin_longitude,
                    dest_lat=payload.destination_latitude,
                    dest_lon=payload.destination_longitude,
                    origin_name=payload.origin_name,
                    dest_name=payload.destination_name,
                )
                for r in routes:
                    is_sel = (r["route_id"] == payload.selected_route_id)
                    save_journey_route(
                        journey_id=jid,
                        route_name=r["name"],
                        geometry=r["geometry"],
                        distance_meters=r["distance_meters"],
                        duration_seconds=r["duration_seconds"],
                        safety_assessment=r.get("safety_assessment"),
                        is_selected=is_sel,
                        route_id=r["route_id"]
                    )
            except Exception as exc:
                app_logger.warning(f"[JOURNEY REUSE] Auto route options generation note: {exc}")

        return {
            **existing,
            "id": jid,
            "journey_id": jid,
            "success": True,
            "message": f"Reused existing {existing.get('status')} journey",
            "data": existing,
            "journey": existing,
        }

    jid = f"journey_{dev_id}_{uuid.uuid4().hex[:10]}"

    journey_dict = {
        "journey_id": jid,
        "device_id": dev_id,
        "status": (payload.status or "SCHEDULED").upper(),
        "origin": payload.origin_name,
        "origin_name": payload.origin_name,
        "destination": payload.destination_name,
        "destination_name": payload.destination_name,
        "start_latitude": payload.origin_latitude,
        "start_longitude": payload.origin_longitude,
        "end_latitude": payload.destination_latitude,
        "end_longitude": payload.destination_longitude,
        "scheduled_departure": payload.scheduled_departure,
        "scheduled_arrival": payload.scheduled_arrival,
        "actual_departure": None,
        "actual_arrival": None,
        "selected_route_id": payload.selected_route_id,
        "route_corridor_meters": payload.route_corridor_meters or 75.0,
        "total_distance_km": 0.0,
        "duration_seconds": 0,
        "duration_formatted": "00:00:00",
        "points_count": 0,
        "route_coordinates_json": "[]",
        "created_at": now_iso,
        "updated_at": now_iso,
    }

    created = create_journey_record(journey_dict)

    # Automatically query and persist route alternatives for this journey
    if payload.origin_latitude and payload.origin_longitude and payload.destination_latitude and payload.destination_longitude:
        try:
            routes = osrm_service.get_alternative_routes(
                origin_lat=payload.origin_latitude,
                origin_lon=payload.origin_longitude,
                dest_lat=payload.destination_latitude,
                dest_lon=payload.destination_longitude,
                origin_name=payload.origin_name,
                dest_name=payload.destination_name,
            )
            for r in routes:
                is_sel = (r["route_id"] == payload.selected_route_id)
                save_journey_route(
                    journey_id=jid,
                    route_name=r["name"],
                    geometry=r["geometry"],
                    distance_meters=r["distance_meters"],
                    duration_seconds=r["duration_seconds"],
                    safety_assessment=r.get("safety_assessment"),
                    is_selected=is_sel,
                    route_id=r["route_id"]
                )
        except Exception as exc:
            app_logger.warning(f"[JOURNEY CREATE] Auto route options generation note: {exc}")

    return {
        **created,
        "id": jid,
        "journey_id": jid,
        "success": True,
        "message": "Journey scheduled successfully",
        "data": created,
        "journey": created,
    }


@router.get("", response_model=List[Dict[str, Any]])
@router.get("/", response_model=List[Dict[str, Any]], include_in_schema=False)
def list_journeys(
    status: Optional[str] = Query(None, description="Filter by status (SCHEDULED, ACTIVE, PAUSED, COMPLETED, CANCELLED)"),
    device_id: Optional[str] = Query(None, description="Filter by device IMEI"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
):
    """Lists journeys with filtering and pagination."""
    return list_journeys_records(status=status, device_id=device_id, limit=limit, offset=skip)


def _broadcast_journey_lifecycle(journey: Dict[str, Any], event_type: str, timestamp: str) -> None:
    """Notify monitoring dashboards that a journey started or ended."""
    jid = str(journey.get("journey_id") or journey.get("id") or "")
    device_id = str(journey.get("device_id") or "")
    started = event_type == "JOURNEY_STARTED"
    try:
        ws_manager.broadcast({
            "type": event_type,
            "event_type": event_type,
            "event_id": f"{jid}:{event_type}",
            "journey_id": jid,
            "device_id": device_id,
            "severity": "INFO",
            "title": "Journey Started" if started else "Journey Completed",
            "message": (
                f"Journey {jid} for device {device_id} has started."
                if started else
                f"Journey {jid} for device {device_id} has ended."
            ),
            "requires_alarm": False,
            "timestamp": timestamp,
        })
    except Exception as exc:  # broadcasting must never break the state change
        app_logger.warning(f"[JOURNEY LIFECYCLE] Broadcast failed for {jid}: {exc}")


@router.post("/{journey_id}/start", response_model=Dict[str, Any])
def start_specific_journey(journey_id: str):
    """Marks a scheduled journey as ACTIVE upon departure (idempotent)."""
    clean_jid = (journey_id or "").strip()
    journey = get_journey_by_id(clean_jid)
    if not journey:
        raise HTTPException(status_code=404, detail=f"Journey {clean_jid} not found")

    if journey.get("status") == "ACTIVE":
        return {
            **journey,
            "id": clean_jid,
            "journey_id": clean_jid,
            "success": True,
            "message": f"Journey {clean_jid} is already active",
            "journey": journey,
            "data": journey,
        }

    now_iso = current_iso_timestamp()
    updated = update_journey_record(clean_jid, {
        "status": "ACTIVE",
        "actual_departure": journey.get("actual_departure") or now_iso,
        "start_time": journey.get("start_time") or now_iso
    }) or journey
    _broadcast_journey_lifecycle(updated, "JOURNEY_STARTED", now_iso)

    return {
        **updated,
        "id": clean_jid,
        "journey_id": clean_jid,
        "success": True,
        "message": f"Journey {clean_jid} started",
        "journey": updated,
        "data": updated,
    }


@router.post("/{journey_id}/complete", response_model=Dict[str, Any])
def complete_specific_journey(journey_id: str):
    """Marks an active journey as COMPLETED upon arrival (idempotent)."""
    clean_jid = (journey_id or "").strip()
    journey = get_journey_by_id(clean_jid)
    if not journey:
        raise HTTPException(status_code=404, detail=f"Journey {clean_jid} not found")

    if journey.get("status") == "COMPLETED":
        return {
            **journey,
            "id": clean_jid,
            "journey_id": clean_jid,
            "success": True,
            "message": f"Journey {clean_jid} is already marked as completed",
            "journey": journey,
            "data": journey,
        }

    now_iso = current_iso_timestamp()
    updated = update_journey_record(clean_jid, {
        "status": "COMPLETED",
        "actual_arrival": journey.get("actual_arrival") or now_iso,
        "end_time": journey.get("end_time") or now_iso
    }) or journey
    route_monitor.reset_journey_state(clean_jid)
    _broadcast_journey_lifecycle(updated, "JOURNEY_COMPLETED", now_iso)
    return {
        **updated,
        "id": clean_jid,
        "journey_id": clean_jid,
        "success": True,
        "message": f"Journey {clean_jid} marked as completed",
        "journey": updated,
        "data": updated,
    }


@router.post("/{journey_id}/pause", response_model=Dict[str, Any])
def pause_specific_journey(journey_id: str):
    """Pauses an active journey."""
    clean_jid = (journey_id or "").strip()
    journey = get_journey_by_id(clean_jid)
    if not journey:
        raise HTTPException(status_code=404, detail=f"Journey {clean_jid} not found")

    updated = update_journey_record(clean_jid, {"status": "PAUSED"}) or journey
    return {
        **updated,
        "id": clean_jid,
        "journey_id": clean_jid,
        "success": True,
        "message": f"Journey {clean_jid} paused",
        "journey": updated,
        "data": updated,
    }


@router.post("/{journey_id}/cancel", response_model=Dict[str, Any])
def cancel_specific_journey(journey_id: str):
    """Cancels a scheduled or active journey."""
    clean_jid = (journey_id or "").strip()
    journey = get_journey_by_id(clean_jid)
    if not journey:
        raise HTTPException(status_code=404, detail=f"Journey {clean_jid} not found")

    updated = update_journey_record(clean_jid, {"status": "CANCELLED"}) or journey
    route_monitor.reset_journey_state(clean_jid)
    return {
        **updated,
        "id": clean_jid,
        "journey_id": clean_jid,
        "success": True,
        "message": f"Journey {clean_jid} cancelled",
        "journey": updated,
        "data": updated,
    }


# =============================================================================
# 4. REAL-TIME LOCATION, PATH, ROUTE, EVENTS & ALERTS ENDPOINTS
# =============================================================================

@router.get("/{journey_id}/live", response_model=Dict[str, Any])
def get_journey_live_position(journey_id: str):
    """
    Returns latest verified GPS position, corridor status, and distance from approved route.
    Frontend displays:
    - Approved route: Blue line
    - Actual travelled path: Green line
    - Live GPS watch: Red dot
    """
    clean_jid = (journey_id or "").strip()
    journey = get_journey_by_id(clean_jid)
    if not journey:
        raise HTTPException(status_code=404, detail=f"Journey {clean_jid} not found")

    dev_id = journey["device_id"]
    dev = device_service.get_device_by_id(dev_id) or {}

    lat = dev.get("last_latitude")
    lon = dev.get("last_longitude")
    ts = dev.get("last_valid_gps_time") or dev.get("last_seen")
    gps_valid = bool(dev.get("gps_valid", False) and lat is not None and lon is not None)
    is_current = bool(dev.get("location_is_current", False))

    selected_route = get_selected_route_for_journey(clean_jid)
    dist_meters = 0.0
    on_route = True

    if lat is not None and lon is not None and selected_route and selected_route.get("geometry"):
        coords = selected_route["geometry"].get("coordinates", [])
        if coords:
            dist_meters = calculate_perpendicular_distance_to_route(float(lat), float(lon), coords)
            corridor = float(journey.get("route_corridor_meters") or 75.0)
            on_route = (dist_meters <= corridor)

    return {
        "device_id": dev_id,
        "journey_id": clean_jid,
        "latitude": lat,
        "longitude": lon,
        "timestamp": ts,
        "gps_valid": gps_valid,
        "location_is_current": is_current,
        "speed_kmh": float(dev.get("speed_kmh") or dev.get("speed") or 0.0),
        "on_selected_route": on_route,
        "distance_from_route_meters": dist_meters,
        "route_corridor_meters": float(journey.get("route_corridor_meters") or 75.0),
        "journey_status": journey.get("status")
    }


@router.get("/{journey_id}/path", response_model=List[Dict[str, Any]])
def get_journey_travelled_path(
    journey_id: str,
    since: Optional[str] = Query(None, description="Retrieve points recorded after this ISO-8601 timestamp")
):
    """
    Returns actual travelled GPS path recorded for this journey.
    Used by frontend to render the green travelled route line.
    Supports incremental fetching via 'since' parameter.
    """
    clean_jid = (journey_id or "").strip()
    journey = get_journey_by_id(clean_jid)
    if not journey:
        raise HTTPException(status_code=404, detail=f"Journey {clean_jid} not found")

    points = get_journey_gps_points(clean_jid, since=since, limit=2500)
    if not points and not since:
        # Fallback to route_coordinates if stored directly on journey record
        fallback = journey.get("route_coordinates", [])
        seen_ts = set()
        cleaned_fallback = []
        for p in fallback:
            ts = p.get("timestamp") or p.get("gps_timestamp")
            if ts and ts in seen_ts:
                continue
            if ts:
                seen_ts.add(ts)
            p_copy = dict(p)
            p_copy["gps_valid"] = True
            p_copy["gps_fix"] = True
            p_copy["location_is_current"] = True
            p_copy["valid"] = True
            if "timestamp" not in p_copy and "gps_timestamp" in p_copy:
                p_copy["timestamp"] = p_copy["gps_timestamp"]
            cleaned_fallback.append(p_copy)
        return cleaned_fallback
    return points


@router.get("/{journey_id}/route", response_model=Dict[str, Any])
def get_journey_approved_route(journey_id: str):
    """
    Returns approved route geometry (GeoJSON LineString) and safety evaluation.
    Used by frontend to render the blue approved route line.
    """
    clean_jid = (journey_id or "").strip()
    journey = get_journey_by_id(clean_jid)
    if not journey:
        raise HTTPException(status_code=404, detail=f"Journey {clean_jid} not found")

    route = get_selected_route_for_journey(clean_jid)
    if not route:
        # If no route is selected yet, return the first available alternative
        routes = get_journey_routes(clean_jid)
        if routes:
            return routes[0]
        raise HTTPException(status_code=404, detail=f"No approved route found for journey {clean_jid}")

    return route


@router.get("/{journey_id}/events", response_model=List[Dict[str, Any]])
def get_journey_event_history(
    journey_id: str,
    limit: int = Query(500, ge=1, le=2000)
):
    """
    Returns complete chronological event history for this journey:
    - Route deviations and rejoins
    - Schedule alerts (early departure, delayed start, overdue)
    - Destination arrival
    - GPS signal loss notices
    """
    clean_jid = (journey_id or "").strip()
    journey = get_journey_by_id(clean_jid)
    if not journey:
        raise HTTPException(status_code=404, detail=f"Journey {clean_jid} not found")

    return get_journey_events(clean_jid, limit=limit)


@router.get("/{journey_id}/alerts", response_model=List[Dict[str, Any]])
def get_journey_alerts(journey_id: str):
    """Returns alerts generated during this journey."""
    clean_jid = (journey_id or "").strip()
    journey = get_journey_by_id(clean_jid)
    if not journey:
        raise HTTPException(status_code=404, detail=f"Journey {clean_jid} not found")

    dev_id = journey["device_id"]
    try:
        all_alerts = alert_service.list_alerts(device_id=dev_id, limit=100)
    except Exception as exc:
        app_logger.warning(f"[JOURNEY ALERTS] Error retrieving alerts for device {dev_id}: {exc}")
        all_alerts = []

    # Filter alerts occurring during journey timeframe
    start_time = journey.get("start_time") or journey.get("created_at")
    end_time = journey.get("end_time")

    results = []
    for a in all_alerts:
        ts = a.get("timestamp") or a.get("created_at") or a.get("event_time")
        if start_time and ts:
            if ts >= start_time:
                if not end_time or ts <= end_time:
                    results.append(a)
        else:
            results.append(a)
    return results


@router.get("/{journey_id}", response_model=Dict[str, Any])
def get_journey_details(journey_id: str):
    """Retrieves full details of a specific journey including selected route."""
    clean_jid = (journey_id or "").strip()
    journey = get_journey_by_id(clean_jid)
    if not journey:
        raise HTTPException(status_code=404, detail=f"Journey {clean_jid} not found")

    selected_route = get_selected_route_for_journey(clean_jid)
    routes = get_journey_routes(clean_jid)
    journey["selected_route"] = selected_route
    journey["available_routes"] = routes
    return journey


# =============================================================================
# 5. WEBSOCKET REAL-TIME STREAMING
# =============================================================================

@router.websocket("/{journey_id}/ws")
async def journey_websocket_endpoint(websocket: WebSocket, journey_id: str):
    """
    WebSocket endpoint streaming live GPS telemetry, corridor status,
    deviation alerts, and journey events to connected frontend clients.
    """
    await websocket.accept()
    ws_manager.connect(websocket)
    clean_jid = (journey_id or "").strip()

    try:
        # Send initial journey handshake state
        journey = get_journey_by_id(clean_jid)
        if journey:
            await websocket.send_text(json.dumps({
                "type": "JOURNEY_INIT",
                "journey_id": clean_jid,
                "status": journey.get("status"),
                "device_id": journey.get("device_id")
            }))

        while True:
            # Keep connection alive; client can send ping or commands
            data = await websocket.receive_text()
            try:
                msg = json.loads(data)
                if msg.get("type") == "ping":
                    await websocket.send_text(json.dumps({"type": "pong"}))
            except Exception:
                pass
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception as exc:
        ws_manager.disconnect(websocket)
        app_logger.debug(f"[WS DISCONNECT] Journey {clean_jid}: {exc}")
