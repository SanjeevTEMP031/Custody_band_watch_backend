"""
Geofence FastAPI Route Handlers
Implements CRUD, device assignment, event history, alert acknowledgment,
and WebSocket streams for real-time monitoring and alarms.
"""

from typing import List, Dict, Any, Optional
from fastapi import (
    APIRouter,
    HTTPException,
    status,
    Query,
    Request,
    WebSocket,
    WebSocketDisconnect,
)
from ..core.dependencies import get_current_user
from ..services.alert_service import ws_manager
from .service import geofence_service
from .alerts import (
    list_geofence_alerts,
    acknowledge_geofence_alert,
    silence_geofence_alert,
    resolve_geofence_alert,
)
from .schemas import (
    GeofenceCreate,
    GeofenceUpdate,
    GeofenceResponse,
    AssignDevicesRequest,
    GeofenceEventResponse,
    GeofenceAlertResponse,
    GeofenceAlertAcknowledgeRequest,
    GeofenceValidateRequest,
)

router = APIRouter(prefix="/geofences", tags=["Geofences & Perimeter Monitoring"])


async def get_user_identity(request: Request) -> str:
    """Attempts to extract authenticated officer identity, falling back to 'admin'."""
    try:
        user = await get_current_user(request)
        return user.get("email") or user.get("full_name") or "admin"
    except Exception:
        return "admin"


# =============================================================================
# 1. STATIC GEOFENCE ROUTES (MUST BE REGISTERED BEFORE /{geofence_id})
# =============================================================================

@router.get("", response_model=List[Dict[str, Any]])
@router.get("/", response_model=List[Dict[str, Any]], include_in_schema=False)
def list_geofences(
    active: Optional[bool] = Query(None, description="Filter by active status"),
    device_id: Optional[str] = Query(None, description="Filter by assigned device IMEI"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500)
):
    """List all configured inclusion and exclusion zones."""
    return geofence_service.list_geofences(active=active, device_id=device_id, skip=skip, limit=limit)


@router.post("", response_model=Dict[str, Any], status_code=status.HTTP_201_CREATED)
@router.post("/", response_model=Dict[str, Any], status_code=status.HTTP_201_CREATED, include_in_schema=False)
async def create_geofence(geo_in: GeofenceCreate, request: Request):
    """
    Create a new geofence zone.
    Supports circle perimeters (center_lat, center_lon, radius_meters)
    and polygon perimeters (polygon_coordinates with minimum 3 closed vertices).
    """
    try:
        user_id = await get_user_identity(request)
        return geofence_service.create_geofence(geo_in, created_by=user_id)
    except ValueError as val_err:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(val_err))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to create geofence: {exc}")


@router.get("/alerts", response_model=List[Dict[str, Any]])
def get_geofence_alerts(
    status: Optional[str] = Query(None, description="Filter by alert status: ACTIVE, ACKNOWLEDGED, RESOLVED"),
    severity: Optional[str] = Query(None, description="Filter by severity: HIGH, CRITICAL, INFO"),
    device_id: Optional[str] = Query(None, description="Filter by device IMEI"),
    geofence_id: Optional[str] = Query(None, description="Filter by geofence ID"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500)
):
    """Retrieve geofence-specific breach and boundary alerts."""
    return list_geofence_alerts(
        status=status,
        severity=severity,
        device_id=device_id,
        geofence_id=geofence_id,
        limit=limit,
        offset=skip
    )


@router.post("/alerts/{alert_id}/acknowledge", response_model=Dict[str, Any])
@router.put("/alerts/{alert_id}/acknowledge", response_model=Dict[str, Any], include_in_schema=False)
async def acknowledge_geofence_alert_endpoint(
    alert_id: str,
    request: Request,
    ack_req: Optional[GeofenceAlertAcknowledgeRequest] = None
):
    """
    Acknowledge a geofence alert.
    Silences audible dashboard alarms, marks alert ACKNOWLEDGED,
    and broadcasts state update via WebSocket.
    """
    user_id = await get_user_identity(request)
    notes = ack_req.notes if ack_req else None
    updated = acknowledge_geofence_alert(alert_id, officer_email=user_id, notes=notes)
    if not updated:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Geofence alert {alert_id} not found")
    return {
        "status": "success",
        "message": "Geofence alert acknowledged and alarm silenced",
        "alert": updated
    }


@router.post("/alerts/{alert_id}/silence", response_model=Dict[str, Any])
@router.put("/alerts/{alert_id}/silence", response_model=Dict[str, Any], include_in_schema=False)
async def silence_geofence_alert_endpoint(
    alert_id: str,
    request: Request
):
    """
    Silence a geofence alert audible alarm.
    Sets requires_alarm=False, updates status to ACKNOWLEDGED,
    and broadcasts state update via WebSocket while keeping alert in history.
    """
    user_id = await get_user_identity(request)
    notes = None
    try:
        body = await request.json()
        if isinstance(body, dict):
            notes = body.get("notes") or body.get("reason")
    except Exception:
        pass
    updated = silence_geofence_alert(alert_id, officer_email=user_id, notes=notes)
    if not updated:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Geofence alert {alert_id} not found")
    return {
        "status": "success",
        "message": "Geofence alert silenced",
        "alert": updated
    }


@router.post("/alerts/silence", response_model=Dict[str, Any])
async def silence_geofence_alert_body(request: Request):
    """Silence geofence alert with alert ID passed in body or query."""
    body = {}
    try:
        body = await request.json()
    except Exception:
        pass
    alert_id = body.get("id") or body.get("alert_id") or request.query_params.get("id") or request.query_params.get("alert_id")
    if not alert_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing alert id")
    user_id = await get_user_identity(request)
    notes = body.get("notes") or body.get("reason")
    updated = silence_geofence_alert(alert_id, officer_email=user_id, notes=notes)
    if not updated:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Geofence alert {alert_id} not found")
    return {
        "status": "success",
        "message": "Geofence alert silenced",
        "alert": updated
    }


@router.post("/alerts/{alert_id}/resolve", response_model=Dict[str, Any])
async def resolve_geofence_alert_endpoint(
    alert_id: str,
    request: Request
):
    """Mark a geofence alert as resolved."""
    user_id = await get_user_identity(request)
    updated = resolve_geofence_alert(alert_id, officer_email=user_id)
    if not updated:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Geofence alert {alert_id} not found")
    return {
        "status": "success",
        "message": "Geofence alert resolved",
        "alert": updated
    }


@router.post("/validate-position", response_model=Dict[str, Any])
def validate_position(req: GeofenceValidateRequest):
    """
    Check if a device's coordinates are currently compliant with all assigned geofences.
    Legacy/backward-compatible compliance check endpoint.
    """
    return geofence_service.check_position_compliance(req.device_id, req.latitude, req.longitude)


# =============================================================================
# 2. WEBSOCKET REAL-TIME STREAMING
# =============================================================================

@router.websocket("/ws")
@router.websocket("/live")
async def websocket_geofence_events(websocket: WebSocket):
    """
    Real-time WebSocket feed for geofence entry/exit events and alarms.
    Dashboard clients receive instant messages with requires_alarm=True on breach.
    """
    await websocket.accept()
    ws_manager.connect(websocket)
    try:
        # Send initial active alerts snapshot
        active_alerts = list_geofence_alerts(status="ACTIVE", limit=50)
        await websocket.send_json({
            "type": "INIT",
            "active_geofence_alerts": active_alerts
        })
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        ws_manager.disconnect(websocket)


# =============================================================================
# 3. DYNAMIC GEOFENCE ROUTES (/{geofence_id})
# =============================================================================

@router.get("/{geofence_id}", response_model=Dict[str, Any])
def get_geofence(geofence_id: str):
    """Get details for a specific geofence zone."""
    geo = geofence_service.get_geofence(geofence_id)
    if not geo:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Geofence {geofence_id} not found")
    return geo


@router.put("/{geofence_id}", response_model=Dict[str, Any])
def update_geofence(geofence_id: str, geo_up: GeofenceUpdate):
    """Update an existing geofence's boundary, radius, name, or active status."""
    try:
        updated = geofence_service.update_geofence(geofence_id, geo_up)
        if not updated:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Geofence {geofence_id} not found")
        return updated
    except ValueError as val_err:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(val_err))


@router.delete("/{geofence_id}", response_model=Dict[str, Any])
def delete_geofence(geofence_id: str):
    """Delete a geofence zone and its associated assignments."""
    success = geofence_service.delete_geofence(geofence_id)
    if not success:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Geofence {geofence_id} not found")
    return {"status": "success", "message": f"Geofence {geofence_id} removed"}


@router.post("/{geofence_id}/assign-devices", response_model=Dict[str, Any])
def assign_devices_to_geofence(geofence_id: str, req: AssignDevicesRequest):
    """Assign one or more custody watch devices to a geofence."""
    try:
        return geofence_service.assign_devices(geofence_id, req.device_ids)
    except ValueError as err:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(err))


@router.delete("/{geofence_id}/devices/{device_id}", response_model=Dict[str, Any])
def remove_device_from_geofence(geofence_id: str, device_id: str):
    """Remove a custody watch device from a geofence."""
    success = geofence_service.remove_device(geofence_id, device_id)
    if not success:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Assignment for device {device_id} in geofence {geofence_id} not found")
    return {"status": "success", "message": f"Device {device_id} unassigned from geofence {geofence_id}"}


@router.get("/{geofence_id}/devices", response_model=List[Dict[str, Any]])
def get_geofence_devices(geofence_id: str):
    """List all custody watch devices assigned to a geofence."""
    return geofence_service.get_assigned_devices(geofence_id)


@router.get("/{geofence_id}/events", response_model=List[Dict[str, Any]])
def get_geofence_events(
    geofence_id: str,
    event_type: Optional[str] = Query(None, description="Filter by event type"),
    device_id: Optional[str] = Query(None, description="Filter by device IMEI"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500)
):
    """List historical transition events (entry, exit, outside) for a geofence."""
    return geofence_service.get_geofence_events(
        geofence_id=geofence_id,
        event_type=event_type,
        device_id=device_id,
        skip=skip,
        limit=limit
    )
