"""
Alert API Routes
Authoritative incident management, alert filtering, acknowledgment, resolution workflows,
and real-time live WebSocket broadcasting.
"""

from typing import List, Dict, Any, Optional
from fastapi import (
    APIRouter,
    HTTPException,
    status,
    Query,
    Depends,
    Request,
    WebSocket,
    WebSocketDisconnect,
)
from ..alerts.alert_schemas import AlertAcknowledgeRequest, AlertResolveRequest
from ..services.alert_service import alert_service, ws_manager
from ..core.dependencies import get_current_user, security_scheme

router = APIRouter(prefix="/alerts", tags=["Alerts"])


async def get_user_identity(request: Request) -> str:
    """Attempts to extract authenticated officer email, falling back to 'admin'."""
    try:
        user = await get_current_user(request)
        return user.get("email") or user.get("full_name") or "admin"
    except Exception:
        return "admin"


# -----------------------------------------------------------------------------
# 1. LIST ALERTS (GET /api/v1/alerts)
# -----------------------------------------------------------------------------
@router.get("", response_model=List[Dict[str, Any]])
@router.get("/", response_model=List[Dict[str, Any]], include_in_schema=False)
def list_alerts(
    status: Optional[str] = Query(None, description="Filter by status: ACTIVE, ACKNOWLEDGED, RESOLVED, CLOSED"),
    severity: Optional[str] = Query(None, description="Filter by severity: CRITICAL, HIGH, MEDIUM, INFO"),
    alert_type: Optional[str] = Query(None, description="Filter by alert type code (e.g. SOS_ALERT, STRAP_CUT_ALERT)"),
    device_id: Optional[str] = Query(None, description="Filter by device IMEI / ID"),
    start_date: Optional[str] = Query(None, description="Start date ISO timestamp filter"),
    end_date: Optional[str] = Query(None, description="End date ISO timestamp filter"),
    skip: int = Query(0, ge=0),
    limit: int = Query(500, ge=1, le=1000)
):
    """
    List security alerts and tamper events with multi-attribute filtering.
    """
    return alert_service.list_alerts(
        status=status,
        severity=severity,
        alert_type=alert_type,
        device_id=device_id,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        offset=skip
    )


# -----------------------------------------------------------------------------
# 2. ACTIVE ALERTS (GET /api/v1/alerts/active)
# -----------------------------------------------------------------------------
@router.get("/active", response_model=List[Dict[str, Any]])
@router.get("/active/", response_model=List[Dict[str, Any]], include_in_schema=False)
def list_active_alerts(
    severity: Optional[str] = Query(None, description="Filter by severity: CRITICAL, HIGH, MEDIUM, INFO"),
    alert_type: Optional[str] = Query(None, description="Filter by alert type"),
    device_id: Optional[str] = Query(None, description="Filter by device IMEI"),
    skip: int = Query(0, ge=0),
    limit: int = Query(500, ge=1, le=1000)
):
    """
    Retrieve all currently unresolved/active alerts requiring officer attention.
    """
    return alert_service.list_alerts(
        status="ACTIVE",
        severity=severity,
        alert_type=alert_type,
        device_id=device_id,
        limit=limit,
        offset=skip
    )


# -----------------------------------------------------------------------------
# 3. STATS / SUMMARY
# -----------------------------------------------------------------------------
@router.get("/stats", response_model=Dict[str, Any])
@router.get("/summary", response_model=Dict[str, Any], include_in_schema=False)
@router.get("/count", response_model=Dict[str, Any], include_in_schema=False)
@router.get("/unread", response_model=Dict[str, Any], include_in_schema=False)
def get_alert_stats():
    """Returns aggregated alert counters."""
    all_alerts = alert_service.list_alerts(limit=1000)
    total = len(all_alerts)
    active_count = sum(1 for a in all_alerts if a.get("status") in ("ACTIVE", "NEW"))
    ack_count = sum(1 for a in all_alerts if a.get("status") == "ACKNOWLEDGED")
    resolved_count = sum(1 for a in all_alerts if a.get("status") == "RESOLVED")
    critical_count = sum(1 for a in all_alerts if a.get("severity") == "critical" and a.get("status") in ("ACTIVE", "NEW", "ACKNOWLEDGED"))
    return {
        "total": total,
        "count": total,
        "active": active_count,
        "new": active_count,
        "unread": active_count,
        "acknowledged": ack_count,
        "resolved": resolved_count,
        "critical": critical_count
    }


# -----------------------------------------------------------------------------
# 4. ALERTS BY DEVICE (GET /api/v1/alerts/device/{device_id})
# -----------------------------------------------------------------------------
@router.get("/device/{device_id}", response_model=List[Dict[str, Any]])
def get_alerts_by_device(
    device_id: str,
    status: Optional[str] = Query(None),
    severity: Optional[str] = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500)
):
    """Retrieve full alert history for a specific watch IMEI."""
    return alert_service.list_alerts(
        device_id=device_id,
        status=status,
        severity=severity,
        limit=limit,
        offset=skip
    )


# -----------------------------------------------------------------------------
# 5. WEBSOCKET LIVE STREAMING (WS /api/v1/alerts/live)
# -----------------------------------------------------------------------------
@router.websocket("/live")
async def websocket_live_alerts(websocket: WebSocket):
    """
    Real-time WebSocket feed for new and updated alerts.
    Subscribers receive instant push notifications as alerts trigger or resolve.
    """
    await websocket.accept()
    ws_manager.connect(websocket)
    try:
        # Send initial active alerts snapshot
        active = alert_service.list_alerts(status="ACTIVE", limit=50)
        await websocket.send_json({
            "type": "INIT",
            "active_alerts": active
        })
        while True:
            # Keepalive receiver
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        ws_manager.disconnect(websocket)


# -----------------------------------------------------------------------------
# 6. GET SINGLE ALERT (GET /api/v1/alerts/{alert_id})
# -----------------------------------------------------------------------------
@router.get("/{alert_id}", response_model=Dict[str, Any])
def get_alert(alert_id: str):
    """Retrieve full incident details for an alarm by integer ID or string alert_id."""
    alert = alert_service.get_alert_by_id(alert_id)
    if not alert:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Alert not found")
    return alert


# -----------------------------------------------------------------------------
# 7. ACKNOWLEDGE ALERT (POST /api/v1/alerts/{alert_id}/acknowledge)
# -----------------------------------------------------------------------------
@router.post("/{alert_id}/acknowledge", response_model=Dict[str, Any])
@router.post("/{alert_id}/ack", response_model=Dict[str, Any], include_in_schema=False)
@router.put("/{alert_id}/acknowledge", response_model=Dict[str, Any], include_in_schema=False)
@router.put("/{alert_id}/ack", response_model=Dict[str, Any], include_in_schema=False)
async def acknowledge_alert(
    alert_id: str,
    request: Request
):
    """
    Acknowledge an incoming security alert.
    Note: Acknowledgement does not automatically resolve an ongoing hardware alert.
    """
    notes = None
    try:
        body = await request.json()
        if isinstance(body, dict):
            notes = body.get("notes") or body.get("resolution_notes")
    except Exception:
        pass

    officer_email = await get_user_identity(request)
    res = alert_service.acknowledge_alert(alert_id, acknowledged_by=officer_email, notes=notes)
    if not res:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Alert not found")
    return res


# -----------------------------------------------------------------------------
# 8. RESOLVE ALERT (POST /api/v1/alerts/{alert_id}/resolve)
# -----------------------------------------------------------------------------
@router.post("/{alert_id}/resolve", response_model=Dict[str, Any])
@router.put("/{alert_id}/resolve", response_model=Dict[str, Any], include_in_schema=False)
async def resolve_alert(
    alert_id: str,
    request: Request
):
    """Mark an alert incident as resolved with explanation notes."""
    notes = "Resolved by officer"
    try:
        body = await request.json()
        if isinstance(body, dict):
            notes = body.get("resolution_notes") or body.get("notes") or notes
    except Exception:
        pass

    officer_email = await get_user_identity(request)
    res = alert_service.resolve_alert(alert_id, resolved_by=officer_email, resolution_notes=notes)
    if not res:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Alert not found")
    return res


# -----------------------------------------------------------------------------
# Backwards-compatible body-based acknowledge & resolve endpoints
# -----------------------------------------------------------------------------
@router.post("/acknowledge", response_model=Dict[str, Any])
@router.post("/ack", response_model=Dict[str, Any], include_in_schema=False)
async def acknowledge_alert_body(request: Request):
    """Acknowledge alert with alert ID passed in body or query."""
    body = {}
    try:
        body = await request.json()
    except Exception:
        pass
    alert_id = body.get("id") or body.get("alert_id") or request.query_params.get("id") or request.query_params.get("alert_id")
    if not alert_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing alert id")
    notes = body.get("notes") or body.get("resolution_notes")
    officer_email = await get_user_identity(request)
    res = alert_service.acknowledge_alert(alert_id, acknowledged_by=officer_email, notes=notes)
    if not res:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Alert not found")
    return res


@router.post("/resolve", response_model=Dict[str, Any])
async def resolve_alert_body(request: Request):
    """Resolve alert with alert ID passed in body or query."""
    body = {}
    try:
        body = await request.json()
    except Exception:
        pass
    alert_id = body.get("id") or body.get("alert_id") or request.query_params.get("id") or request.query_params.get("alert_id")
    if not alert_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing alert id")
    notes = body.get("resolution_notes") or body.get("notes") or "Resolved by officer"
    officer_email = await get_user_identity(request)
    res = alert_service.resolve_alert(alert_id, resolved_by=officer_email, resolution_notes=notes)
    if not res:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Alert not found")
    return res


@router.put("/{alert_id}", response_model=Dict[str, Any])
@router.patch("/{alert_id}", response_model=Dict[str, Any], include_in_schema=False)
async def update_alert(
    alert_id: str,
    request: Request
):
    """Direct status update for an alert (ACKNOWLEDGED / RESOLVED)."""
    try:
        body = await request.json()
    except Exception:
        body = {}

    st = body.get("status", "ACKNOWLEDGED").upper()
    notes = body.get("notes") or body.get("resolution_notes")
    officer_email = await get_user_identity(request)

    if st == "RESOLVED":
        return alert_service.resolve_alert(alert_id, resolved_by=officer_email, resolution_notes=notes or "Resolved")
    else:
        return alert_service.acknowledge_alert(alert_id, acknowledged_by=officer_email, notes=notes)
