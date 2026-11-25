"""
Geofence Alerts & Real-Time Notification Module
Manages geofence alarm states, persistence in SQLite and Firestore,
and WebSocket dispatch with requires_alarm flags for audible client playback.
"""

import uuid
from typing import Dict, Any, Optional, List
from ..utils.datetime_utils import current_iso_timestamp
from ..utils.logger import app_logger
from ..services.alert_service import alert_service, ws_manager
from .models import get_db_connection, sync_alert_to_firestore


def trigger_geofence_alert(
    event_id: str,
    device_id: str,
    geofence_id: str,
    geofence_name: str,
    event_type: str,
    latitude: Optional[float],
    longitude: Optional[float],
    timestamp: str,
    severity: Optional[str] = None,
    fence_type: str = "INCLUSION",
) -> Dict[str, Any]:
    """
    Creates a persistent geofence alert, dispatches it to WebSocket clients,
    and sets requires_alarm=True for breach events so frontend monitoring dashboards play audible alarms.
    Accurately distinguishes between EXCLUSION (breach on entry) and INCLUSION (breach on exit).
    """
    now_iso = current_iso_timestamp()
    alert_id = f"alert_geo_{uuid.uuid4().hex[:10]}"
    rule = fence_type.upper() if fence_type else "INCLUSION"

    is_exit = event_type == "GEOFENCE_EXIT"
    is_reentry = event_type == "GEOFENCE_REENTRY"
    is_entry = event_type == "GEOFENCE_ENTRY"

    if rule == "EXCLUSION":
        # Entering or being inside an exclusion zone is a breach
        is_breach = is_entry or is_reentry
    else:
        # Exiting an inclusion zone is a breach
        is_breach = is_exit

    if severity:
        sev = severity.upper()
    else:
        sev = "HIGH" if is_breach else "INFO"

    requires_alarm = (sev == "HIGH" or is_breach)

    if is_exit:
        title = "Outside Geofence"
        if rule == "EXCLUSION":
            msg = f"GPS device {device_id} exited exclusion zone '{geofence_name}'."
            core_alert_type = "EXCLUSION_ZONE_CLEARED"
        else:
            msg = f"GPS device {device_id} exited assigned geofence '{geofence_name}'."
            core_alert_type = "GEOFENCE_BREACH"
    elif is_reentry or is_entry:
        title = "Inside Geofence"
        if rule == "EXCLUSION":
            msg = f"GPS device {device_id} entered restricted exclusion zone '{geofence_name}'."
            core_alert_type = "EXCLUSION_ZONE_BREACH"
        else:
            msg = f"GPS device {device_id} entered geofence '{geofence_name}'."
            core_alert_type = "GEOFENCE_ENTRY"
    else:
        title = "Geofence Event"
        msg = f"GPS device {device_id} geofence notice for '{geofence_name}'."
        core_alert_type = "GEOFENCE_EVENT"

    # 1. Insert into geofence_alerts table
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO geofence_alerts (
            id, event_id, device_id, geofence_id, severity, status, requires_alarm,
            title, message, latitude, longitude, event_type, event_time, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        alert_id, event_id, device_id, geofence_id, sev, "ACTIVE",
        1 if requires_alarm else 0, title, msg, latitude, longitude,
        event_type, timestamp, now_iso, now_iso
    ))
    conn.commit()
    conn.close()

    alert_doc = {
        "id": alert_id,
        "alert_id": alert_id,
        "event_id": event_id,
        "device_id": device_id,
        "geofence_id": geofence_id,
        "geofence_name": geofence_name,
        "event_type": event_type,
        "severity": sev,
        "status": "ACTIVE",
        "requires_alarm": requires_alarm,
        "title": title,
        "message": msg,
        "latitude": latitude,
        "longitude": longitude,
        "acknowledged": False,
        "acknowledged_by": None,
        "acknowledged_at": None,
        "resolved_by": None,
        "resolved_at": None,
        "timestamp": timestamp or now_iso,
        "created_at": now_iso,
        "updated_at": now_iso
    }

    # 2. Push to main alerts system so it appears in the primary officer Alert Center & tables
    core_alert_id = None
    try:
        core_alert = alert_service.trigger_alert(
            device_id=device_id,
            alert_type=core_alert_type,
            title=title,
            severity=sev.lower(),
            message=msg,
            latitude=latitude,
            longitude=longitude,
            gps_valid=True,
            gps_timestamp=timestamp or now_iso,
            source="GEOFENCE_ENGINE",
            raw_event_code=event_type,
            requires_alarm=requires_alarm,
            geofence_alert_id=alert_id
        )
        if core_alert and isinstance(core_alert, dict):
            core_alert_id = core_alert.get("alert_id")
            if core_alert_id:
                conn = get_db_connection()
                cur = conn.cursor()
                cur.execute("UPDATE geofence_alerts SET core_alert_id = ? WHERE id = ?", (core_alert_id, alert_id))
                conn.commit()
                conn.close()
                alert_doc["core_alert_id"] = core_alert_id
    except Exception as exc:
        app_logger.warning(f"[GEOFENCE ALERT] Failed linking to core alert_service: {exc}")

    # 3. Async Firestore sync
    sync_alert_to_firestore(alert_doc)

    # 4. Dispatch WebSocket real-time notification
    notification_payload = {
        "type": event_type,
        "event_type": event_type,
        "device_id": device_id,
        "geofence_id": geofence_id,
        "geofence_name": geofence_name,
        "severity": sev,
        "title": title,
        "message": msg,
        "timestamp": timestamp or now_iso,
        "latitude": latitude,
        "longitude": longitude,
        "requires_alarm": requires_alarm,
        "acknowledged": False,
        "alert_id": alert_id,
        "core_alert_id": core_alert_id,
        "event_id": event_id,
        "status": "ACTIVE",
    }
    ws_manager.broadcast(notification_payload)

    app_logger.info(
        f"[GEOFENCE NOTIFICATION DISPATCHED] Device={device_id} Type={event_type} "
        f"Geofence={geofence_name} Title='{title}' Alarm={requires_alarm}"
    )
    return alert_doc


def acknowledge_geofence_alert(
    alert_id: str,
    officer_email: str = "admin",
    notes: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """
    Acknowledges a geofence alert.
    Silences the audible alarm (requires_alarm -> False),
    updates status to ACKNOWLEDGED, and broadcasts the state change to all monitoring clients.
    Synchronizes across geofence_alerts and core alerts tables and Firestore collections.
    """
    now_iso = current_iso_timestamp()
    target_str = str(alert_id).strip()
    conn = get_db_connection()
    cur = conn.cursor()

    row = cur.execute(
        "SELECT * FROM geofence_alerts WHERE id = ? OR event_id = ? OR core_alert_id = ?",
        (target_str, target_str, target_str)
    ).fetchone()

    # Fallback to check core alerts if not found directly
    core_alert_row = None
    if not row:
        try:
            core_alert_row = cur.execute(
                "SELECT * FROM alerts WHERE alert_id = ? OR geofence_alert_id = ? OR id = ?",
                (target_str, target_str, target_str if target_str.isdigit() else -1)
            ).fetchone()
            if core_alert_row and core_alert_row["geofence_alert_id"]:
                row = cur.execute("SELECT * FROM geofence_alerts WHERE id = ?", (core_alert_row["geofence_alert_id"],)).fetchone()
        except Exception:
            pass

    if not row and not core_alert_row:
        conn.close()
        return None

    actual_id = row["id"] if row else (core_alert_row["geofence_alert_id"] or target_str)
    core_id = (row["core_alert_id"] if row else None) or (core_alert_row["alert_id"] if core_alert_row else target_str)

    if row:
        cur.execute("""
            UPDATE geofence_alerts
            SET status = 'ACKNOWLEDGED',
                requires_alarm = 0,
                acknowledged_by = ?,
                acknowledged_at = ?,
                updated_at = ?
            WHERE id = ?
        """, (officer_email, now_iso, now_iso, actual_id))
        conn.commit()

    updated = cur.execute("SELECT * FROM geofence_alerts WHERE id = ?", (actual_id,)).fetchone() if row else None
    conn.close()

    doc = dict(updated) if updated else (dict(core_alert_row) if core_alert_row else {})
    doc["id"] = actual_id
    doc["requires_alarm"] = False
    doc["acknowledged"] = True
    doc["status"] = "ACKNOWLEDGED"

    # Sync to Firestore
    sync_alert_to_firestore(doc)

    # Broadcast acknowledgment over WebSocket to immediately silence client alarms
    ack_broadcast = {
        "type": "GEOFENCE_ALERT_ACKNOWLEDGED",
        "alert_id": actual_id,
        "core_alert_id": core_id,
        "device_id": doc.get("device_id"),
        "geofence_id": doc.get("geofence_id"),
        "status": "ACKNOWLEDGED",
        "requires_alarm": False,
        "acknowledged_by": officer_email,
        "acknowledged_at": now_iso,
        "notes": notes
    }
    ws_manager.broadcast(ack_broadcast)

    # Also acknowledge core alert_service record if present
    try:
        alert_service.acknowledge_alert(core_id or actual_id, acknowledged_by=officer_email, notes=notes)
    except Exception:
        pass

    app_logger.info(f"[GEOFENCE ALARM SILENCED] Alert {actual_id} acknowledged by {officer_email}")
    return doc


def silence_geofence_alert(
    alert_id: str,
    officer_email: str = "admin",
    notes: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """
    Silences a geofence alert audible siren while retaining the alert in history.
    Sets requires_alarm=0, status=ACKNOWLEDGED, and broadcasts GEOFENCE_ALERT_ACKNOWLEDGED.
    """
    return acknowledge_geofence_alert(alert_id, officer_email=officer_email, notes=notes or "Silenced by officer")


def resolve_geofence_alert(
    alert_id: str,
    officer_email: str = "admin",
    notes: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """Marks a geofence alert incident as resolved."""
    now_iso = current_iso_timestamp()
    target_str = str(alert_id).strip()
    conn = get_db_connection()
    cur = conn.cursor()

    row = cur.execute(
        "SELECT * FROM geofence_alerts WHERE id = ? OR event_id = ? OR core_alert_id = ?",
        (target_str, target_str, target_str)
    ).fetchone()

    core_alert_row = None
    if not row:
        try:
            core_alert_row = cur.execute(
                "SELECT * FROM alerts WHERE alert_id = ? OR geofence_alert_id = ? OR id = ?",
                (target_str, target_str, target_str if target_str.isdigit() else -1)
            ).fetchone()
            if core_alert_row and core_alert_row["geofence_alert_id"]:
                row = cur.execute("SELECT * FROM geofence_alerts WHERE id = ?", (core_alert_row["geofence_alert_id"],)).fetchone()
        except Exception:
            pass

    if not row and not core_alert_row:
        conn.close()
        return None

    actual_id = row["id"] if row else (core_alert_row["geofence_alert_id"] or target_str)
    core_id = (row["core_alert_id"] if row else None) or (core_alert_row["alert_id"] if core_alert_row else target_str)

    if row:
        cur.execute("""
            UPDATE geofence_alerts
            SET status = 'RESOLVED',
                requires_alarm = 0,
                resolved_by = ?,
                resolved_at = ?,
                updated_at = ?
            WHERE id = ?
        """, (officer_email, now_iso, now_iso, actual_id))
        conn.commit()

    updated = cur.execute("SELECT * FROM geofence_alerts WHERE id = ?", (actual_id,)).fetchone() if row else None
    conn.close()

    doc = dict(updated) if updated else (dict(core_alert_row) if core_alert_row else {})
    doc["id"] = actual_id
    doc["requires_alarm"] = False
    doc["status"] = "RESOLVED"

    sync_alert_to_firestore(doc)

    ws_manager.broadcast({
        "type": "GEOFENCE_ALERT_RESOLVED",
        "alert_id": actual_id,
        "core_alert_id": core_id,
        "device_id": doc.get("device_id"),
        "geofence_id": doc.get("geofence_id"),
        "status": "RESOLVED",
        "requires_alarm": False,
        "resolved_by": officer_email,
        "resolved_at": now_iso
    })

    try:
        alert_service.resolve_alert(core_id or actual_id, resolved_by=officer_email, resolution_notes=notes)
    except Exception:
        pass

    return doc


def list_geofence_alerts(
    status: Optional[str] = None,
    severity: Optional[str] = None,
    device_id: Optional[str] = None,
    geofence_id: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
) -> List[Dict[str, Any]]:
    """Query geofence alerts with status, severity, device and geofence filters."""
    conn = get_db_connection()
    cur = conn.cursor()

    query = """
        SELECT ga.*, g.name AS geofence_name,
               COALESCE(ge.event_type, ga.event_type) AS resolved_event_type,
               COALESCE(ge.event_time, ga.event_time) AS resolved_event_time
        FROM geofence_alerts ga
        LEFT JOIN geofences g ON CAST(g.id AS TEXT) = CAST(ga.geofence_id AS TEXT)
        LEFT JOIN geofence_events ge ON ge.id = ga.event_id
        WHERE 1=1
    """
    params = []

    if status:
        query += " AND ga.status = ?"
        params.append(status.upper())
    if severity:
        query += " AND ga.severity = ?"
        params.append(severity.upper())
    if device_id:
        query += " AND ga.device_id = ?"
        params.append(str(device_id))
    if geofence_id:
        query += " AND ga.geofence_id = ?"
        params.append(str(geofence_id))

    query += " ORDER BY ga.created_at DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    rows = cur.execute(query, params).fetchall()
    conn.close()

    results = []
    for r in rows:
        d = dict(r)
        # Joined event row is authoritative; the alert's own stored copy is the fallback.
        d["event_type"] = d.pop("resolved_event_type", None)
        d["event_time"] = d.pop("resolved_event_time", None)
        st = str(d.get("status", "ACTIVE")).upper()
        if st in ("ACKNOWLEDGED", "SILENCED", "RESOLVED", "CLOSED"):
            d["requires_alarm"] = False
            d["acknowledged"] = True
        else:
            d["requires_alarm"] = bool(d.get("requires_alarm"))
            d["acknowledged"] = False
        results.append(d)
    return results
