
"""
Geofence Detection Engine

Evaluates verified GPS points against assigned geofences.

Features:
- Circular and polygon geofence detection
- Physical INSIDE / OUTSIDE tracking
- INCLUSION and EXCLUSION violation rules
- Consecutive GPS reading confirmation
- GPS timestamp and accuracy validation
- Persistent geofence state
- ENTRY, EXIT and REENTRY events
- Alert generation and Firestore event synchronization
"""

import json
import math
import threading
import uuid

from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

from ..config import settings
from ..utils.datetime_utils import current_iso_timestamp, parse_to_utc_datetime
from ..utils.logger import app_logger, gps_logger

from .geometry import (
    is_point_in_circle,
    is_point_in_polygon,
)

from .models import (
    get_db_connection,
    sync_event_to_firestore,
)

from .alerts import trigger_geofence_alert


_eval_lock = threading.RLock()

DEFAULT_CONFIRMATION_READINGS = max(
    1,
    int(getattr(settings, "GEOFENCE_CONFIRMATION_READINGS", 2)),
)


def _parse_utc_dt(ts_str: Optional[str]) -> Optional[datetime]:
    return parse_to_utc_datetime(ts_str)


def get_active_geofences_for_device(
    device_id: str,
) -> List[Dict[str, Any]]:
    """
    Retrieve active geofences assigned to a device.

    Supports direct device IDs and legacy aliases.
    """

    conn = get_db_connection()

    try:
        cur = conn.cursor()

        device_id = str(device_id).strip()
        all_ids = {device_id}

        try:
            dev_row = cur.execute(
                """
                SELECT device_id, serial_number, wearer_name
                FROM devices
                WHERE device_id = ?
                   OR serial_number = ?
                   OR wearer_name = ?
                """,
                (device_id, device_id, device_id),
            ).fetchone()

            if dev_row:
                for column in (
                    "device_id",
                    "serial_number",
                    "wearer_name",
                ):
                    value = dev_row[column]

                    if value:
                        all_ids.add(str(value).strip())

        except Exception as exc:
            app_logger.debug(
                "Device alias lookup unavailable: %s",
                exc,
            )

        identifiers = sorted(all_ids)
        placeholders = ",".join("?" for _ in identifiers)

        query = f"""
            SELECT DISTINCT g.*
            FROM geofences g
            LEFT JOIN device_geofence_assignments a
                ON g.id = a.geofence_id
            WHERE g.active = 1
              AND (
                  (
                      a.device_id IN ({placeholders})
                      AND a.active = 1
                  )
                  OR
                  g.assigned_device_id IN ({placeholders})
              )
        """

        params = identifiers + identifiers
        rows = cur.execute(query, params).fetchall()

        results = []

        for row in rows:
            fence = dict(row)

            fence["active"] = bool(fence.get("active"))
            fence["entry_alert"] = bool(
                fence.get("entry_alert", 1)
            )
            fence["exit_alert"] = bool(
                fence.get("exit_alert", 1)
            )

            polygon = fence.get("polygon_coordinates")

            if isinstance(polygon, str):
                try:
                    fence["polygon_coordinates"] = json.loads(
                        polygon
                    )
                except (ValueError, TypeError):
                    fence["polygon_coordinates"] = []

            elif not polygon:
                fence["polygon_coordinates"] = []

            results.append(fence)

        return results

    finally:
        conn.close()


def evaluate_geofence(
    device_id: str,
    latitude: float,
    longitude: float,
    timestamp: str,
    accuracy: float = 0.0,
    confirmation_readings: Optional[int] = None,
    max_age_seconds: Optional[float] = None,
) -> List[Dict[str, Any]]:
    """
    Evaluate a GPS point against assigned geofences.

    Physical states:
        INSIDE  = physically inside the boundary
        OUTSIDE = physically outside the boundary

    Violation rules:
        INCLUSION = outside is a violation
        EXCLUSION = inside is a violation

    Transitions require consecutive readings.
    """

    if not isinstance(latitude, (int, float)):
        gps_logger.warning(
            "[GEOFENCE] Non-numeric latitude: %s",
            device_id,
        )
        return []

    if not isinstance(longitude, (int, float)):
        gps_logger.warning(
            "[GEOFENCE] Non-numeric longitude: %s",
            device_id,
        )
        return []

    lat = float(latitude)
    lon = float(longitude)

    if not (
        math.isfinite(lat)
        and math.isfinite(lon)
        and -90.0 <= lat <= 90.0
        and -180.0 <= lon <= 180.0
    ):
        gps_logger.warning(
            "[GEOFENCE] Invalid coordinates device=%s",
            device_id,
        )
        return []

    if lat == 0.0 and lon == 0.0:
        gps_logger.info(
            "[GEOFENCE] Zero coordinates ignored device=%s",
            device_id,
        )
        return []

    pt_dt = _parse_utc_dt(timestamp)

    if pt_dt is None:
        gps_logger.warning(
            "[GEOFENCE] Invalid timestamp device=%s",
            device_id,
        )
        return []

    now_dt = datetime.now(timezone.utc)

    if (pt_dt - now_dt).total_seconds() > 15.0:
        gps_logger.warning(
            "[GEOFENCE] Future GPS timestamp device=%s",
            device_id,
        )
        return []

    # Reject stale GPS positions if configured or passed.
    max_age = (
        float(max_age_seconds)
        if max_age_seconds is not None
        else (
            float(settings.GEOFENCE_MAX_GPS_AGE_SECONDS)
            if hasattr(settings, "GEOFENCE_MAX_GPS_AGE_SECONDS") and settings.GEOFENCE_MAX_GPS_AGE_SECONDS is not None
            else None
        )
    )

    if max_age is not None and (now_dt - pt_dt).total_seconds() > max_age:
        gps_logger.info(
            "[GEOFENCE] Stale GPS ignored device=%s",
            device_id,
        )
        return []

    try:
        accuracy = float(accuracy or 0.0)
    except (TypeError, ValueError):
        return []

    if not math.isfinite(accuracy):
        return []

    if accuracy > 500.0:
        gps_logger.warning(
            "[GEOFENCE] Poor GPS accuracy device=%s accuracy=%s",
            device_id,
            accuracy,
        )
        return []

    threshold = (
        confirmation_readings
        if confirmation_readings is not None
        else DEFAULT_CONFIRMATION_READINGS
    )

    threshold = max(1, int(threshold))

    with _eval_lock:
        active_fences = get_active_geofences_for_device(
            device_id
        )

        if not active_fences:
            gps_logger.debug(
                "[GEOFENCE] No assigned geofences device=%s",
                device_id,
            )
            return []

        conn = get_db_connection()
        cur = conn.cursor()

        now_iso = current_iso_timestamp()
        results: List[Dict[str, Any]] = []

        try:
            for fence in active_fences:
                fence_id = str(fence["id"])
                fence_name = fence["name"]

                geometry_type = (
                    fence.get("type") or "CIRCLE"
                ).upper()

                rule_type = (
                    fence.get("fence_type") or "INCLUSION"
                ).upper()

                # ----------------------------------------
                # 1. Calculate physical containment
                # ----------------------------------------

                physically_inside = False

                if geometry_type == "POLYGON":
                    polygon = (
                        fence.get("polygon_coordinates") or []
                    )

                    physically_inside = is_point_in_polygon(
                        lat,
                        lon,
                        polygon,
                    )

                else:
                    center_lat = fence.get(
                        "center_latitude"
                    )
                    center_lon = fence.get(
                        "center_longitude"
                    )
                    radius = fence.get("radius_meters")

                    if (
                        center_lat is None
                        or center_lon is None
                        or radius is None
                        or float(radius) <= 0
                    ):
                        app_logger.warning(
                            "[GEOFENCE] Invalid circle geometry fence=%s",
                            fence_id,
                        )
                        continue

                    physically_inside = is_point_in_circle(
                        lat,
                        lon,
                        float(center_lat),
                        float(center_lon),
                        float(radius),
                    )

                # ----------------------------------------
                # 2. Physical state, independent of policy
                # ----------------------------------------

                target_state = (
                    "INSIDE"
                    if physically_inside
                    else "OUTSIDE"
                )

                is_violation = (
                    (
                        rule_type == "INCLUSION"
                        and not physically_inside
                    )
                    or
                    (
                        rule_type == "EXCLUSION"
                        and physically_inside
                    )
                )

                # ----------------------------------------
                # 3. Load previous confirmed state
                # ----------------------------------------

                state_row = cur.execute(
                    """
                    SELECT *
                    FROM device_geofence_states
                    WHERE device_id = ?
                      AND geofence_id = ?
                    """,
                    (device_id, fence_id),
                ).fetchone()

                # ----------------------------------------
                # 4. Initialize without false alert
                # ----------------------------------------

                if not state_row:
                    cur.execute(
                        """
                        INSERT INTO device_geofence_states (
                            device_id,
                            geofence_id,
                            confirmed_state,
                            candidate_state,
                            candidate_count,
                            last_valid_gps_time,
                            last_evaluated_at,
                            updated_at
                        )
                        VALUES (?, ?, ?, NULL, 0, ?, ?, ?)
                        """,
                        (
                            device_id,
                            fence_id,
                            target_state,
                            timestamp,
                            now_iso,
                            now_iso,
                        ),
                    )

                    conn.commit()

                    app_logger.info(
                        "[GEOFENCE INITIAL STATE] "
                        "device=%s fence=%s state=%s",
                        device_id,
                        fence_name,
                        target_state,
                    )

                    results.append(
                        {
                            "geofence_id": fence_id,
                            "geofence_name": fence_name,
                            "state": target_state,
                            "is_violation": is_violation,
                            "event": None,
                            "is_initial": True,
                        }
                    )

                    continue

                confirmed_state = state_row[
                    "confirmed_state"
                ]

                candidate_state = state_row[
                    "candidate_state"
                ]

                candidate_count = (
                    state_row["candidate_count"] or 0
                )

                last_valid_gps_time = state_row[
                    "last_valid_gps_time"
                ]

                # ----------------------------------------
                # 5. Reject replayed/out-of-order points
                # ----------------------------------------

                last_dt = _parse_utc_dt(
                    last_valid_gps_time
                )

                if last_dt and pt_dt <= last_dt:
                    app_logger.debug(
                        "[GEOFENCE REPLAY] "
                        "device=%s fence=%s",
                        device_id,
                        fence_id,
                    )
                    continue

                # ----------------------------------------
                # 6. No physical state change
                # ----------------------------------------

                if target_state == confirmed_state:
                    cur.execute(
                        """
                        UPDATE device_geofence_states
                        SET candidate_state = NULL,
                            candidate_count = 0,
                            last_valid_gps_time = ?,
                            last_evaluated_at = ?,
                            updated_at = ?
                        WHERE device_id = ?
                          AND geofence_id = ?
                        """,
                        (
                            timestamp,
                            now_iso,
                            now_iso,
                            device_id,
                            fence_id,
                        ),
                    )

                    conn.commit()

                    results.append(
                        {
                            "geofence_id": fence_id,
                            "geofence_name": fence_name,
                            "state": confirmed_state,
                            "is_violation": is_violation,
                            "event": (
                                "GEOFENCE_OUTSIDE"
                                if confirmed_state == "OUTSIDE"
                                else None
                            ),
                            "is_transition": False,
                        }
                    )

                    continue

                # ----------------------------------------
                # 7. Consecutive reading confirmation
                # ----------------------------------------

                if candidate_state == target_state:
                    new_candidate_count = (
                        candidate_count + 1
                    )
                else:
                    new_candidate_count = 1

                if new_candidate_count < threshold:
                    cur.execute(
                        """
                        UPDATE device_geofence_states
                        SET candidate_state = ?,
                            candidate_count = ?,
                            last_valid_gps_time = ?,
                            last_evaluated_at = ?,
                            updated_at = ?
                        WHERE device_id = ?
                          AND geofence_id = ?
                        """,
                        (
                            target_state,
                            new_candidate_count,
                            timestamp,
                            now_iso,
                            now_iso,
                            device_id,
                            fence_id,
                        ),
                    )

                    conn.commit()

                    results.append(
                        {
                            "geofence_id": fence_id,
                            "geofence_name": fence_name,
                            "state": confirmed_state,
                            "candidate_state": target_state,
                            "candidate_count": new_candidate_count,
                            "is_transition": False,
                            "event": None,
                        }
                    )

                    continue

                # ----------------------------------------
                # 8. Confirm physical state transition
                # ----------------------------------------

                old_state = confirmed_state
                new_confirmed_state = target_state

                cur.execute(
                    """
                    UPDATE device_geofence_states
                    SET confirmed_state = ?,
                        candidate_state = NULL,
                        candidate_count = 0,
                        last_valid_gps_time = ?,
                        last_evaluated_at = ?,
                        updated_at = ?
                    WHERE device_id = ?
                      AND geofence_id = ?
                    """,
                    (
                        new_confirmed_state,
                        timestamp,
                        now_iso,
                        now_iso,
                        device_id,
                        fence_id,
                    ),
                )

                # ----------------------------------------
                # 9. Determine physical event type
                # ----------------------------------------

                event_id = (
                    f"geoevent_{uuid.uuid4().hex[:12]}"
                )

                if new_confirmed_state == "OUTSIDE":
                    event_type = "GEOFENCE_EXIT"
                else:
                    event_type = (
                        "GEOFENCE_REENTRY"
                        if old_state == "OUTSIDE"
                        else "GEOFENCE_ENTRY"
                    )

                # ----------------------------------------
                # 10. Save event to database
                # ----------------------------------------

                cur.execute(
                    """
                    INSERT INTO geofence_events (
                        id,
                        device_id,
                        geofence_id,
                        event_type,
                        latitude,
                        longitude,
                        event_time,
                        created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event_id,
                        device_id,
                        fence_id,
                        event_type,
                        lat,
                        lon,
                        timestamp,
                        now_iso,
                    ),
                )

                conn.commit()

                event_doc = {
                    "id": event_id,
                    "device_id": device_id,
                    "geofence_id": fence_id,
                    "geofence_name": fence_name,
                    "event_type": event_type,
                    "latitude": lat,
                    "longitude": lon,
                    "event_time": timestamp,
                    "created_at": now_iso,
                }

                try:
                    sync_event_to_firestore(event_doc)
                except Exception as exc:
                    app_logger.warning(
                        "[GEOFENCE] Firestore sync failed "
                        "event=%s error=%s",
                        event_id,
                        exc,
                    )

                app_logger.warning(
                    "[GEOFENCE TRANSITION] "
                    "device=%s fence=%s %s -> %s event=%s",
                    device_id,
                    fence_name,
                    old_state,
                    new_confirmed_state,
                    event_type,
                )

                # ----------------------------------------
                # 11. Alert policy and severity
                # ----------------------------------------

                is_exit = (
                    event_type == "GEOFENCE_EXIT"
                )

                is_entry = event_type in (
                    "GEOFENCE_ENTRY",
                    "GEOFENCE_REENTRY",
                )

                should_alert = (
                    (
                        is_exit
                        and fence.get("exit_alert", True)
                    )
                    or
                    (
                        is_entry
                        and fence.get("entry_alert", True)
                    )
                )

                # INCLUSION: EXIT is a violation.
                # EXCLUSION: ENTRY is a violation.

                is_breach = (
                    (
                        rule_type == "INCLUSION"
                        and is_exit
                    )
                    or
                    (
                        rule_type == "EXCLUSION"
                        and is_entry
                    )
                )

                severity = (
                    "HIGH"
                    if is_breach
                    else "INFO"
                )

                alert_created = None

                if should_alert:
                    try:
                        alert_created = trigger_geofence_alert(
                            event_id=event_id,
                            device_id=device_id,
                            geofence_id=fence_id,
                            geofence_name=fence_name,
                            event_type=event_type,
                            latitude=lat,
                            longitude=lon,
                            timestamp=timestamp,
                            severity=severity,
                            fence_type=rule_type,
                        )

                        app_logger.info(
                            "[GEOFENCE ALERT] "
                            "device=%s fence=%s "
                            "event=%s severity=%s",
                            device_id,
                            fence_name,
                            event_type,
                            severity,
                        )

                    except Exception:
                        app_logger.exception(
                            "[GEOFENCE ALERT FAILED] "
                            "device=%s fence=%s event=%s",
                            device_id,
                            fence_name,
                            event_type,
                        )

                results.append(
                    {
                        "geofence_id": fence_id,
                        "geofence_name": fence_name,
                        "old_state": old_state,
                        "state": new_confirmed_state,
                        "is_violation": is_violation,
                        "event": event_type,
                        "event_id": event_id,
                        "alert": alert_created,
                        "severity": severity,
                        "is_transition": True,
                    }
                )

        finally:
            conn.close()

        return results


def handle_device_gps_unknown(
    device_id: str,
    reason: str = "SIGNAL_LOSS",
):
    """
    Handle invalid or unavailable GPS.

    Do not create false ENTRY/EXIT events.
    Do not infer movement from missing GPS.
    """

    app_logger.info(
        "[GEOFENCE] GPS unknown "
        "device=%s reason=%s; "
        "no boundary transition generated",
        device_id,
        reason,
    )

