"""
Alert & Tampering Monitoring Service for Huaten C05 GPS Custody Watch
Production-ready authoritative lifecycle owner for security events, tamper detection,
sensor states, location snapshot capturing, and real-time broadcasting.
"""

import asyncio
import json
import math
import os
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List, Set

from .alert_config import (
    ALERT_CONFIG,
    ALERT_SEVERITIES,
    ALERT_TITLES,
    LEGACY_ALERT_TYPE_MAP,
)
from ..core.firebase import get_firestore_client, is_firebase_active
from ..utils.datetime_utils import current_iso_timestamp, parse_to_utc_datetime
from ..utils.logger import app_logger

BASE_DIR = Path(__file__).resolve().parent.parent.parent
LOCAL_DB = BASE_DIR / "custody_watch.db"


class AlertWebSocketManager:
    """Manages active WebSocket connections for live alert broadcasting."""

    def __init__(self):
        self._active_connections: Set[Any] = set()
        self._lock = threading.Lock()
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def set_loop(self, loop: asyncio.AbstractEventLoop):
        self._loop = loop

    def connect(self, websocket: Any):
        with self._lock:
            self._active_connections.add(websocket)

    def disconnect(self, websocket: Any):
        with self._lock:
            self._active_connections.discard(websocket)

    def broadcast(self, message: Dict[str, Any]):
        """Thread-safe broadcast to all connected WebSocket clients."""
        with self._lock:
            connections = list(self._active_connections)

        if not connections:
            return

        payload = json.dumps(message)

        for ws in connections:
            try:
                # If in an event loop
                if self._loop and self._loop.is_running():
                    asyncio.run_coroutine_threadsafe(ws.send_text(payload), self._loop)
                else:
                    try:
                        loop = asyncio.get_event_loop()
                        if loop.is_running():
                            asyncio.run_coroutine_threadsafe(ws.send_text(payload), loop)
                        else:
                            loop.run_until_complete(ws.send_text(payload))
                    except Exception:
                        pass
            except Exception as e:
                app_logger.debug(f"[WS BROADCAST] Failed to send message to client: {e}")
                with self._lock:
                    self._active_connections.discard(ws)


ws_manager = AlertWebSocketManager()


class AlertService:
    """Authoritative service for custody alert detection, lifecycle, and persistence."""

    def __init__(self):
        self._db_lock = threading.RLock()
        self._disconnect_history: Dict[str, List[float]] = {}
        self._disconnect_lock = threading.Lock()
        self._sustained_deviation: Dict[str, Dict[str, Any]] = {}
        self._sustained_separation: Dict[str, Dict[str, Any]] = {}
        self._sustained_stop: Dict[str, Dict[str, Any]] = {}
        self._ensure_tables()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(LOCAL_DB), timeout=30.0)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_tables(self):
        """Ensures SQLite alerts and audit tables exist with all required columns."""
        with self._db_lock:
            conn = self._get_connection()
            cur = conn.cursor()

            cur.execute("""
                CREATE TABLE IF NOT EXISTS alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    alert_id TEXT UNIQUE,
                    device_id TEXT NOT NULL,
                    alert_type TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'ACTIVE',
                    title TEXT NOT NULL,
                    message TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT 'C05_DEVICE',
                    event_time TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    acknowledged_at TEXT,
                    acknowledged_by TEXT,
                    resolved_at TEXT,
                    resolved_by TEXT,
                    latitude REAL,
                    longitude REAL,
                    location_source TEXT NOT NULL DEFAULT 'UNAVAILABLE',
                    location_timestamp TEXT,
                    location_age_seconds INTEGER,
                    location_accuracy_m REAL,
                    location_is_stale INTEGER NOT NULL DEFAULT 1,
                    raw_event_code TEXT,
                    notes TEXT,
                    requires_alarm INTEGER NOT NULL DEFAULT 0,
                    geofence_alert_id TEXT,
                    timestamp TEXT
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS alert_audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    alert_id TEXT NOT NULL,
                    device_id TEXT NOT NULL,
                    action TEXT NOT NULL,
                    performed_by TEXT,
                    timestamp TEXT NOT NULL,
                    details TEXT
                )
            """)

            # Upgrade existing table columns if necessary before creating indexes
            existing_cols = [c[1] for c in cur.execute("PRAGMA table_info(alerts)").fetchall()]
            new_columns = {
                "alert_id": "TEXT",
                "severity": "TEXT",
                "title": "TEXT",
                "source": "TEXT DEFAULT 'C05_DEVICE'",
                "event_time": "TEXT",
                "received_at": "TEXT",
                "acknowledged_at": "TEXT",
                "acknowledged_by": "TEXT",
                "resolved_at": "TEXT",
                "resolved_by": "TEXT",
                "location_source": "TEXT DEFAULT 'UNAVAILABLE'",
                "location_timestamp": "TEXT",
                "location_age_seconds": "INTEGER",
                "location_accuracy_m": "REAL",
                "location_is_stale": "INTEGER DEFAULT 1",
                "raw_event_code": "TEXT",
                "notes": "TEXT",
                "requires_alarm": "INTEGER DEFAULT 0",
                "geofence_alert_id": "TEXT"
            }
            for col, col_def in new_columns.items():
                if col not in existing_cols:
                    try:
                        cur.execute(f"ALTER TABLE alerts ADD COLUMN {col} {col_def}")
                    except Exception:
                        pass

            cur.execute("CREATE INDEX IF NOT EXISTS idx_alerts_device_status ON alerts(device_id, status)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_alerts_alert_id ON alerts(alert_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_alerts_severity ON alerts(severity)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_alerts_geo_id ON alerts(geofence_alert_id)")

            # Upgrade devices table for custody sensor states
            existing_dev_cols = [c[1] for c in cur.execute("PRAGMA table_info(devices)").fetchall()]
            dev_new_cols = {
                "connectivity_status": "TEXT DEFAULT 'ONLINE'",
                "strap_status": "TEXT DEFAULT 'UNKNOWN'",
                "bracelet_status": "TEXT DEFAULT 'UNKNOWN'",
                "skin_contact_status": "TEXT DEFAULT 'UNSUPPORTED'",
                "sensor_states_json": "TEXT",
                "active_alerts_json": "TEXT",
                "active_alert_count": "INTEGER DEFAULT 0",
                "has_critical_alert": "INTEGER DEFAULT 0",
                "last_alert_time": "TEXT",
                "last_alert_type": "TEXT"
            }
            for col, col_def in dev_new_cols.items():
                if col not in existing_dev_cols:
                    try:
                        cur.execute(f"ALTER TABLE devices ADD COLUMN {col} {col_def}")
                    except Exception:
                        pass

            conn.commit()
            conn.close()

    # -------------------------------------------------------------------------
    # Location Snapshot Capturing (Requirement 5)
    # -------------------------------------------------------------------------
    def capture_location_snapshot(
        self,
        device_id: str,
        event_lat: Optional[float] = None,
        event_lon: Optional[float] = None,
        event_gps_time: Optional[str] = None,
        is_valid_gps: bool = False,
        event_time_iso: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Captures location snapshot for the alert following mandatory priority:
        1. Fresh, valid GPS position at event time.
        2. Last known valid GPS position from telemetry history, clearly labeled as historical.
        3. Otherwise 'UNAVAILABLE' (never replace with 0,0 or blindly use device's current location).
        """
        now = datetime.now(timezone.utc)
        event_dt = parse_to_utc_datetime(event_time_iso) or now

        # Check Priority 1: Fresh, valid GPS position in event packet
        if (
            is_valid_gps
            and event_lat is not None
            and event_lon is not None
            and not (event_lat == 0.0 and event_lon == 0.0)
            and abs(event_lat) <= 90.0
            and abs(event_lon) <= 180.0
        ):
            age_sec = 0
            gps_dt = parse_to_utc_datetime(event_gps_time) if event_gps_time else event_dt
            if gps_dt:
                age_sec = int((event_dt - gps_dt).total_seconds())

            if age_sec < -15:
                # Future GPS reading (>15s into future) is untrusted / rejected
                pass
            else:
                display_age_sec = max(0, age_sec)
                is_stale = display_age_sec > ALERT_CONFIG["gps_unavailable_seconds"]
                return {
                    "latitude": round(event_lat, 6),
                    "longitude": round(event_lon, 6),
                    "location_source": "GPS_FRESH" if not is_stale else "HISTORICAL",
                    "location_timestamp": event_gps_time or event_time_iso or now.isoformat(),
                    "location_age_seconds": display_age_sec,
                    "location_accuracy_m": 5.0,
                    "location_is_stale": is_stale
                }

        # Check Priority 2: Last known valid GPS position from telemetry database
        with self._db_lock:
            conn = self._get_connection()
            cur = conn.cursor()
            row = cur.execute("""
                SELECT latitude, longitude, timestamp FROM telemetry
                WHERE device_id = ? AND gps_valid = 1 AND latitude IS NOT NULL AND longitude IS NOT NULL
                  AND NOT (latitude = 0.0 AND longitude = 0.0)
                ORDER BY id DESC LIMIT 1
            """, (device_id,)).fetchone()
            conn.close()

        if row and row["latitude"] is not None and row["longitude"] is not None:
            hist_lat = float(row["latitude"])
            hist_lon = float(row["longitude"])
            hist_time = row["timestamp"]
            age_sec = None
            if hist_time:
                hist_dt = parse_to_utc_datetime(hist_time)
                if hist_dt:
                    age_diff = (event_dt - hist_dt).total_seconds()
                    if age_diff >= -15:
                        age_sec = max(0, int(age_diff))

            display_age = age_sec if age_sec is not None else 120
            is_stale = display_age > ALERT_CONFIG["gps_unavailable_seconds"]
            return {
                "latitude": round(hist_lat, 6),
                "longitude": round(hist_lon, 6),
                "location_source": "HISTORICAL" if is_stale else "GPS_FRESH",
                "location_timestamp": hist_time,
                "location_age_seconds": display_age,
                "location_accuracy_m": 15.0,
                "location_is_stale": is_stale
            }

        # Priority 3: Location Unavailable
        return {
            "latitude": None,
            "longitude": None,
            "location_source": "UNAVAILABLE",
            "location_timestamp": None,
            "location_age_seconds": None,
            "location_accuracy_m": None,
            "location_is_stale": True
        }

    # -------------------------------------------------------------------------
    # Idempotent Alert Triggering & Deduplication
    # -------------------------------------------------------------------------
    def trigger_alert(
        self,
        device_id: str,
        alert_type: str,
        message: Optional[str] = None,
        title: Optional[str] = None,
        severity: Optional[str] = None,
        latitude: Optional[float] = None,
        longitude: Optional[float] = None,
        gps_valid: bool = False,
        gps_timestamp: Optional[str] = None,
        event_time: Optional[str] = None,
        source: str = "C05_DEVICE",
        raw_event_code: Optional[str] = None,
        sensor_states: Optional[Dict[str, Any]] = None,
        requires_alarm: Optional[bool] = None,
        geofence_alert_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Authoritatively creates or deduplicates an active alert.
        Prevents duplicate records while an alert of this type is already ACTIVE or ACKNOWLEDGED.
        """
        canonical_type = LEGACY_ALERT_TYPE_MAP.get(alert_type.upper(), alert_type.upper())
        sev = severity.upper() if severity else ALERT_SEVERITIES.get(canonical_type, "HIGH")
        alert_title = title or ALERT_TITLES.get(canonical_type, f"Alert: {canonical_type}")
        msg = message or f"Security event {canonical_type} reported for device {device_id}."
        now_iso = current_iso_timestamp()
        event_time_iso = event_time or now_iso

        if requires_alarm is not None:
            alarm_flag = bool(requires_alarm)
        else:
            alarm_flag = (sev in ("CRITICAL", "HIGH") or canonical_type in (
                "GEOFENCE_BREACH", "EXCLUSION_ZONE_BREACH", "ROUTE_DEVIATION_ALERT",
                "UNAUTHORIZED_EARLY_DEPARTURE", "JOURNEY_OVERDUE", "SOS_ALERT",
                "STRAP_CUT_ALERT", "TAMPER_ALERT", "BRACELET_REMOVED"
            ))

        # 1. Deduplication check: Is there already an ongoing active/acknowledged alert of this type?
        with self._db_lock:
            conn = self._get_connection()
            cur = conn.cursor()
            existing = cur.execute("""
                SELECT * FROM alerts
                WHERE device_id = ? AND alert_type = ? AND status IN ('ACTIVE', 'ACKNOWLEDGED', 'NEW')
                ORDER BY id DESC LIMIT 1
            """, (device_id, canonical_type)).fetchone()
            conn.close()

        if existing:
            # Active alert already exists; update sensor state on device and return existing
            existing_dict = self._format_alert_row(existing)
            self._update_device_alert_state(device_id, sensor_states=sensor_states)
            return existing_dict

        # 2. Capture location snapshot
        loc_snap = self.capture_location_snapshot(
            device_id=device_id,
            event_lat=latitude,
            event_lon=longitude,
            event_gps_time=gps_timestamp,
            is_valid_gps=gps_valid,
            event_time_iso=event_time_iso
        )

        # 3. Build unique alert record
        alert_unique_id = f"alt_{device_id}_{canonical_type.lower()}_{int(time.time())}_{uuid.uuid4().hex[:6]}"

        alert_record: Dict[str, Any] = {
            "alert_id": alert_unique_id,
            "device_id": device_id,
            "alert_type": canonical_type,
            "severity": sev,
            "status": "ACTIVE",
            "title": alert_title,
            "message": msg,
            "source": source,
            "event_time": event_time_iso,
            "received_at": now_iso,
            "acknowledged_at": None,
            "acknowledged_by": None,
            "resolved_at": None,
            "resolved_by": None,
            "latitude": loc_snap["latitude"],
            "longitude": loc_snap["longitude"],
            "location_source": loc_snap["location_source"],
            "location_timestamp": loc_snap["location_timestamp"],
            "location_age_seconds": loc_snap["location_age_seconds"],
            "location_accuracy_m": loc_snap["location_accuracy_m"],
            "location_is_stale": 1 if loc_snap["location_is_stale"] else 0,
            "raw_event_code": raw_event_code,
            "notes": None,
            "requires_alarm": 1 if alarm_flag else 0,
            "geofence_alert_id": geofence_alert_id,
            "timestamp": event_time_iso
        }

        # 4. Save to SQLite
        with self._db_lock:
            conn = self._get_connection()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO alerts (
                    alert_id, device_id, alert_type, severity, status, title, message, source,
                    event_time, received_at, acknowledged_at, acknowledged_by, resolved_at, resolved_by,
                    latitude, longitude, location_source, location_timestamp, location_age_seconds,
                    location_accuracy_m, location_is_stale, raw_event_code, notes, requires_alarm,
                    geofence_alert_id, timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                alert_record["alert_id"],
                alert_record["device_id"],
                alert_record["alert_type"],
                alert_record["severity"],
                alert_record["status"],
                alert_record["title"],
                alert_record["message"],
                alert_record["source"],
                alert_record["event_time"],
                alert_record["received_at"],
                alert_record["acknowledged_at"],
                alert_record["acknowledged_by"],
                alert_record["resolved_at"],
                alert_record["resolved_by"],
                alert_record["latitude"],
                alert_record["longitude"],
                alert_record["location_source"],
                alert_record["location_timestamp"],
                alert_record["location_age_seconds"],
                alert_record["location_accuracy_m"],
                alert_record["location_is_stale"],
                alert_record["raw_event_code"],
                alert_record["notes"],
                alert_record["requires_alarm"],
                alert_record["geofence_alert_id"],
                alert_record["timestamp"]
            ))
            inserted_id = cur.lastrowid
            conn.commit()
            conn.close()

        alert_record["id"] = inserted_id
        alert_record["created_at"] = alert_record["event_time"]

        # 5. Save to Firestore (Requirement 4)
        if is_firebase_active():
            def _sync_firestore():
                try:
                    db = get_firestore_client()
                    if db:
                        fs_payload = {**alert_record}
                        fs_payload["location_is_stale"] = bool(alert_record["location_is_stale"])
                        fs_payload["requires_alarm"] = bool(alarm_flag)
                        db.collection("alerts").document(alert_unique_id).set(fs_payload, merge=True)
                except Exception as e:
                    app_logger.error(f"[FIRESTORE ALERT SYNC] Failed to push alert {alert_unique_id}: {e}")

            threading.Thread(target=_sync_firestore, daemon=True).start()

        # 6. Update device sensor states and active alerts summary on devices/{device_id}
        self._update_device_alert_state(
            device_id=device_id,
            sensor_states=sensor_states,
            latest_alert=alert_record
        )

        # 7. Audit log
        self._log_audit(alert_unique_id, device_id, "TRIGGERED", source, f"Severity: {sev}")

        # 8. Broadcast to WebSocket clients (Requirement 3 & 7)
        ws_manager.broadcast({
            "type": "ALERT_NEW",
            "alert": self._normalize_api_alert(alert_record)
        })

        app_logger.warning(
            f"[ALERT SERVICE] [{sev}] {canonical_type} for Device {device_id} - "
            f"Loc: {loc_snap['location_source']} ({loc_snap['latitude']},{loc_snap['longitude']})"
        )

        return self._normalize_api_alert(alert_record)

    # -------------------------------------------------------------------------
    # Alert Acknowledgment
    # -------------------------------------------------------------------------
    def acknowledge_alert(
        self,
        alert_identifier: Any,
        acknowledged_by: str = "admin",
        notes: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Acknowledges an alert and silences audible alarm (requires_alarm=0).
        Synchronizes both alerts and geofence_alerts tables and Firestore collections.
        """
        now_iso = current_iso_timestamp()
        target_str = str(alert_identifier).strip()
        geo_id = None
        alert_id_str = None
        dev_id = None
        int_id = None

        with self._db_lock:
            conn = self._get_connection()
            cur = conn.cursor()

            # 1. Search in alerts table by id, alert_id, or geofence_alert_id
            row = None
            if isinstance(alert_identifier, int) or target_str.isdigit():
                row = cur.execute(
                    "SELECT * FROM alerts WHERE id = ? OR alert_id = ? OR geofence_alert_id = ?",
                    (int(target_str), target_str, target_str)
                ).fetchone()
            else:
                row = cur.execute(
                    "SELECT * FROM alerts WHERE alert_id = ? OR geofence_alert_id = ?",
                    (target_str, target_str)
                ).fetchone()

            # 2. If not found in alerts, check in geofence_alerts
            geo_row = None
            if not row:
                try:
                    geo_row = cur.execute(
                        "SELECT * FROM geofence_alerts WHERE id = ? OR core_alert_id = ? OR event_id = ?",
                        (target_str, target_str, target_str)
                    ).fetchone()
                    if geo_row:
                        geo_id = geo_row["id"]
                        dev_id = geo_row["device_id"]
                        core_id = geo_row["core_alert_id"]
                        if core_id:
                            row = cur.execute("SELECT * FROM alerts WHERE alert_id = ?", (core_id,)).fetchone()
                except Exception:
                    pass
            else:
                geo_id = row["geofence_alert_id"]
                if not geo_id:
                    try:
                        gr = cur.execute(
                            "SELECT id FROM geofence_alerts WHERE core_alert_id = ? OR id = ?",
                            (row["alert_id"], target_str)
                        ).fetchone()
                        if gr:
                            geo_id = gr["id"]
                    except Exception:
                        pass

            if not row and not geo_row:
                conn.close()
                return None

            if row:
                alert_id_str = row["alert_id"] or f"alt_{row['id']}"
                int_id = row["id"]
                dev_id = row["device_id"]
                cur.execute("""
                    UPDATE alerts
                    SET status = 'ACKNOWLEDGED',
                        requires_alarm = 0,
                        acknowledged_at = ?,
                        acknowledged_by = ?,
                        notes = COALESCE(?, notes)
                    WHERE id = ?
                """, (now_iso, acknowledged_by, notes, int_id))

            # Synchronize geofence_alerts table
            try:
                cur.execute("""
                    UPDATE geofence_alerts
                    SET status = 'ACKNOWLEDGED',
                        requires_alarm = 0,
                        acknowledged_at = ?,
                        acknowledged_by = ?,
                        updated_at = ?
                    WHERE id = ? OR core_alert_id = ? OR event_id = ? OR (? IS NOT NULL AND id = ?)
                """, (now_iso, acknowledged_by, now_iso, target_str, alert_id_str or target_str, target_str, geo_id, geo_id or ""))
            except Exception:
                pass

            conn.commit()
            updated_row = cur.execute("SELECT * FROM alerts WHERE id = ?", (int_id,)).fetchone() if int_id else None
            conn.close()

        updated_dict = self._format_alert_row(updated_row) if updated_row else (dict(geo_row) if geo_row else {})
        if not alert_id_str:
            alert_id_str = target_str

        # Sync update to Firestore
        if is_firebase_active():
            def _sync_ack():
                try:
                    db = get_firestore_client()
                    if db:
                        db.collection("alerts").document(alert_id_str).set({
                            "status": "ACKNOWLEDGED",
                            "requires_alarm": False,
                            "acknowledged": True,
                            "acknowledged_at": now_iso,
                            "acknowledged_by": acknowledged_by,
                            "notes": notes
                        }, merge=True)
                        if geo_id and geo_id != alert_id_str:
                            db.collection("geofence_alerts").document(geo_id).set({
                                "status": "ACKNOWLEDGED",
                                "requires_alarm": False,
                                "acknowledged": True,
                                "acknowledged_at": now_iso,
                                "acknowledged_by": acknowledged_by,
                                "updated_at": now_iso
                            }, merge=True)
                            db.collection("alerts").document(geo_id).set({
                                "status": "ACKNOWLEDGED",
                                "requires_alarm": False,
                                "acknowledged": True,
                                "acknowledged_at": now_iso,
                                "acknowledged_by": acknowledged_by
                            }, merge=True)
                except Exception as e:
                    app_logger.error(f"[FIRESTORE ACK SYNC] Error: {e}")

            threading.Thread(target=_sync_ack, daemon=True).start()

        if dev_id:
            self._log_audit(alert_id_str, dev_id, "ACKNOWLEDGED", acknowledged_by, notes or "")
            self._update_device_alert_state(dev_id)

        normalized = self._normalize_api_alert(updated_dict)
        normalized["requires_alarm"] = False
        normalized["acknowledged"] = True

        ws_manager.broadcast({
            "type": "ALERT_UPDATE",
            "alert": normalized
        })
        if geo_id or "GEOFENCE" in str(normalized.get("type", "")).upper():
            ws_manager.broadcast({
                "type": "GEOFENCE_ALERT_ACKNOWLEDGED",
                "alert_id": geo_id or alert_id_str,
                "core_alert_id": alert_id_str,
                "device_id": dev_id,
                "status": "ACKNOWLEDGED",
                "requires_alarm": False,
                "acknowledged_by": acknowledged_by,
                "acknowledged_at": now_iso,
                "notes": notes
            })

        return normalized

    # -------------------------------------------------------------------------
    # Alert Silence
    # -------------------------------------------------------------------------
    def silence_alert(
        self,
        alert_identifier: Any,
        silenced_by: str = "admin",
        notes: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Silences audible alarms without removing the alert from incident history.
        Sets requires_alarm=0, status='ACKNOWLEDGED', and broadcasts ALERT_SILENCED.
        Synchronizes both alerts and geofence_alerts tables and Firestore collections.
        """
        now_iso = current_iso_timestamp()
        target_str = str(alert_identifier).strip()
        geo_id = None
        alert_id_str = None
        dev_id = None
        int_id = None
        silence_note = notes or "Silenced by monitoring officer"

        with self._db_lock:
            conn = self._get_connection()
            cur = conn.cursor()

            # 1. Search in alerts table by id, alert_id, or geofence_alert_id
            row = None
            if isinstance(alert_identifier, int) or target_str.isdigit():
                row = cur.execute(
                    "SELECT * FROM alerts WHERE id = ? OR alert_id = ? OR geofence_alert_id = ?",
                    (int(target_str), target_str, target_str)
                ).fetchone()
            else:
                row = cur.execute(
                    "SELECT * FROM alerts WHERE alert_id = ? OR geofence_alert_id = ?",
                    (target_str, target_str)
                ).fetchone()

            # 2. If not found in alerts, check in geofence_alerts
            geo_row = None
            if not row:
                try:
                    geo_row = cur.execute(
                        "SELECT * FROM geofence_alerts WHERE id = ? OR core_alert_id = ? OR event_id = ?",
                        (target_str, target_str, target_str)
                    ).fetchone()
                    if geo_row:
                        geo_id = geo_row["id"]
                        dev_id = geo_row["device_id"]
                        core_id = geo_row["core_alert_id"]
                        if core_id:
                            row = cur.execute("SELECT * FROM alerts WHERE alert_id = ?", (core_id,)).fetchone()
                except Exception:
                    pass
            else:
                geo_id = row["geofence_alert_id"]
                if not geo_id:
                    try:
                        gr = cur.execute(
                            "SELECT id FROM geofence_alerts WHERE core_alert_id = ? OR id = ?",
                            (row["alert_id"], target_str)
                        ).fetchone()
                        if gr:
                            geo_id = gr["id"]
                    except Exception:
                        pass

            if not row and not geo_row:
                conn.close()
                return None

            if row:
                alert_id_str = row["alert_id"] or f"alt_{row['id']}"
                int_id = row["id"]
                dev_id = row["device_id"]
                cur.execute("""
                    UPDATE alerts
                    SET status = 'ACKNOWLEDGED',
                        requires_alarm = 0,
                        acknowledged_at = COALESCE(acknowledged_at, ?),
                        acknowledged_by = COALESCE(acknowledged_by, ?),
                        notes = COALESCE(?, notes)
                    WHERE id = ?
                """, (now_iso, silenced_by, silence_note, int_id))

            try:
                cur.execute("""
                    UPDATE geofence_alerts
                    SET status = 'ACKNOWLEDGED',
                        requires_alarm = 0,
                        acknowledged_at = COALESCE(acknowledged_at, ?),
                        acknowledged_by = COALESCE(acknowledged_by, ?),
                        updated_at = ?
                    WHERE id = ? OR core_alert_id = ? OR event_id = ? OR (? IS NOT NULL AND id = ?)
                """, (now_iso, silenced_by, now_iso, target_str, alert_id_str or target_str, target_str, geo_id, geo_id or ""))
            except Exception:
                pass

            conn.commit()
            updated_row = cur.execute("SELECT * FROM alerts WHERE id = ?", (int_id,)).fetchone() if int_id else None
            conn.close()

        updated_dict = self._format_alert_row(updated_row) if updated_row else (dict(geo_row) if geo_row else {})
        if not alert_id_str:
            alert_id_str = target_str

        if is_firebase_active():
            def _sync_silence():
                try:
                    db = get_firestore_client()
                    if db:
                        db.collection("alerts").document(alert_id_str).set({
                            "status": "ACKNOWLEDGED",
                            "requires_alarm": False,
                            "acknowledged": True,
                            "acknowledged_at": now_iso,
                            "acknowledged_by": silenced_by,
                            "notes": silence_note
                        }, merge=True)
                        if geo_id and geo_id != alert_id_str:
                            db.collection("geofence_alerts").document(geo_id).set({
                                "status": "ACKNOWLEDGED",
                                "requires_alarm": False,
                                "acknowledged": True,
                                "acknowledged_at": now_iso,
                                "acknowledged_by": silenced_by,
                                "updated_at": now_iso
                            }, merge=True)
                            db.collection("alerts").document(geo_id).set({
                                "status": "ACKNOWLEDGED",
                                "requires_alarm": False,
                                "acknowledged": True,
                                "acknowledged_at": now_iso,
                                "acknowledged_by": silenced_by
                            }, merge=True)
                except Exception as e:
                    app_logger.error(f"[FIRESTORE SILENCE SYNC] Error: {e}")

            threading.Thread(target=_sync_silence, daemon=True).start()

        if dev_id:
            self._log_audit(alert_id_str, dev_id, "SILENCED", silenced_by, silence_note)
            self._update_device_alert_state(dev_id)

        normalized = self._normalize_api_alert(updated_dict)
        normalized["requires_alarm"] = False
        normalized["acknowledged"] = True

        ws_manager.broadcast({
            "type": "ALERT_SILENCED",
            "alert_id": alert_id_str,
            "device_id": dev_id,
            "requires_alarm": False,
            "status": "ACKNOWLEDGED",
            "silenced_by": silenced_by,
            "silenced_at": now_iso,
            "alert": normalized
        })
        ws_manager.broadcast({
            "type": "ALERT_UPDATE",
            "alert": normalized
        })
        if geo_id or "GEOFENCE" in str(normalized.get("type", "")).upper():
            ws_manager.broadcast({
                "type": "GEOFENCE_ALERT_ACKNOWLEDGED",
                "alert_id": geo_id or alert_id_str,
                "core_alert_id": alert_id_str,
                "device_id": dev_id,
                "status": "ACKNOWLEDGED",
                "requires_alarm": False,
                "acknowledged_by": silenced_by,
                "acknowledged_at": now_iso,
                "notes": silence_note
            })

        return normalized

    # -------------------------------------------------------------------------
    # Alert Resolution
    # -------------------------------------------------------------------------
    def resolve_alert(
        self,
        alert_identifier: Any,
        resolved_by: str = "admin",
        resolution_notes: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Marks an alert as RESOLVED and recalculates device active alert state.
        Synchronizes both alerts and geofence_alerts tables and Firestore collections.
        """
        now_iso = current_iso_timestamp()
        target_str = str(alert_identifier).strip()
        geo_id = None
        alert_id_str = None
        dev_id = None
        int_id = None

        with self._db_lock:
            conn = self._get_connection()
            cur = conn.cursor()

            row = None
            if isinstance(alert_identifier, int) or target_str.isdigit():
                row = cur.execute(
                    "SELECT * FROM alerts WHERE id = ? OR alert_id = ? OR geofence_alert_id = ?",
                    (int(target_str), target_str, target_str)
                ).fetchone()
            else:
                row = cur.execute(
                    "SELECT * FROM alerts WHERE alert_id = ? OR geofence_alert_id = ?",
                    (target_str, target_str)
                ).fetchone()

            geo_row = None
            if not row:
                try:
                    geo_row = cur.execute(
                        "SELECT * FROM geofence_alerts WHERE id = ? OR core_alert_id = ? OR event_id = ?",
                        (target_str, target_str, target_str)
                    ).fetchone()
                    if geo_row:
                        geo_id = geo_row["id"]
                        dev_id = geo_row["device_id"]
                        core_id = geo_row["core_alert_id"]
                        if core_id:
                            row = cur.execute("SELECT * FROM alerts WHERE alert_id = ?", (core_id,)).fetchone()
                except Exception:
                    pass
            else:
                geo_id = row["geofence_alert_id"]
                if not geo_id:
                    try:
                        gr = cur.execute(
                            "SELECT id FROM geofence_alerts WHERE core_alert_id = ? OR id = ?",
                            (row["alert_id"], target_str)
                        ).fetchone()
                        if gr:
                            geo_id = gr["id"]
                    except Exception:
                        pass

            if not row and not geo_row:
                conn.close()
                return None

            if row:
                alert_id_str = row["alert_id"] or f"alt_{row['id']}"
                int_id = row["id"]
                dev_id = row["device_id"]
                cur.execute("""
                    UPDATE alerts
                    SET status = 'RESOLVED',
                        requires_alarm = 0,
                        resolved_at = ?,
                        resolved_by = ?,
                        notes = COALESCE(?, notes)
                    WHERE id = ?
                """, (now_iso, resolved_by, resolution_notes or "Resolved", int_id))

            try:
                cur.execute("""
                    UPDATE geofence_alerts
                    SET status = 'RESOLVED',
                        requires_alarm = 0,
                        resolved_at = ?,
                        resolved_by = ?,
                        updated_at = ?
                    WHERE id = ? OR core_alert_id = ? OR event_id = ? OR (? IS NOT NULL AND id = ?)
                """, (now_iso, resolved_by, now_iso, target_str, alert_id_str or target_str, target_str, geo_id, geo_id or ""))
            except Exception:
                pass

            conn.commit()
            updated_row = cur.execute("SELECT * FROM alerts WHERE id = ?", (int_id,)).fetchone() if int_id else None
            conn.close()

        updated_dict = self._format_alert_row(updated_row) if updated_row else (dict(geo_row) if geo_row else {})
        if not alert_id_str:
            alert_id_str = target_str

        # Update Firestore alert record
        if is_firebase_active():
            def _sync_res():
                try:
                    db = get_firestore_client()
                    if db:
                        db.collection("alerts").document(alert_id_str).set({
                            "status": "RESOLVED",
                            "requires_alarm": False,
                            "resolved_at": now_iso,
                            "resolved_by": resolved_by,
                            "notes": resolution_notes
                        }, merge=True)
                        if geo_id and geo_id != alert_id_str:
                            db.collection("geofence_alerts").document(geo_id).set({
                                "status": "RESOLVED",
                                "requires_alarm": False,
                                "resolved_at": now_iso,
                                "resolved_by": resolved_by,
                                "updated_at": now_iso
                            }, merge=True)
                            db.collection("alerts").document(geo_id).set({
                                "status": "RESOLVED",
                                "requires_alarm": False,
                                "resolved_at": now_iso,
                                "resolved_by": resolved_by
                            }, merge=True)
                except Exception as e:
                    app_logger.error(f"[FIRESTORE RESOLVE SYNC] Error: {e}")

            threading.Thread(target=_sync_res, daemon=True).start()

        if dev_id:
            self._update_device_alert_state(dev_id)
            self._log_audit(alert_id_str, dev_id, "RESOLVED", resolved_by, resolution_notes or "")

        normalized = self._normalize_api_alert(updated_dict)
        normalized["requires_alarm"] = False

        ws_manager.broadcast({
            "type": "ALERT_UPDATE",
            "alert": normalized
        })
        ws_manager.broadcast({
            "type": "ALERT_RESOLVED",
            "alert_id": alert_id_str,
            "device_id": dev_id,
            "status": "RESOLVED",
            "requires_alarm": False
        })
        if geo_id or "GEOFENCE" in str(normalized.get("type", "")).upper():
            ws_manager.broadcast({
                "type": "GEOFENCE_ALERT_RESOLVED",
                "alert_id": geo_id or alert_id_str,
                "core_alert_id": alert_id_str,
                "device_id": dev_id,
                "geofence_id": normalized.get("geofence_id"),
                "status": "RESOLVED",
                "requires_alarm": False,
                "resolved_by": resolved_by,
                "resolved_at": now_iso
            })

        return normalized

    # -------------------------------------------------------------------------
    # Condition Cleared -> Auto Resolution
    # -------------------------------------------------------------------------
    def resolve_active_alerts_by_type(
        self,
        device_id: str,
        alert_types: List[str],
        resolved_by: str = "SYSTEM_SENSOR_RESTORED",
        notes: str = "Underlying hardware condition cleared"
    ) -> List[Dict[str, Any]]:
        """
        Auto-resolves active alerts of specified types when condition clears.
        """
        resolved_list = []
        with self._db_lock:
            conn = self._get_connection()
            cur = conn.cursor()
            placeholders = ",".join("?" for _ in alert_types)
            rows = cur.execute(f"""
                SELECT id FROM alerts
                WHERE device_id = ? AND alert_type IN ({placeholders}) AND status IN ('ACTIVE', 'ACKNOWLEDGED', 'NEW')
            """, [device_id, *alert_types]).fetchall()
            ids = [r["id"] for r in rows]
            conn.close()

        for alert_id in ids:
            res = self.resolve_alert(alert_id, resolved_by=resolved_by, resolution_notes=notes)
            if res:
                resolved_list.append(res)

        return resolved_list

    # -------------------------------------------------------------------------
    # Device State & Firestore Synchronization
    # -------------------------------------------------------------------------
    def _update_device_alert_state(
        self,
        device_id: str,
        sensor_states: Optional[Dict[str, Any]] = None,
        latest_alert: Optional[Dict[str, Any]] = None
    ):
        """
        Calculates and synchronizes device connectivity, strap, bracelet, and active-alert states.
        """
        with self._db_lock:
            conn = self._get_connection()
            cur = conn.cursor()

            # Query all currently active alerts for this device
            active_rows = cur.execute("""
                SELECT alert_type, severity, title, event_time FROM alerts
                WHERE device_id = ? AND status IN ('ACTIVE', 'ACKNOWLEDGED', 'NEW')
                ORDER BY id DESC
            """, (device_id,)).fetchall()

            active_alerts = [dict(r) for r in active_rows]
            active_count = len(active_alerts)
            has_critical = any(a.get("severity") == "CRITICAL" for a in active_alerts)

            # Determine sensor states based on active alerts and explicit sensor input
            strap_status = "SECURED"
            bracelet_status = "CONNECTED"
            skin_contact = "DETECTED"
            connectivity = "ONLINE"

            active_types = {a["alert_type"] for a in active_alerts}

            if "STRAP_CUT_ALERT" in active_types:
                strap_status = "CUT"
            elif "STRAP_OPENED" in active_types:
                strap_status = "OPENED"
            elif not active_alerts and sensor_states and "strap_status" in sensor_states:
                strap_status = sensor_states["strap_status"]

            if "BRACELET_REMOVED" in active_types:
                bracelet_status = "REMOVED"
            elif "BRACELET_DISCONNECTED" in active_types:
                bracelet_status = "DISCONNECTED"
            elif sensor_states and "bracelet_status" in sensor_states:
                bracelet_status = sensor_states["bracelet_status"]

            if "SKIN_CONTACT_LOST" in active_types:
                skin_contact = "LOST"
            elif sensor_states and "skin_contact_status" in sensor_states:
                skin_contact = sensor_states["skin_contact_status"]
            else:
                skin_contact = "UNSUPPORTED"

            if "WATCH_OFFLINE" in active_types or "DEVICE_COMMUNICATION_LOST" in active_types:
                connectivity = "OFFLINE"

            active_json = json.dumps(active_alerts)
            sensors_dict = {
                "strap": strap_status,
                "bracelet": bracelet_status,
                "skin_contact": skin_contact,
                "connectivity": connectivity,
                "last_evaluated": current_iso_timestamp()
            }
            if sensor_states:
                sensors_dict.update(sensor_states)
            sensors_json = json.dumps(sensors_dict)

            last_alert_time = latest_alert.get("event_time") if latest_alert else None
            last_alert_type = latest_alert.get("alert_type") if latest_alert else None

            cur.execute("""
                UPDATE devices
                SET connectivity_status = ?,
                    strap_status = ?,
                    bracelet_status = ?,
                    skin_contact_status = ?,
                    sensor_states_json = ?,
                    active_alerts_json = ?,
                    active_alert_count = ?,
                    has_critical_alert = ?,
                    last_alert_time = COALESCE(?, last_alert_time),
                    last_alert_type = COALESCE(?, last_alert_type)
                WHERE device_id = ?
            """, (
                connectivity,
                strap_status,
                bracelet_status,
                skin_contact,
                sensors_json,
                active_json,
                active_count,
                1 if has_critical else 0,
                last_alert_time,
                last_alert_type,
                device_id
            ))
            conn.commit()
            conn.close()

        # Update Firestore devices/{device_id}
        if is_firebase_active():
            def _sync_device_state():
                try:
                    db = get_firestore_client()
                    if db:
                        db.collection("devices").document(device_id).set({
                            "connectivity_status": connectivity,
                            "strap_status": strap_status,
                            "bracelet_status": bracelet_status,
                            "skin_contact_status": skin_contact,
                            "sensor_states": sensors_dict,
                            "active_alerts": active_alerts,
                            "active_alert_count": active_count,
                            "has_critical_alert": has_critical,
                            "last_alert_time": last_alert_time,
                            "last_alert_type": last_alert_type,
                            "updated_at": current_iso_timestamp()
                        }, merge=True)
                except Exception as e:
                    app_logger.error(f"[FIRESTORE DEVICE ALERT SYNC] Error: {e}")

            threading.Thread(target=_sync_device_state, daemon=True).start()

    # -------------------------------------------------------------------------
    # Query & Retrieval
    # -------------------------------------------------------------------------
    def list_alerts(
        self,
        status: Optional[str] = None,
        severity: Optional[str] = None,
        alert_type: Optional[str] = None,
        device_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        with self._db_lock:
            conn = self._get_connection()
            cur = conn.cursor()

            query = "SELECT * FROM alerts WHERE 1=1"
            params: List[Any] = []

            if status:
                st = status.upper()
                if st in ("ACTIVE", "UNRESOLVED"):
                    query += " AND status IN ('ACTIVE', 'ACKNOWLEDGED', 'NEW')"
                else:
                    query += " AND status = ?"
                    params.append(st)

            if severity:
                query += " AND severity = ?"
                params.append(severity.upper())

            if alert_type:
                canonical = LEGACY_ALERT_TYPE_MAP.get(alert_type.upper(), alert_type.upper())
                query += " AND alert_type = ?"
                params.append(canonical)

            if device_id:
                query += " AND device_id = ?"
                params.append(device_id)

            if start_date:
                query += " AND event_time >= ?"
                params.append(start_date)

            if end_date:
                query += " AND event_time <= ?"
                params.append(end_date)

            query += " ORDER BY id DESC LIMIT ? OFFSET ?"
            params.extend([limit, offset])

            rows = cur.execute(query, params).fetchall()
            conn.close()

        results = []
        seen_keys = set()
        for r in rows:
            formatted = self._normalize_api_alert(self._format_alert_row(r))
            aid = str(formatted.get("alert_id") or formatted.get("id") or "")
            dev = str(formatted.get("device_id") or "")
            atype = str(formatted.get("alert_type") or "")
            ts = str(formatted.get("timestamp") or formatted.get("event_time") or "")
            sig = aid or f"{dev}_{atype}_{ts}"
            if sig in seen_keys:
                continue
            seen_keys.add(sig)
            results.append(formatted)
        return results

    def get_recent_alerts(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Backwards-compatible helper returning most recent alerts."""
        return self.list_alerts(limit=limit)

    def get_device_alerts(self, device_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Returns alerts for a specific device."""
        return self.list_alerts(device_id=device_id, limit=limit)

    def get_alert_by_id(self, alert_identifier: Any) -> Optional[Dict[str, Any]]:
        target_str = str(alert_identifier).strip()
        with self._db_lock:
            conn = self._get_connection()
            cur = conn.cursor()
            row = None
            if isinstance(alert_identifier, int) or target_str.isdigit():
                row = cur.execute(
                    "SELECT * FROM alerts WHERE id = ? OR alert_id = ? OR geofence_alert_id = ?",
                    (int(target_str), target_str, target_str)
                ).fetchone()
            else:
                row = cur.execute(
                    "SELECT * FROM alerts WHERE alert_id = ? OR geofence_alert_id = ?",
                    (target_str, target_str)
                ).fetchone()

            # Fallback to geofence_alerts if not found in alerts
            if not row:
                try:
                    geo_row = cur.execute(
                        "SELECT * FROM geofence_alerts WHERE id = ? OR core_alert_id = ? OR event_id = ?",
                        (target_str, target_str, target_str)
                    ).fetchone()
                    if geo_row:
                        core_id = geo_row["core_alert_id"]
                        if core_id:
                            row = cur.execute("SELECT * FROM alerts WHERE alert_id = ?", (core_id,)).fetchone()
                        if not row:
                            conn.close()
                            d = dict(geo_row)
                            d["requires_alarm"] = bool(d.get("requires_alarm")) if d.get("status") not in ("ACKNOWLEDGED", "SILENCED", "RESOLVED", "CLOSED") else False
                            return self._normalize_api_alert(d)
                except Exception:
                    pass
            conn.close()

        if not row:
            return None
        return self._normalize_api_alert(self._format_alert_row(row))

    def get_device_alert_status(self, device_id: str) -> Dict[str, Any]:
        """Returns comprehensive real-time alert, sensor, and connectivity status for a device."""
        active_alerts = self.list_alerts(device_id=device_id, status="ACTIVE", limit=50)
        recent_alerts = self.list_alerts(device_id=device_id, limit=10)

        # Retrieve device record
        with self._db_lock:
            conn = self._get_connection()
            cur = conn.cursor()
            dev_row = cur.execute("SELECT * FROM devices WHERE device_id = ?", (device_id,)).fetchone()
            conn.close()

        dev = dict(dev_row) if dev_row else {}

        # Parse sensor states
        sensors = {}
        if dev.get("sensor_states_json"):
            try:
                sensors = json.loads(dev["sensor_states_json"])
            except Exception:
                pass

        # Most recent valid location snapshot
        loc_snap = self.capture_location_snapshot(device_id=device_id)
        has_alarm = any(bool(a.get("requires_alarm")) for a in active_alerts)

        return {
            "device_id": device_id,
            "connectivity_status": dev.get("connectivity_status", "ONLINE" if dev.get("status") == "ONLINE" else "UNKNOWN"),
            "strap_status": dev.get("strap_status", "UNKNOWN"),
            "bracelet_status": dev.get("bracelet_status", "UNKNOWN"),
            "skin_contact_status": dev.get("skin_contact_status", "UNSUPPORTED"),
            "sensor_states": sensors or {
                "strap": dev.get("strap_status", "UNKNOWN"),
                "bracelet": dev.get("bracelet_status", "UNKNOWN"),
                "skin_contact": dev.get("skin_contact_status", "UNSUPPORTED")
            },
            "active_alert_count": len(active_alerts),
            "has_critical_alert": any(a.get("severity") == "CRITICAL" for a in active_alerts),
            "requires_alarm": has_alarm,
            "active_alerts": active_alerts,
            "recent_alerts": recent_alerts,
            "last_known_location": loc_snap,
            "current_live_location": {
                "latitude": dev.get("last_latitude"),
                "longitude": dev.get("last_longitude"),
                "timestamp": dev.get("last_valid_gps_time"),
                "gps_valid": bool(dev.get("gps_valid"))
            }
        }

    # -------------------------------------------------------------------------
    # Anomaly Detection & Background Evaluation
    # -------------------------------------------------------------------------
    def check_battery_alert(self, device_id: str, battery_percent: int):
        """Checks and triggers or resolves LOW_BATTERY_ALERT."""
        thresh = ALERT_CONFIG["low_battery_percent"]
        if 0 < battery_percent <= thresh:
            self.trigger_alert(
                device_id=device_id,
                alert_type="LOW_BATTERY_ALERT",
                severity="MEDIUM",
                message=f"Battery level critically low at {battery_percent}%. Immediate recharge required."
            )
        elif battery_percent > thresh + 5:
            # Recharging above threshold clears low battery alert
            self.resolve_active_alerts_by_type(
                device_id=device_id,
                alert_types=["LOW_BATTERY_ALERT"],
                resolved_by="SYSTEM_BATTERY_RECHARGED",
                notes=f"Battery recovered to {battery_percent}%."
            )

    def record_disconnection_event(self, device_id: str):
        """Records TCP/UDP disconnect and checks repeated disconnection window."""
        now = time.time()
        window = ALERT_CONFIG["repeated_disconnect_window_seconds"]
        limit = ALERT_CONFIG["repeated_disconnect_count"]

        with self._disconnect_lock:
            history = self._disconnect_history.setdefault(device_id, [])
            history.append(now)
            # Prune events outside window
            self._disconnect_history[device_id] = [t for t in history if now - t <= window]
            recent_count = len(self._disconnect_history[device_id])

        if recent_count >= limit:
            self.trigger_alert(
                device_id=device_id,
                alert_type="REPEATED_DISCONNECTION",
                severity="HIGH",
                message=f"Device experienced {recent_count} disconnections within {int(window / 60)} minutes."
            )

    def check_vehicle_separation(
        self,
        device_id: str,
        watch_lat: float,
        watch_lon: float,
        vehicle_lat: float,
        vehicle_lon: float,
        separation_threshold_m: Optional[float] = None
    ):
        """
        Calculates separation between custody watch and assigned vehicle using Haversine formula.
        Applies sustained duration rule to prevent false positives from brief GPS drift.
        """
        thresh = separation_threshold_m or ALERT_CONFIG["vehicle_separation_meters"]
        dist_m = self._haversine_distance_meters(watch_lat, watch_lon, vehicle_lat, vehicle_lon)
        now = time.time()

        tracker = self._sustained_separation.setdefault(device_id, {"first_seen": None, "count": 0})

        if dist_m > thresh:
            if tracker["first_seen"] is None:
                tracker["first_seen"] = now
                tracker["count"] = 1
            else:
                tracker["count"] += 1

            # Require sustained separation for at least 30 seconds and 2+ readings
            if (now - tracker["first_seen"]) >= 30 and tracker["count"] >= 2:
                self.trigger_alert(
                    device_id=device_id,
                    alert_type="VEHICLE_SEPARATION_ALERT",
                    severity="HIGH",
                    message=f"Watch separated from assigned vehicle by {dist_m:.1f} meters (threshold: {thresh}m).",
                    latitude=watch_lat,
                    longitude=watch_lon,
                    gps_valid=True
                )
        else:
            # Separation cleared
            tracker["first_seen"] = None
            tracker["count"] = 0
            self.resolve_active_alerts_by_type(
                device_id=device_id,
                alert_types=["VEHICLE_SEPARATION_ALERT"],
                resolved_by="SYSTEM_REUNITED",
                notes=f"Watch rejoined vehicle within {dist_m:.1f} meters."
            )

    def check_unscheduled_stop(
        self,
        device_id: str,
        speed_kmh: float,
        current_lat: float,
        current_lon: float,
        approved_stops: Optional[List[Dict[str, Any]]] = None
    ):
        """Checks whether vehicle has stopped outside approved schedule for >= 5 minutes."""
        now = time.time()
        tracker = self._sustained_stop.setdefault(device_id, {"stop_start": None, "lat": current_lat, "lon": current_lon})
        max_stop_sec = ALERT_CONFIG["unscheduled_stop_minutes"] * 60

        if speed_kmh < 1.5:
            if tracker["stop_start"] is None:
                tracker["stop_start"] = now
                tracker["lat"] = current_lat
                tracker["lon"] = current_lon
            else:
                duration = now - tracker["stop_start"]
                if duration >= max_stop_sec:
                    # Check if stop is near approved stop
                    is_approved = False
                    if approved_stops:
                        for s in approved_stops:
                            if self._haversine_distance_meters(current_lat, current_lon, s["lat"], s["lon"]) < 100:
                                is_approved = True
                                break
                    if not is_approved:
                        self.trigger_alert(
                            device_id=device_id,
                            alert_type="UNSCHEDULED_STOP_ALERT",
                            severity="MEDIUM",
                            message=f"Vehicle has been stationary for {int(duration / 60)} minutes outside approved stops.",
                            latitude=current_lat,
                            longitude=current_lon,
                            gps_valid=True
                        )
        else:
            tracker["stop_start"] = None
            self.resolve_active_alerts_by_type(
                device_id=device_id,
                alert_types=["UNSCHEDULED_STOP_ALERT"],
                resolved_by="SYSTEM_MOVEMENT_RESUMED",
                notes="Vehicle resumed travel."
            )

    # -------------------------------------------------------------------------
    # Helper Utilities
    # -------------------------------------------------------------------------
    @staticmethod
    def _haversine_distance_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        R = 6371000.0  # Earth radius in meters
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = (
            math.sin(dlat / 2.0) ** 2
            + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2.0) ** 2
        )
        c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
        return R * c

    def _format_alert_row(self, row: sqlite3.Row) -> Dict[str, Any]:
        d = dict(row)
        d["location_is_stale"] = bool(d.get("location_is_stale", 1))
        d["requires_alarm"] = bool(d.get("requires_alarm", 0))
        return d

    def _normalize_api_alert(self, alert_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Provides full compatibility with frontend ApiAlert and backend OpenAPI schema."""
        d = dict(alert_dict)
        d.setdefault("id", d.get("id") or d.get("alert_id"))
        d.setdefault("imei", d.get("device_id"))
        d.setdefault("type", d.get("alert_type"))
        d.setdefault("created_at", d.get("event_time") or d.get("timestamp"))
        d.setdefault("date", d.get("event_time") or d.get("timestamp"))
        sev = str(d.get("severity", "HIGH")).upper()
        d["severity"] = sev
        st = str(d.get("status", "ACTIVE")).upper()
        d["status"] = st
        d.setdefault("location_time", d.get("location_timestamp"))
        d["location_is_stale"] = bool(d.get("location_is_stale"))
        # The alerts table has no gps_valid column. Route-monitor alerts are only raised
        # from verified fixes and always store that fix's own coordinates, so the flag is
        # provable for them; other sources may carry a last-known telemetry position.
        if "gps_valid" not in d:
            d["gps_valid"] = bool(
                str(d.get("source") or "").upper() == "ROUTE_MONITOR"
                and str(d.get("location_source") or "").upper() in ("GPS_FRESH", "HISTORICAL")
                and d.get("latitude") is not None
                and d.get("longitude") is not None
            )

        if st in ("ACKNOWLEDGED", "SILENCED", "RESOLVED", "CLOSED"):
            d["requires_alarm"] = False
            d["acknowledged"] = True
        else:
            if "requires_alarm" in d and d["requires_alarm"] is not None:
                d["requires_alarm"] = bool(d["requires_alarm"])
            else:
                d["requires_alarm"] = (sev in ("CRITICAL", "HIGH"))
            d["acknowledged"] = False

        return d

    def _log_audit(self, alert_id: str, device_id: str, action: str, performed_by: str, details: str):
        try:
            with self._db_lock:
                conn = self._get_connection()
                cur = conn.cursor()
                cur.execute("""
                    INSERT INTO alert_audit_log (alert_id, device_id, action, performed_by, timestamp, details)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (alert_id, device_id, action, performed_by, current_iso_timestamp(), details))
                conn.commit()
                conn.close()
        except Exception as e:
            app_logger.debug(f"[AUDIT LOG] Error: {e}")


alert_service = AlertService()
