"""
Firestore & Local Persistence Service
Provides abstraction for Firestore with transparent SQLite fallback when offline/unconfigured.
Uses real device telemetry without mock defaults.
"""

import hashlib
import json
import math
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from google.cloud import firestore as cloud_firestore
from ..core.firebase import get_firestore_client, is_firebase_active
from ..utils.logger import app_logger
from ..utils.datetime_utils import current_iso_timestamp

BASE_DIR = Path(__file__).resolve().parent.parent.parent
LOCAL_DB = BASE_DIR / "custody_watch.db"
TRUSTED_GPS_SOURCES = frozenset({
    "watch_frame",
    "watch_tcp",
    "watch_udp",
    "gps_ingest_api"
})
LOG_LOCATION_TYPES = frozenset({
    "location",
    "location_on_demand",
    "sos_alarm"
})
GPS_FIX_MAX_AGE_SECONDS = 900
MAX_FIRESTORE_TELEMETRY_OUTBOX_RECORDS = 50000
FIRESTORE_OUTBOX_BATCH_SIZE = 4
FIRESTORE_RETRY_BASE_SECONDS = 2
FIRESTORE_RETRY_MAX_SECONDS = 300


def _first_present(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _battery_fields(value: Any) -> Dict[str, Any]:
    return {
        "battery": value,
        "batteryLevel": value,
        "battery_level": value,
        "battery_pct": value,
        "battery_percent": value,
        "battery_percentage": value,
        "charge": value,
    }


def fix_event_timestamp(fix: Dict[str, Any]) -> Any:
    """Prefer gps_time, then other known fix-time fields from writer or backend docs."""
    return _first_present(
        fix.get("gps_time"),
        fix.get("timestamp"),
        fix.get("at"),
        fix.get("location_time"),
        fix.get("last_valid_gps_time"),
        fix.get("received_at"),
    )


def _json_default(value: Any):
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError("Unsupported Firestore outbox value: {}".format(type(value).__name__))


def parse_iso_timestamp(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        parsed = value
    elif value is not None:
        try:
            parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
    else:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def normalize_iso_timestamp(value: Any) -> Optional[str]:
    parsed = parse_iso_timestamp(value)
    return parsed.isoformat() if parsed else None


def is_newer_timestamp(incoming: Any, existing: Any, allow_equal: bool = False) -> bool:
    incoming_dt = parse_iso_timestamp(incoming)
    if incoming_dt is None:
        return False
    existing_dt = parse_iso_timestamp(existing)
    if existing_dt is None:
        return True
    return incoming_dt >= existing_dt if allow_equal else incoming_dt > existing_dt


def valid_coordinate_pair(latitude: Any, longitude: Any) -> Optional[Tuple[float, float]]:
    try:
        lat = float(latitude)
        lon = float(longitude)
    except (TypeError, ValueError, OverflowError):
        return None
    if (
        not math.isfinite(lat)
        or not math.isfinite(lon)
        or not -90.0 <= lat <= 90.0
        or not -180.0 <= lon <= 180.0
        or (lat == 0.0 and lon == 0.0)
    ):
        return None
    return lat, lon


def is_gps_valid_flag(value: Any) -> bool:
    return value is True or (type(value) is int and value == 1)


def normalize_verified_gps_fix(
    gps_valid: Any,
    latitude: Any,
    longitude: Any,
    timestamp: Any,
    gps_source: Any,
    *,
    max_age_seconds: Optional[int] = GPS_FIX_MAX_AGE_SECONDS
) -> Optional[Tuple[float, float, str, str]]:
    source = str(gps_source or "").strip().lower()
    position = valid_coordinate_pair(latitude, longitude)
    normalized_time = normalize_iso_timestamp(timestamp)
    fix_time = parse_iso_timestamp(normalized_time)
    if (
        not is_gps_valid_flag(gps_valid)
        or source not in TRUSTED_GPS_SOURCES
        or position is None
        or fix_time is None
    ):
        return None

    age_seconds = (datetime.now(timezone.utc) - fix_time).total_seconds()
    if age_seconds < 0 or (
        max_age_seconds is not None and age_seconds > max_age_seconds
    ):
        return None

    return round(position[0], 6), round(position[1], 6), normalized_time, source


class FirestoreService:
    DEVICE_CACHE_TTL_SECONDS = 10
    DEVICES_CACHE_TTL_SECONDS = 30
    FIRESTORE_READ_RETRY_BASE_SECONDS = 5
    FIRESTORE_READ_RETRY_MAX_SECONDS = 300

    def __init__(self):
        self._firestore_history_sync_lock = threading.Lock()
        self._device_cache_lock = threading.RLock()
        self._device_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
        self._devices_cache: Optional[Tuple[float, List[Dict[str, Any]]]] = None
        self._device_firestore_retry_at: Dict[str, float] = {}
        self._device_firestore_failures: Dict[str, int] = {}
        self._devices_firestore_retry_at = 0.0
        self._devices_firestore_failures = 0
        self._history_firestore_retry_at: Dict[str, float] = {}
        self._history_firestore_failures: Dict[str, int] = {}
        self._sync_worker_lock = threading.Lock()
        self._sync_worker_stop = threading.Event()
        self._sync_worker_thread: Optional[threading.Thread] = None
        self._init_local_db()

    def _cached_device(
        self, device_id: str, allow_expired: bool = False
    ) -> Optional[Dict[str, Any]]:
        with self._device_cache_lock:
            cached = self._device_cache.get(device_id)
        if cached is None:
            return None
        if (
            allow_expired
            or time.monotonic() - cached[0] < self.DEVICE_CACHE_TTL_SECONDS
        ):
            return dict(cached[1])
        return None

    def _cache_device(self, device_id: str, device: Dict[str, Any]) -> None:
        with self._device_cache_lock:
            self._device_cache[device_id] = (time.monotonic(), dict(device))

    def _record_device_read_failure(self, device_id: str):
        failures = self._device_firestore_failures.get(device_id, 0) + 1
        self._device_firestore_failures[device_id] = failures
        delay = min(
            self.FIRESTORE_READ_RETRY_BASE_SECONDS * (2 ** min(failures - 1, 6)),
            self.FIRESTORE_READ_RETRY_MAX_SECONDS
        )
        self._device_firestore_retry_at[device_id] = time.monotonic() + delay

    @staticmethod
    def _merge_device_snapshots(
        remote: Optional[Dict[str, Any]],
        local: Optional[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        if remote is None:
            return dict(local) if local is not None else None
        if local is None:
            return dict(remote)

        remote_seen = parse_iso_timestamp(remote.get("last_seen"))
        local_seen = parse_iso_timestamp(local.get("last_seen"))
        if local_seen is not None and (remote_seen is None or local_seen > remote_seen):
            merged = dict(remote)
            merged.update({key: value for key, value in local.items() if value is not None})
        else:
            merged = dict(local)
            merged.update(remote)

        remote_fix = parse_iso_timestamp(remote.get("last_valid_gps_time"))
        local_fix = parse_iso_timestamp(local.get("last_valid_gps_time"))
        location_source = (
            local
            if local_fix is not None and (remote_fix is None or local_fix > remote_fix)
            else remote
        )
        for key in (
            "last_latitude",
            "last_longitude",
            "latitude",
            "longitude",
            "lat",
            "lon",
            "lng",
            "last_valid_gps_time",
            "gps_source",
            "maps_url",
            "last_fix",
            "latest",
        ):
            if location_source.get(key) is not None:
                merged[key] = location_source[key]
        return merged

    @staticmethod
    def _get_local_device(device_id: str) -> Optional[Dict[str, Any]]:
        conn = sqlite3.connect(str(LOCAL_DB))
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute(
                "SELECT * FROM devices WHERE device_id = ?",
                (device_id,)
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    @staticmethod
    def _get_local_devices() -> List[Dict[str, Any]]:
        conn = sqlite3.connect(str(LOCAL_DB))
        conn.row_factory = sqlite3.Row
        try:
            return [
                dict(row)
                for row in conn.execute("SELECT * FROM devices").fetchall()
            ]
        finally:
            conn.close()

    def _enqueue_device_state(self, device_id: str, gps_state_supplied: bool):
        device = self._get_local_device(device_id)
        if device is None:
            return

        excluded_fields = {
            "last_latitude",
            "last_longitude",
            "latitude",
            "longitude",
            "lat",
            "lon",
            "lng",
            "last_valid_gps_time",
            "gps_source",
            "maps_url",
        }
        sync_doc = {
            key: value
            for key, value in device.items()
            if value is not None and key not in excluded_fields
        }
        sync_doc["status"] = device.get("status", "ONLINE")
        sync_doc["last_seen"] = normalize_iso_timestamp(device.get("last_seen"))
        if gps_state_supplied:
            gps_valid = is_gps_valid_flag(device.get("gps_valid"))
            sync_doc.update({
                "gps_valid": gps_valid,
                "gps_fix": gps_valid,
                "gps_locked": gps_valid,
                "gps_status": device.get("gps_status") or "No Fix",
            })
        else:
            for key in ("gps_valid", "gps_fix", "gps_locked", "gps_status"):
                sync_doc.pop(key, None)

        battery = device.get("battery")
        if battery is not None:
            sync_doc.update(_battery_fields(battery))
        if device.get("is_charging") is not None:
            charging = bool(device.get("is_charging"))
            sync_doc.update({
                "charging": charging,
                "charge_status": "charging" if charging else "discharging",
            })
        heading = device.get("heading") or device.get("bearing") or device.get("course")
        if heading is not None:
            heading_value = float(heading)
            sync_doc.update({
                "heading": heading_value,
                "bearing": heading_value,
                "course": heading_value,
                "direction": heading_value,
                "direction_deg": heading_value,
                "angle": heading_value,
                "rotation": heading_value,
            })
        speed = device.get("speed") or device.get("speed_kmh")
        if speed is not None:
            sync_doc["speed"] = float(speed)
            sync_doc["speed_kmh"] = float(speed)

        event_time = sync_doc["last_seen"] or current_iso_timestamp()
        sync_doc["last_seen"] = event_time
        payload_json = json.dumps({
            "device_id": device_id,
            "update_data": sync_doc,
            "event_time": event_time,
        }, separators=(",", ":"), default=_json_default)
        conn = sqlite3.connect(str(LOCAL_DB))
        try:
            existing = conn.execute(
                "SELECT payload_json FROM firestore_device_outbox WHERE device_id = ?",
                (device_id,)
            ).fetchone()
            if existing is not None:
                previous = json.loads(existing[0])
                if not is_newer_timestamp(event_time, previous.get("event_time"), allow_equal=True):
                    return
            elif conn.execute(
                "SELECT COUNT(*) FROM firestore_device_outbox"
            ).fetchone()[0] >= MAX_FIRESTORE_TELEMETRY_OUTBOX_RECORDS:
                app_logger.error(
                    "Firestore device outbox full device=%s limit=%d",
                    device_id,
                    MAX_FIRESTORE_TELEMETRY_OUTBOX_RECORDS
                )
                return

            conn.execute("""
                INSERT INTO firestore_device_outbox (
                    device_id, payload_json, attempts, next_attempt_at, queued_at, last_error
                ) VALUES (?, ?, 0, 0, ?, NULL)
                ON CONFLICT(device_id) DO UPDATE SET
                    payload_json = excluded.payload_json,
                    attempts = 0,
                    next_attempt_at = 0,
                    queued_at = excluded.queued_at,
                    last_error = NULL
            """, (device_id, payload_json, time.time()))
            conn.commit()
        finally:
            conn.close()

    def _invalidate_device_caches(self, device_id: Optional[str] = None) -> None:
        with self._device_cache_lock:
            if device_id is None:
                self._device_cache.clear()
            else:
                self._device_cache.pop(device_id, None)
            self._devices_cache = None

    def _init_local_db(self):
        """Prepares local SQLite tables for fallback persistence."""
        conn = sqlite3.connect(str(LOCAL_DB))
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS devices (
                device_id TEXT PRIMARY KEY,
                maker TEXT,
                wearer_name TEXT,
                case_number TEXT,
                last_seen TEXT,
                battery INTEGER,
                steps INTEGER,
                heart_rate INTEGER,
                status TEXT,
                last_latitude REAL,
                last_longitude REAL,
                maps_url TEXT,
                gps_source TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS telemetry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT,
                timestamp TEXT,
                received_at TEXT,
                gps_valid INTEGER,
                latitude REAL,
                longitude REAL,
                speed_kmh REAL,
                battery INTEGER,
                steps INTEGER,
                maps_url TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS firestore_telemetry_outbox (
                document_id TEXT PRIMARY KEY,
                device_id TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at REAL NOT NULL DEFAULT 0,
                queued_at REAL NOT NULL,
                last_error TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS firestore_device_outbox (
                device_id TEXT PRIMARY KEY,
                payload_json TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at REAL NOT NULL DEFAULT 0,
                queued_at REAL NOT NULL,
                last_error TEXT
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_firestore_telemetry_outbox_due
            ON firestore_telemetry_outbox(next_attempt_at, queued_at)
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS firestore_history_sync (
                device_id TEXT PRIMARY KEY,
                watermark TEXT,
                watermark_kind TEXT,
                checked_at REAL NOT NULL DEFAULT 0
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT,
                timestamp TEXT,
                alert_type TEXT,
                message TEXT,
                latitude REAL,
                longitude REAL,
                status TEXT,
                acknowledged_by TEXT,
                resolved_by TEXT,
                notes TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                email TEXT PRIMARY KEY,
                password_hash TEXT,
                full_name TEXT,
                role TEXT,
                created_at TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS journeys (
                journey_id TEXT PRIMARY KEY,
                device_id TEXT,
                status TEXT,
                start_time TEXT,
                end_time TEXT,
                start_latitude REAL,
                start_longitude REAL,
                end_latitude REAL,
                end_longitude REAL,
                total_distance_km REAL,
                duration_seconds INTEGER,
                duration_formatted TEXT,
                points_count INTEGER,
                route_coordinates_json TEXT,
                created_at TEXT,
                updated_at TEXT
            )
        """)

        # Migration columns for devices
        existing_device_cols = [c[1] for c in cur.execute("PRAGMA table_info(devices)").fetchall()]
        for col in ["heading", "bearing", "course", "speed"]:
            if col not in existing_device_cols:
                try:
                    cur.execute(f"ALTER TABLE devices ADD COLUMN {col} REAL DEFAULT 0.0")
                except Exception:
                    pass
        for col in ["is_charging", "charging"]:
            if col not in existing_device_cols:
                try:
                    cur.execute(f"ALTER TABLE devices ADD COLUMN {col} INTEGER DEFAULT 0")
                except Exception:
                    pass
        for col in ["last_valid_gps_time", "gps_status", "satellites", "gsm_signal", "gps_source"]:
            if col not in existing_device_cols:
                try:
                    cur.execute(f"ALTER TABLE devices ADD COLUMN {col} TEXT")
                except Exception:
                    pass
        for col in ["gps_valid", "gps_fix", "gps_locked"]:
            if col not in existing_device_cols:
                try:
                    cur.execute(f"ALTER TABLE devices ADD COLUMN {col} INTEGER DEFAULT 0")
                except Exception:
                    pass

        # Migration columns for telemetry
        existing_telem_cols = [c[1] for c in cur.execute("PRAGMA table_info(telemetry)").fetchall()]
        for col in ["heading", "bearing", "course"]:
            if col not in existing_telem_cols:
                try:
                    cur.execute(f"ALTER TABLE telemetry ADD COLUMN {col} REAL DEFAULT 0.0")
                except Exception:
                    pass
        if "received_at" not in existing_telem_cols:
            cur.execute("ALTER TABLE telemetry ADD COLUMN received_at TEXT")
        for col in ["is_charging", "charging"]:
            if col not in existing_telem_cols:
                try:
                    cur.execute(f"ALTER TABLE telemetry ADD COLUMN {col} INTEGER DEFAULT 0")
                except Exception:
                    pass
        if "gps_source" not in existing_telem_cols:
            try:
                cur.execute("ALTER TABLE telemetry ADD COLUMN gps_source TEXT")
            except Exception:
                pass
        if "firestore_document_id" not in existing_telem_cols:
            try:
                cur.execute("ALTER TABLE telemetry ADD COLUMN firestore_document_id TEXT")
            except Exception:
                pass
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_telemetry_firestore_document_id
            ON telemetry(firestore_document_id)
            WHERE firestore_document_id IS NOT NULL
        """)

        conn.commit()
        conn.close()

    @staticmethod
    def _latest_section(device: Dict[str, Any], key: str) -> Dict[str, Any]:
        latest = device.get("latest")
        if not isinstance(latest, dict):
            return {}
        section = latest.get(key)
        return section if isinstance(section, dict) else {}

    @staticmethod
    def _location_telemetry_fields(record: Dict[str, Any]) -> Dict[str, Any]:
        speed = _first_present(record.get("speed_kmh"), record.get("speed"))
        heading = _first_present(
            record.get("heading"),
            record.get("bearing"),
            record.get("course"),
            record.get("direction_deg"),
        )
        accuracy = _first_present(
            record.get("accuracy_m"),
            record.get("horizontal_accuracy_m"),
            record.get("accuracy"),
        )
        try:
            accuracy = float(accuracy) if accuracy is not None else None
            if accuracy is not None and (not math.isfinite(accuracy) or accuracy < 0):
                accuracy = None
        except (TypeError, ValueError, OverflowError):
            accuracy = None
        return {
            "speed": speed,
            "speed_kmh": speed,
            "heading": heading,
            "bearing": heading,
            "accuracy_m": accuracy,
        }

    @classmethod
    def _device_location_candidates(cls, device: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Root, last_fix, and latest.* location-shaped dicts for enrichment/verification."""
        latest = device.get("latest") if isinstance(device.get("latest"), dict) else {}
        candidates: List[Dict[str, Any]] = [
            {
                "latitude": _first_present(
                    device.get("last_latitude"),
                    device.get("latitude"),
                    device.get("lat"),
                ),
                "longitude": _first_present(
                    device.get("last_longitude"),
                    device.get("longitude"),
                    device.get("lon"),
                    device.get("lng"),
                ),
                "gps_valid": device.get("gps_valid"),
                "gps_fix": device.get("gps_fix"),
                "gps_locked": device.get("gps_locked"),
                "gps_source": device.get("gps_source"),
                "gps_time": _first_present(
                    device.get("last_valid_gps_time"), device.get("gps_time")
                ),
                "timestamp": device.get("timestamp"),
                "at": device.get("at"),
                "location_time": device.get("location_time"),
                "last_valid_gps_time": device.get("last_valid_gps_time"),
                "received_at": _first_present(
                    device.get("received_at"), device.get("last_seen")
                ),
                "raw": device.get("raw"),
            }
        ]
        last_fix = device.get("last_fix")
        if isinstance(last_fix, dict):
            inherited = dict(last_fix)
            if inherited.get("gps_valid") is None and inherited.get("gps_fix") is None:
                inherited["gps_valid"] = device.get("gps_valid")
                inherited["gps_fix"] = device.get("gps_fix")
            if inherited.get("gps_source") is None:
                inherited["gps_source"] = device.get("gps_source")
            candidates.append(inherited)
        for key in ("location", "location_on_demand", "sos_alarm"):
            section = latest.get(key)
            if isinstance(section, dict):
                candidates.append(section)
        return candidates

    @classmethod
    def _best_verified_device_fix(
        cls, device_id: str, device: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        best: Optional[Dict[str, Any]] = None
        for candidate in cls._device_location_candidates(device):
            verified = cls._normalize_verified_fix(device_id, candidate)
            if verified is None:
                continue
            if best is None or is_newer_timestamp(
                verified["timestamp"], best.get("timestamp")
            ):
                best = verified
        return best

    @classmethod
    def _apply_device_vitals(cls, d: Dict[str, Any]) -> None:
        """Normalize fields written by the watch receiver into device-level aliases."""
        latest_status = cls._latest_section(d, "status")
        latest_location = cls._latest_section(d, "location")
        latest_hr = cls._latest_section(d, "heart_rate")
        latest_temp = cls._latest_section(d, "temperature")
        latest_bp = cls._latest_section(d, "blood_pressure")
        latest_o2 = cls._latest_section(d, "blood_oxygen")
        latest_glucose = cls._latest_section(d, "blood_glucose")
        latest_sleep = cls._latest_section(d, "sleep")
        latest_sleep_report = cls._latest_section(d, "sleep_report")
        latest_sleep_allday = cls._latest_section(d, "sleep_allday")

        batt = _first_present(
            d.get("battery_percent"),
            d.get("battery"),
            d.get("battery_level"),
            d.get("battery_pct"),
            d.get("batteryLevel"),
            d.get("battery_percentage"),
            latest_status.get("battery_percent"),
            latest_location.get("battery_percent"),
            d.get("charge"),
        )
        if batt is not None:
            try:
                b_int = int(float(batt))
                d.update(_battery_fields(b_int))
                d["battery_status"] = "normal" if b_int > 20 else "low"
            except (ValueError, TypeError):
                pass
        else:
            d.update(_battery_fields(None))
            d.pop("battery_status", None)

        steps = _first_present(
            d.get("steps"),
            latest_status.get("steps"),
            latest_location.get("steps"),
        )
        if steps is not None:
            try:
                d["steps"] = int(float(steps))
            except (ValueError, TypeError):
                d["steps"] = None
        else:
            d["steps"] = None

        heart_rate = _first_present(
            d.get("heart_rate"),
            latest_hr.get("heart_rate_bpm"),
        )
        if heart_rate is not None:
            try:
                d["heart_rate"] = int(float(heart_rate))
            except (ValueError, TypeError):
                d["heart_rate"] = None
        else:
            d["heart_rate"] = None
        hrv = _first_present(d.get("hrv"), latest_hr.get("hrv"))
        if hrv is not None:
            d["hrv"] = hrv

        temperature = _first_present(
            d.get("temperature"),
            latest_temp.get("temperature_c"),
        )
        if temperature is not None:
            d["temperature"] = temperature
            d["temperature_c"] = temperature

        blood_pressure = d.get("blood_pressure")
        if blood_pressure is None:
            systolic = latest_bp.get("systolic")
            diastolic = latest_bp.get("diastolic")
            if systolic is not None and diastolic is not None:
                blood_pressure = f"{systolic}/{diastolic}"
                d["systolic"] = systolic
                d["diastolic"] = diastolic
            elif latest_bp:
                blood_pressure = latest_bp
        if blood_pressure is not None:
            d["blood_pressure"] = blood_pressure

        blood_oxygen = _first_present(
            d.get("blood_oxygen"),
            d.get("spo2"),
            latest_o2.get("spo2_percent"),
        )
        if blood_oxygen is not None:
            d["blood_oxygen"] = blood_oxygen
            d["spo2"] = blood_oxygen
            d["spo2_percent"] = blood_oxygen

        glucose = _first_present(
            d.get("blood_glucose"),
            d.get("glucose_mmol_l"),
            latest_glucose.get("glucose_mmol_l"),
        )
        if glucose is not None:
            d["blood_glucose"] = glucose
            d["glucose_mmol_l"] = glucose

        sleep = d.get("sleep")
        if sleep is None and latest_sleep:
            sleep = latest_sleep
        if sleep:
            d["sleep"] = sleep
        for field, section in (
            ("sleep_report", latest_sleep_report),
            ("sleep_allday", latest_sleep_allday),
        ):
            if section:
                d[field] = section

        latest_status_fields = ("mileage_km",)
        latest_location_fields = (
            "altitude_m",
            "satellites",
            "signal_percent",
            "steps",
            "rolls",
            "alarms",
            "cell_mcc",
            "cell_mnc",
            "cells",
            "wifi",
            "nearby_wifi_count",
            "speed_kmh",
            "direction_deg",
        )
        for field in latest_status_fields:
            value = _first_present(d.get(field), latest_status.get(field))
            if value is not None:
                d[field] = value
        for field in latest_location_fields:
            value = _first_present(d.get(field), latest_location.get(field))
            if value is not None:
                d[field] = value
        if d.get("signal_percent") is not None:
            d["gsm_signal"] = d["signal_percent"]

    def _enrich_device_dict(self, d: Dict[str, Any]) -> Dict[str, Any]:
        """Calculates dynamic status, online state, and GPS status for a device."""
        now_dt = datetime.now(timezone.utc)
        dev_id = str(d.get("device_id") or d.get("id") or "")
        d["id"] = dev_id
        d["device_id"] = dev_id
        d["imei"] = dev_id

        last_seen = d.get("last_seen")
        received_dt = parse_iso_timestamp(last_seen)
        received_age = (
            (now_dt - received_dt).total_seconds()
            if received_dt is not None else None
        )
        is_online = received_age is not None and 0 <= received_age <= 600

        d["online"] = is_online
        d["status"] = "ONLINE" if is_online else "OFFLINE"
        d["last_packet_received"] = last_seen

        verified = self._best_verified_device_fix(dev_id, d)
        lat = None
        lon = None
        if verified is not None:
            lat = verified["latitude"]
            lon = verified["longitude"]
            if not d.get("last_valid_gps_time"):
                d["last_valid_gps_time"] = verified["timestamp"]
            if not d.get("gps_source"):
                d["gps_source"] = verified["gps_source"]
        else:
            for candidate in self._device_location_candidates(d):
                position = valid_coordinate_pair(
                    candidate.get("latitude", candidate.get("lat")),
                    candidate.get(
                        "longitude",
                        candidate.get("lon", candidate.get("lng")),
                    ),
                )
                if position is not None:
                    lat, lon = position
                    break

        d["last_latitude"] = lat
        d["last_longitude"] = lon
        d["latitude"] = lat
        d["longitude"] = lon
        d["lat"] = lat
        d["lon"] = lon
        d["lng"] = lon

        # GPS status is independent of packet/connection status.
        last_valid_gps_time = d.get("last_valid_gps_time")
        if last_valid_gps_time is None and verified is not None:
            last_valid_gps_time = verified["timestamp"]
            d["last_valid_gps_time"] = last_valid_gps_time
        fix_dt = parse_iso_timestamp(last_valid_gps_time)
        fix_age = (
            (now_dt - fix_dt).total_seconds()
            if fix_dt is not None else None
        )
        has_position = valid_coordinate_pair(lat, lon) is not None
        current_fix = (
            verified is not None
            and has_position
            and fix_age is not None
            and 0 <= fix_age <= GPS_FIX_MAX_AGE_SECONDS
        )
        if current_fix:
            gps_status = "GPS Locked"
        elif has_position and (
            (fix_age is not None and fix_age > GPS_FIX_MAX_AGE_SECONDS)
            or verified is not None
        ):
            gps_status = "Location Stale"
        else:
            gps_status = "No Fix"

        d["gps_valid"] = current_fix
        d["gps_fix"] = current_fix
        d["gps_locked"] = current_fix
        d["gps_status"] = gps_status

        self._apply_device_vitals(d)

        is_chg = bool(d.get("is_charging") or d.get("charging") or False)
        d["is_charging"] = is_chg
        d["charging"] = is_chg
        d["isCharging"] = is_chg
        d["charge_status"] = "charging" if is_chg else "discharging"

        # Direction / Heading
        h = float(
            d.get("heading")
            or d.get("bearing")
            or d.get("course")
            or d.get("direction_deg")
            or 0.0
        )
        d["heading"] = h
        d["bearing"] = h
        d["course"] = h
        d["direction"] = h
        d["direction_deg"] = h
        d["angle"] = h
        d["rotation"] = h

        spd = float(d.get("speed") or d.get("speed_kmh") or 0.0)
        d["speed"] = spd
        d["speed_kmh"] = spd

        return d

    # --- DEVICES ---
    def save_device(
        self, device_data: Dict[str, Any], sync_firestore: bool = True
    ) -> bool:
        """
        Saves device telemetry and state to SQLite and Firebase Firestore.
        Preserves previous valid coordinates and battery readings without mock defaults.
        """
        dev_id = device_data["device_id"]
        batt_val = device_data.get("battery") if device_data.get("battery") is not None else device_data.get("battery_level")
        steps_val = device_data.get("steps")
        charging_value = device_data.get("is_charging")
        if charging_value is None:
            charging_value = device_data.get("charging")
        has_charging_value = charging_value is not None
        is_charging_val = 1 if charging_value else 0
        heading_val = (
            device_data.get("heading")
            if device_data.get("heading") is not None
            else (device_data.get("bearing") if device_data.get("bearing") is not None else device_data.get("course"))
        )
        speed_val = device_data.get("speed_kmh") if device_data.get("speed_kmh") is not None else device_data.get("speed")
        incoming_lat = device_data.get("last_latitude")
        if incoming_lat is None:
            incoming_lat = device_data.get("latitude", device_data.get("lat"))
        incoming_lon = device_data.get("last_longitude")
        if incoming_lon is None:
            incoming_lon = device_data.get("longitude", device_data.get("lon", device_data.get("lng")))
        verified_fix = normalize_verified_gps_fix(
            device_data.get("gps_valid"),
            incoming_lat,
            incoming_lon,
            device_data.get("last_valid_gps_time"),
            device_data.get("gps_source")
        )
        incoming_position = verified_fix[:2] if verified_fix else None
        incoming_fix_time = verified_fix[2] if verified_fix else None
        incoming_fix_is_valid = verified_fix is not None
        gps_state_supplied = "gps_valid" in device_data or "gps_status" in device_data
        received_at = normalize_iso_timestamp(device_data.get("last_seen")) or current_iso_timestamp()
        gps_valid_value = is_gps_valid_flag(device_data.get("gps_valid", False))
        gps_status_value = device_data.get("gps_status", "No Fix")

        # 1. Local SQLite Backup
        sqlite_saved = True
        try:
            conn = sqlite3.connect(str(LOCAL_DB))
            cur = conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            existing = cur.execute(
                "SELECT * FROM devices WHERE device_id = ?", (dev_id,)
            ).fetchone()
            columns = [column[1] for column in cur.execute("PRAGMA table_info(devices)").fetchall()]
            previous = dict(zip(columns, existing)) if existing else {}

            last_lat = previous.get("last_latitude")
            last_lon = previous.get("last_longitude")
            last_valid_gps_time = previous.get("last_valid_gps_time")
            maps_url = previous.get("maps_url")
            accepted_fix = (
                incoming_fix_is_valid
                and is_newer_timestamp(incoming_fix_time, last_valid_gps_time)
            )
            gps_source_value = (
                verified_fix[3] if accepted_fix and verified_fix else previous.get("gps_source")
            )
            if accepted_fix and incoming_position is not None:
                last_lat, last_lon = incoming_position
                last_valid_gps_time = incoming_fix_time
                maps_url = f"https://maps.google.com/?q={last_lat:.6f},{last_lon:.6f}"

            state_is_newer = gps_state_supplied and is_newer_timestamp(
                received_at, previous.get("last_seen"), allow_equal=True
            )
            if state_is_newer:
                incoming_gps_valid = is_gps_valid_flag(device_data.get("gps_valid", False))
                if not incoming_gps_valid or accepted_fix:
                    gps_valid_value = 1 if incoming_gps_valid and accepted_fix else 0
                    gps_status_value = device_data.get("gps_status", "No Fix")
                else:
                    gps_valid_value = previous.get("gps_valid", 0)
                    gps_status_value = previous.get("gps_status", "No Fix")
            else:
                gps_valid_value = previous.get("gps_valid", 0)
                gps_status_value = previous.get("gps_status", "No Fix")
            last_seen_value = (
                received_at
                if is_newer_timestamp(received_at, previous.get("last_seen"), allow_equal=True)
                else previous.get("last_seen")
            )
            battery_value = batt_val if batt_val is not None else previous.get("battery")
            steps_value = steps_val if steps_val is not None else previous.get("steps")
            heart_rate_value = (
                device_data.get("heart_rate")
                if device_data.get("heart_rate") is not None
                else previous.get("heart_rate")
            )
            charging_db_value = (
                is_charging_val if has_charging_value else previous.get("is_charging", 0)
            )
            heading_db_value = (
                float(heading_val)
                if heading_val is not None
                else previous.get("heading")
            )
            speed_db_value = (
                float(speed_val)
                if speed_val is not None
                else previous.get("speed")
            )

            cur.execute("""
                INSERT INTO devices (
                    device_id, maker, wearer_name, case_number, last_seen,
                    battery, steps, heart_rate, status, last_latitude, last_longitude, maps_url,
                    heading, bearing, course, speed, is_charging, last_valid_gps_time, gps_status, gps_valid,
                    gps_source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(device_id) DO UPDATE SET
                    maker = excluded.maker,
                    wearer_name = excluded.wearer_name,
                    case_number = excluded.case_number,
                    last_seen = excluded.last_seen,
                    battery = excluded.battery,
                    steps = excluded.steps,
                    heart_rate = excluded.heart_rate,
                    status = excluded.status,
                    last_latitude = excluded.last_latitude,
                    last_longitude = excluded.last_longitude,
                    maps_url = excluded.maps_url,
                    heading = excluded.heading,
                    bearing = excluded.bearing,
                    course = excluded.course,
                    speed = excluded.speed,
                    is_charging = excluded.is_charging,
                    last_valid_gps_time = excluded.last_valid_gps_time,
                    gps_status = excluded.gps_status,
                    gps_valid = excluded.gps_valid,
                    gps_source = excluded.gps_source
            """, (
                dev_id,
                device_data.get("maker") or previous.get("maker"),
                device_data.get("wearer_name") or previous.get("wearer_name"),
                device_data.get("case_number") or previous.get("case_number"),
                last_seen_value,
                battery_value,
                steps_value,
                heart_rate_value,
                device_data.get("status", previous.get("status", "ACTIVE")),
                last_lat,
                last_lon,
                maps_url,
                heading_db_value,
                heading_db_value,
                heading_db_value,
                speed_db_value,
                charging_db_value,
                last_valid_gps_time,
                gps_status_value,
                gps_valid_value,
                gps_source_value
            ))
            conn.commit()
        except Exception as e:
            sqlite_saved = False
            if "conn" in locals():
                conn.rollback()
            app_logger.error(f"SQLite save_device error: {e}")
        finally:
            if "conn" in locals():
                conn.close()
        self._invalidate_device_caches(dev_id)

        if not sync_firestore:
            if not sqlite_saved:
                return False
            self._enqueue_device_state(dev_id, gps_state_supplied)
            return True

        # 2. Asynchronous Firestore Sync
        if is_firebase_active():
            def _bg_sync():
                try:
                    db = get_firestore_client()
                    if db:
                        ts = received_at
                        sync_doc: Dict[str, Any] = {
                            key: value for key, value in device_data.items()
                            if value is not None
                        }
                        for key in (
                            "last_latitude", "last_longitude", "latitude", "longitude",
                            "lat", "lon", "lng", "last_valid_gps_time", "maps_url", "latest",
                            "last_fix", "gps_source", "gps_valid", "gps_fix", "gps_locked",
                            "gps_status"
                        ):
                            sync_doc.pop(key, None)

                        sync_doc["status"] = device_data.get("status", "ONLINE")
                        sync_doc["last_seen"] = ts

                        if heading_val is not None:
                            h = float(heading_val)
                            sync_doc.update({
                                "heading": h,
                                "bearing": h,
                                "course": h,
                                "direction": h,
                                "direction_deg": h,
                                "angle": h,
                                "rotation": h
                            })
                        else:
                            h = 0.0
                        if speed_val is not None:
                            spd = float(speed_val)
                            sync_doc["speed"] = spd
                            sync_doc["speed_kmh"] = spd
                        else:
                            spd = 0.0

                        if gps_state_supplied:
                            is_valid_fix = bool(gps_valid_value)
                            sync_doc.update({
                                "gps_valid": is_valid_fix,
                                "gps_fix": is_valid_fix,
                                "gps_locked": is_valid_fix,
                                "gps_status": gps_status_value
                            })

                        if batt_val is not None:
                            sync_doc.update(_battery_fields(batt_val))

                        location = incoming_position if incoming_fix_is_valid else None
                        if location is not None and incoming_fix_time is not None:
                            lat, lon = location
                            sync_doc.update({
                                "last_latitude": lat,
                                "last_longitude": lon,
                                "lat": lat,
                                "lon": lon,
                                "latitude": lat,
                                "longitude": lon,
                                "last_valid_gps_time": incoming_fix_time,
                                "maps_url": f"https://maps.google.com/?q={lat:.6f},{lon:.6f}"
                            })
                            gps_source = verified_fix[3] if verified_fix else None
                            if gps_source:
                                sync_doc["gps_source"] = gps_source
                                sync_doc["last_fix"] = {
                                    "at": incoming_fix_time,
                                    "lat": lat,
                                    "lon": lon,
                                    "gps_source": gps_source,
                                    "gps_valid": True
                                }
                            sync_doc["latest"] = {
                                "location": {
                                    "lat": lat,
                                    "lon": lon,
                                    "latitude": lat,
                                    "longitude": lon,
                                    "direction_deg": h,
                                    "heading": h,
                                    "bearing": h,
                                    "course": h,
                                    "speed_kmh": spd,
                                    "gps_fix": True,
                                    "gps_valid": True,
                                    "gps_locked": True,
                                    "gps_status": "GPS Locked",
                                    "gps_source": gps_source,
                                    "device_id": dev_id,
                                    "gps_time": incoming_fix_time,
                                    "received_at": ts
                                },
                                "status": {
                                    "device_id": dev_id,
                                    "received_at": ts,
                                    "status": device_data.get("status", "ONLINE")
                                }
                            }
                            if batt_val is not None:
                                sync_doc["latest"]["location"]["battery_percent"] = batt_val
                                sync_doc["latest"]["status"]["battery_percent"] = batt_val

                        self._merge_firestore_device(
                            db,
                            dev_id,
                            sync_doc,
                            event_time=ts,
                            location_time=incoming_fix_time,
                            has_valid_location=location is not None
                        )
                except Exception as e:
                    app_logger.error(f"Firestore save_device sync error for {dev_id}: {e}")

            threading.Thread(target=_bg_sync, daemon=True).start()
        return sqlite_saved

    @staticmethod
    def _merge_firestore_device(
        db: Any,
        device_id: str,
        update_data: Dict[str, Any],
        event_time: Any,
        location_time: Any = None,
        has_valid_location: bool = False
    ):
        """Atomically merge device telemetry without allowing an older fix to win."""
        doc_ref = db.collection("devices").document(device_id)
        location_fields = {
            "last_latitude", "last_longitude", "latitude", "longitude", "lat",
            "lon", "lng", "last_valid_gps_time", "last_fix", "gps_source", "maps_url",
            "latest"
        }
        gps_state_fields = {"gps_valid", "gps_fix", "gps_locked", "gps_status"}

        @cloud_firestore.transactional
        def _apply(transaction):
            snapshot = doc_ref.get(transaction=transaction)
            current = snapshot.to_dict() if snapshot.exists else {}
            patch = dict(update_data)

            if not is_newer_timestamp(event_time, current.get("last_seen"), allow_equal=True):
                for key in gps_state_fields:
                    patch.pop(key, None)
            if not has_valid_location or not is_newer_timestamp(
                location_time, current.get("last_valid_gps_time")
            ):
                for key in location_fields:
                    patch.pop(key, None)
            if not is_newer_timestamp(event_time, current.get("last_seen"), allow_equal=True):
                patch["last_seen"] = current.get("last_seen")

            transaction.set(doc_ref, patch, merge=True)

        _apply(db.transaction())

    def get_devices(self) -> List[Dict[str, Any]]:
        """
        Fetches all devices from Firebase Firestore when active;
        falls back to SQLite with explicit logging if Firebase is offline.
        """
        now = time.monotonic()
        with self._device_cache_lock:
            cached_devices = self._devices_cache
        if (
            cached_devices
            and time.monotonic() - cached_devices[0] < self.DEVICES_CACHE_TTL_SECONDS
        ):
            local_by_id = {
                str(device.get("device_id")): device
                for device in self._get_local_devices()
            }
            return [
                self._enrich_device_dict(
                    self._merge_device_snapshots(
                        device,
                        local_by_id.get(str(device.get("device_id")))
                    ) or dict(device)
                )
                for device in cached_devices[1]
            ]

        if is_firebase_active() and now >= self._devices_firestore_retry_at:
            try:
                db = get_firestore_client()
                if db:
                    docs = db.collection("devices").stream(timeout=2.0)
                    remote_devices = []
                    for doc in docs:
                        d = doc.to_dict() or {}
                        d["device_id"] = doc.id
                        remote_devices.append(d)
                    self._devices_firestore_failures = 0
                    self._devices_firestore_retry_at = 0.0
                    local_by_id = {
                        str(device.get("device_id")): device
                        for device in self._get_local_devices()
                    }
                    merged_devices = [
                        self._merge_device_snapshots(
                            device,
                            local_by_id.pop(str(device.get("device_id")), None)
                        ) or device
                        for device in remote_devices
                    ]
                    merged_devices.extend(local_by_id.values())
                    formatted = [
                        self._enrich_device_dict(dict(device))
                        for device in merged_devices
                    ]
                    with self._device_cache_lock:
                        self._devices_cache = (time.monotonic(), [dict(d) for d in formatted])
                    return formatted
            except Exception as exc:
                self._devices_firestore_failures += 1
                delay = min(
                    self.FIRESTORE_READ_RETRY_BASE_SECONDS
                    * (2 ** min(self._devices_firestore_failures - 1, 6)),
                    self.FIRESTORE_READ_RETRY_MAX_SECONDS
                )
                self._devices_firestore_retry_at = time.monotonic() + delay
                app_logger.warning(
                    "Firestore get_devices deferred retry_seconds=%d exception_type=%s",
                    delay,
                    type(exc).__name__
                )

        # Local records are authoritative for packets accepted by this receiver.
        local_devices = self._get_local_devices()
        if cached_devices:
            local_by_id = {
                str(device.get("device_id")): device for device in local_devices
            }
            merged = [
                self._merge_device_snapshots(
                    device,
                    local_by_id.pop(str(device.get("device_id")), None)
                ) or dict(device)
                for device in cached_devices[1]
            ]
            merged.extend(local_by_id.values())
        else:
            merged = local_devices
        formatted = [self._enrich_device_dict(dict(device)) for device in merged]
        with self._device_cache_lock:
            self._devices_cache = (time.monotonic(), [dict(d) for d in formatted])
        return formatted

    def get_device(self, device_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetches single device details from Firestore when active;
        falls back to SQLite with explicit logging if Firebase is offline.
        """
        local_device = self._get_local_device(device_id)
        cached_device = self._cached_device(device_id, allow_expired=True)
        fresh_cached_device = self._cached_device(device_id)
        if fresh_cached_device is not None:
            merged = self._merge_device_snapshots(fresh_cached_device, local_device)
            return self._enrich_device_dict(merged or fresh_cached_device)

        if (
            is_firebase_active()
            and time.monotonic() >= self._device_firestore_retry_at.get(device_id, 0.0)
        ):
            try:
                db = get_firestore_client()
                if db:
                    doc = db.collection("devices").document(device_id).get(timeout=2.0)
                    if doc.exists:
                        d = doc.to_dict() or {}
                        d["device_id"] = doc.id
                        self._device_firestore_failures.pop(device_id, None)
                        self._device_firestore_retry_at.pop(device_id, None)
                        merged = self._merge_device_snapshots(d, local_device) or d
                        enriched = self._enrich_device_dict(merged)
                        self._cache_device(device_id, merged)
                        return enriched
            except Exception as exc:
                self._record_device_read_failure(device_id)
                retry_in = max(
                    0,
                    int(
                        self._device_firestore_retry_at[device_id]
                        - time.monotonic()
                    )
                )
                app_logger.warning(
                    "Firestore get_device deferred device=%s retry_seconds=%d "
                    "exception_type=%s",
                    device_id,
                    retry_in,
                    type(exc).__name__
                )

        fallback = self._merge_device_snapshots(cached_device, local_device)
        if fallback is not None:
            enriched = self._enrich_device_dict(fallback)
            self._cache_device(device_id, fallback)
            return enriched
        return None

    def get_local_device(self, device_id: str) -> Optional[Dict[str, Any]]:
        """Read the receiver's SQLite-authoritative device snapshot without cloud I/O."""
        device = self._get_local_device(device_id)
        return self._enrich_device_dict(device) if device is not None else None

    def update_device_wearer(self, device_id: str, wearer_name: str, case_number: str):
        conn = sqlite3.connect(str(LOCAL_DB))
        cur = conn.cursor()
        cur.execute(
            "UPDATE devices SET wearer_name = ?, case_number = ? WHERE device_id = ?",
            (wearer_name, case_number, device_id)
        )
        conn.commit()
        conn.close()
        self._invalidate_device_caches(device_id)

        if is_firebase_active():
            def _bg_sync():
                try:
                    db = get_firestore_client()
                    if db:
                        db.collection("devices").document(device_id).set({
                            "wearer_name": wearer_name,
                            "case_number": case_number
                        }, merge=True)
                except Exception as e:
                    app_logger.error(f"Firestore update_device_wearer error: {e}")
            threading.Thread(target=_bg_sync, daemon=True).start()

    # --- TELEMETRY ---
    def save_telemetry(
        self, data: Dict[str, Any], wait_for_firestore: bool = False
    ) -> bool:
        """
        Persist verified watch telemetry locally and enqueue idempotent Firestore writes.
        """
        dev_id = str(data["device_id"])
        verified_fix = normalize_verified_gps_fix(
            data.get("gps_valid"),
            data.get("latitude"),
            data.get("longitude"),
            data.get("timestamp"),
            data.get("gps_source"),
            max_age_seconds=None
        )
        if verified_fix is None:
            app_logger.info(
                "GPS telemetry not persisted device=%s reason=unverified_fix",
                dev_id
            )
            return False
        lat, lon, gps_time, gps_source = verified_fix
        is_gps_valid = True
        is_current = normalize_verified_gps_fix(
            data.get("gps_valid"),
            lat,
            lon,
            gps_time,
            gps_source
        ) is not None
        speed_val = data.get("speed_kmh") if data.get("speed_kmh") is not None else data.get("speed")
        bearing_val = (
            data.get("heading")
            if data.get("heading") is not None
            else (data.get("bearing") if data.get("bearing") is not None else data.get("course"))
        )
        batt_val = data.get("battery")
        steps_val = data.get("steps")
        received_at = normalize_iso_timestamp(data.get("received_at")) or current_iso_timestamp()
        history_document_id = hashlib.sha256(
            f"{dev_id}|{gps_time}|{lat:.6f}|{lon:.6f}|{gps_source}".encode("utf-8")
        ).hexdigest()
        maps_url = f"https://maps.google.com/?q={lat:.6f},{lon:.6f}"
        gps_status = "GPS Locked" if is_current else "Location Stale"
        telemetry_record_data = {
            **data,
            "timestamp": gps_time,
            "received_at": received_at,
            "gps_valid": True,
            "gps_fix": True,
            "gps_locked": True,
            "gps_current": is_current,
            "gps_status": gps_status,
            "gps_source": gps_source,
            "latitude": lat,
            "longitude": lon,
            "maps_url": maps_url,
        }
        heading = float(bearing_val or 0.0)
        speed = float(speed_val or 0.0)
        firestore_record: Dict[str, Any] = {
            **telemetry_record_data,
            "heading": heading,
            "bearing": heading,
            "course": heading,
            "direction_deg": heading,
            "direction": heading,
            "speed": speed,
            "speed_kmh": speed,
        }
        if batt_val is not None:
            firestore_record["battery"] = batt_val
            firestore_record["battery_percent"] = batt_val

        device_update: Dict[str, Any] = {
            "heading": heading,
            "bearing": heading,
            "course": heading,
            "direction": heading,
            "direction_deg": heading,
            "angle": heading,
            "rotation": heading,
            "speed": speed,
            "speed_kmh": speed,
            "status": "ONLINE",
            "last_seen": received_at,
            "gps_valid": is_current,
            "gps_fix": is_current,
            "gps_locked": is_current,
            "gps_status": gps_status,
        }
        if batt_val is not None:
            device_update.update(_battery_fields(batt_val))
        if steps_val is not None:
            device_update["steps"] = steps_val
        if data.get("is_charging") is not None or data.get("charging") is not None:
            charging = bool(data.get("is_charging") or data.get("charging"))
            device_update.update({
                "is_charging": charging,
                "charging": charging,
                "charge_status": "charging" if charging else "discharging",
            })
        if is_current:
            device_update.update({
                "gps_source": gps_source,
                "last_fix": {
                    "at": gps_time,
                    "lat": lat,
                    "lon": lon,
                    "gps_source": gps_source,
                    "gps_valid": True,
                },
                "last_latitude": lat,
                "last_longitude": lon,
                "lat": lat,
                "lon": lon,
                "latitude": lat,
                "longitude": lon,
                "last_valid_gps_time": gps_time,
                "maps_url": maps_url,
                "latest": {
                    "location": {
                        "lat": lat,
                        "lon": lon,
                        "latitude": lat,
                        "longitude": lon,
                        "direction_deg": heading,
                        "heading": heading,
                        "bearing": heading,
                        "course": heading,
                        "speed_kmh": speed,
                        "gps_fix": True,
                        "gps_valid": True,
                        "gps_locked": True,
                        "gps_status": "GPS Locked",
                        "gps_source": gps_source,
                        "device_id": dev_id,
                        "gps_time": gps_time,
                        "received_at": received_at,
                    },
                    "status": {
                        "device_id": dev_id,
                        "received_at": received_at,
                        "status": "ONLINE",
                    },
                },
            })
            if batt_val is not None:
                device_update["latest"]["location"]["battery_percent"] = batt_val
                device_update["latest"]["status"]["battery_percent"] = batt_val

        outbox_payload = {
            "device_id": dev_id,
            "document_id": history_document_id,
            "telemetry": firestore_record,
            "device_update": device_update,
            "event_time": received_at,
            "location_time": gps_time,
            "has_valid_location": is_current,
        }
        payload_json = json.dumps(
            outbox_payload,
            separators=(",", ":"),
            default=_json_default
        )

        # 1. Local SQLite
        try:
            conn = sqlite3.connect(str(LOCAL_DB))
            cur = conn.cursor()
            cur.execute("BEGIN")
            cur.execute("""
                INSERT OR IGNORE INTO telemetry (
                    device_id, timestamp, received_at, gps_valid, latitude, longitude,
                    speed_kmh, battery, steps, maps_url, heading, bearing, course,
                    gps_source, firestore_document_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                dev_id,
                gps_time,
                received_at,
                1 if is_gps_valid else 0,
                lat,
                lon,
                float(speed_val) if speed_val is not None else 0.0,
                batt_val,
                steps_val,
                maps_url,
                float(bearing_val) if bearing_val is not None else 0.0,
                float(bearing_val) if bearing_val is not None else 0.0,
                float(bearing_val) if bearing_val is not None else 0.0,
                gps_source,
                history_document_id
            ))
            sqlite_inserted = cur.rowcount == 1
            pending_count = cur.execute(
                "SELECT COUNT(*) FROM firestore_telemetry_outbox"
            ).fetchone()[0]
            queue_record_exists = cur.execute(
                "SELECT 1 FROM firestore_telemetry_outbox WHERE document_id = ?",
                (history_document_id,)
            ).fetchone() is not None
            if queue_record_exists or pending_count < MAX_FIRESTORE_TELEMETRY_OUTBOX_RECORDS:
                cur.execute("""
                    INSERT OR IGNORE INTO firestore_telemetry_outbox (
                        document_id, device_id, payload_json, next_attempt_at, queued_at
                    ) VALUES (?, ?, ?, 0, ?)
                """, (
                    history_document_id,
                    dev_id,
                    payload_json,
                    time.time()
                ))
            else:
                app_logger.error(
                    "Firestore telemetry outbox full device=%s limit=%d",
                    dev_id,
                    MAX_FIRESTORE_TELEMETRY_OUTBOX_RECORDS
                )
            conn.commit()
            conn.close()
            app_logger.info(
                "SQLite verified GPS telemetry saved device=%s inserted=%s source=%s",
                dev_id,
                sqlite_inserted,
                gps_source
            )
        except sqlite3.Error as exc:
            app_logger.error(
                "SQLite GPS telemetry persistence failed device=%s exception_type=%s",
                dev_id,
                type(exc).__name__
            )
            try:
                conn.rollback()
                conn.close()
            except (UnboundLocalError, sqlite3.Error):
                pass
            return False

        if wait_for_firestore:
            self.sync_pending_firestore_telemetry(
                document_id=history_document_id,
                force=True
            )
        return True

    def _sync_firestore_telemetry_payload(self, db: Any, payload: Dict[str, Any]):
        device_id = str(payload["device_id"])
        document_id = str(payload["document_id"])
        db.collection("telemetry").document(document_id).set(
            payload["telemetry"], merge=True
        )
        db.collection("devices").document(device_id).collection(
            "history"
        ).document(document_id).set(payload["telemetry"], merge=True)
        self._merge_firestore_device(
            db,
            device_id,
            payload["device_update"],
            event_time=payload["event_time"],
            location_time=payload["location_time"],
            has_valid_location=bool(payload["has_valid_location"])
        )

    def sync_pending_firestore_telemetry(
        self,
        limit: int = FIRESTORE_OUTBOX_BATCH_SIZE,
        document_id: Optional[str] = None,
        force: bool = False
    ) -> int:
        """Retry due telemetry writes from the durable SQLite outbox."""
        if not is_firebase_active():
            return 0
        now = time.time()
        conn = sqlite3.connect(str(LOCAL_DB))
        conn.row_factory = sqlite3.Row
        try:
            if document_id is not None:
                rows = conn.execute(
                    """
                    SELECT document_id, payload_json, attempts
                    FROM firestore_telemetry_outbox
                    WHERE document_id = ? AND (next_attempt_at <= ? OR ?)
                    """,
                    (document_id, now, int(force))
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT document_id, payload_json, attempts
                    FROM firestore_telemetry_outbox
                    WHERE next_attempt_at <= ?
                    ORDER BY queued_at
                    LIMIT ?
                    """,
                    (now, max(1, min(limit, FIRESTORE_OUTBOX_BATCH_SIZE)))
                ).fetchall()
        finally:
            conn.close()
        if not rows:
            return 0

        db = get_firestore_client()
        if db is None:
            return 0

        synced_count = 0
        for row in rows:
            queued_id = str(row["document_id"])
            try:
                payload = json.loads(row["payload_json"])
                self._sync_firestore_telemetry_payload(db, payload)
                conn = sqlite3.connect(str(LOCAL_DB))
                try:
                    conn.execute(
                        "DELETE FROM firestore_telemetry_outbox WHERE document_id = ?",
                        (queued_id,)
                    )
                    conn.commit()
                finally:
                    conn.close()
                synced_count += 1
                app_logger.info(
                    "Firestore verified GPS telemetry sync succeeded document_id=%s",
                    queued_id
                )
            except Exception as exc:
                attempts = int(row["attempts"]) + 1
                delay = min(
                    FIRESTORE_RETRY_BASE_SECONDS ** min(attempts, 8),
                    FIRESTORE_RETRY_MAX_SECONDS
                )
                conn = sqlite3.connect(str(LOCAL_DB))
                try:
                    conn.execute(
                        """
                        UPDATE firestore_telemetry_outbox
                        SET attempts = ?, next_attempt_at = ?, last_error = ?
                        WHERE document_id = ?
                        """,
                        (attempts, time.time() + delay, type(exc).__name__, queued_id)
                    )
                    conn.commit()
                finally:
                    conn.close()
                app_logger.warning(
                    "Firestore telemetry sync deferred document_id=%s attempts=%d "
                    "retry_seconds=%d exception_type=%s",
                    queued_id,
                    attempts,
                    delay,
                    type(exc).__name__
                )
        return synced_count

    def sync_pending_firestore_device_updates(
        self, limit: int = FIRESTORE_OUTBOX_BATCH_SIZE
    ) -> int:
        """Sync the latest coalesced packet state for each device."""
        if not is_firebase_active():
            return 0
        now = time.time()
        conn = sqlite3.connect(str(LOCAL_DB))
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                """
                SELECT device_id, payload_json, attempts
                FROM firestore_device_outbox
                WHERE next_attempt_at <= ?
                ORDER BY queued_at
                LIMIT ?
                """,
                (now, max(1, min(limit, FIRESTORE_OUTBOX_BATCH_SIZE)))
            ).fetchall()
        finally:
            conn.close()
        if not rows:
            return 0

        db = get_firestore_client()
        if db is None:
            return 0

        synced_count = 0
        for row in rows:
            device_id = str(row["device_id"])
            try:
                payload = json.loads(row["payload_json"])
                self._merge_firestore_device(
                    db,
                    device_id,
                    payload["update_data"],
                    event_time=payload["event_time"],
                    has_valid_location=False
                )
                conn = sqlite3.connect(str(LOCAL_DB))
                try:
                    conn.execute(
                        "DELETE FROM firestore_device_outbox WHERE device_id = ?",
                        (device_id,)
                    )
                    conn.commit()
                finally:
                    conn.close()
                synced_count += 1
            except Exception as exc:
                attempts = int(row["attempts"]) + 1
                delay = min(
                    FIRESTORE_RETRY_BASE_SECONDS ** min(attempts, 8),
                    FIRESTORE_RETRY_MAX_SECONDS
                )
                conn = sqlite3.connect(str(LOCAL_DB))
                try:
                    conn.execute(
                        """
                        UPDATE firestore_device_outbox
                        SET attempts = ?, next_attempt_at = ?, last_error = ?
                        WHERE device_id = ?
                        """,
                        (attempts, time.time() + delay, type(exc).__name__, device_id)
                    )
                    conn.commit()
                finally:
                    conn.close()
                app_logger.warning(
                    "Firestore device state sync deferred device=%s attempts=%d "
                    "retry_seconds=%d exception_type=%s",
                    device_id,
                    attempts,
                    delay,
                    type(exc).__name__
                )
        return synced_count

    def _firestore_sync_worker(self):
        while not self._sync_worker_stop.wait(2.0):
            try:
                self.sync_pending_firestore_telemetry()
                self.sync_pending_firestore_device_updates()
            except Exception as exc:
                app_logger.error(
                    "Firestore telemetry worker failed exception_type=%s",
                    type(exc).__name__
                )

    def start_firestore_sync_worker(self):
        with self._sync_worker_lock:
            if self._sync_worker_thread and self._sync_worker_thread.is_alive():
                return
            self._sync_worker_stop.clear()
            self._sync_worker_thread = threading.Thread(
                target=self._firestore_sync_worker,
                daemon=True,
                name="FirestoreTelemetrySync"
            )
            self._sync_worker_thread.start()

    def stop_firestore_sync_worker(self):
        with self._sync_worker_lock:
            thread = self._sync_worker_thread
            self._sync_worker_stop.set()
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=5)
        with self._sync_worker_lock:
            if self._sync_worker_thread is thread:
                self._sync_worker_thread = None

    @staticmethod
    def _looks_like_watch_frame(raw: Any) -> bool:
        if not isinstance(raw, str):
            return False
        frame = raw.strip()
        return frame.startswith("[") and frame.endswith("]") and "*" in frame

    @staticmethod
    def _matches_raw_watch_gps_fix(device_id: str, fix: Dict[str, Any]) -> bool:
        raw = fix.get("raw")
        if not isinstance(raw, str):
            return False
        frame = raw.strip()
        if not frame.startswith("[") or not frame.endswith("]"):
            return False

        try:
            maker, frame_device_id, length_hex, payload = frame[1:-1].split("*", 3)
            if frame_device_id != device_id or int(length_hex, 16) != len(payload.encode("latin-1")):
                return False
            fields = payload.split(",")
            if (
                not maker
                or len(fields) < 8
                or fields[0].strip().upper() not in {"UD", "CRUD"}
                or fields[3].strip().upper() != "A"
            ):
                return False

            latitude = float(fields[4].strip())
            longitude = float(fields[6].strip())
            if fields[5].strip().upper() == "S":
                latitude = -abs(latitude)
            if fields[7].strip().upper() == "W":
                longitude = -abs(longitude)
            raw_position = valid_coordinate_pair(latitude, longitude)
            stored_position = valid_coordinate_pair(
                fix.get("latitude", fix.get("lat")),
                fix.get("longitude", fix.get("lon", fix.get("lng")))
            )
            if raw_position is None or stored_position is None:
                return False
            if tuple(round(value, 6) for value in raw_position) != tuple(
                round(value, 6) for value in stored_position
            ):
                return False

            raw_time = datetime.strptime(
                f"{fields[1].strip()}{fields[2].strip()}", "%y%m%d%H%M%S"
            ).replace(tzinfo=timezone.utc)
            stored_time = parse_iso_timestamp(fix_event_timestamp(fix))
            return stored_time is not None and abs((raw_time - stored_time).total_seconds()) <= 1
        except (IndexError, TypeError, ValueError, UnicodeEncodeError):
            return False

    @staticmethod
    def _normalize_verified_fix(
        device_id: str, fix: Any
    ) -> Optional[Dict[str, Any]]:
        if not isinstance(fix, dict):
            return None

        # Explicit gps_fix false rejects the fix even if other flags are set.
        if fix.get("gps_fix") is False:
            return None

        gps_source = fix.get("gps_source") or fix.get("source")
        has_fix_flag = any(
            is_gps_valid_flag(fix.get(flag))
            for flag in ("gps_valid", "gps_fix", "gps_locked")
        )
        latitude = fix.get("latitude", fix.get("lat"))
        longitude = fix.get("longitude", fix.get("lon", fix.get("lng")))
        timestamp = fix_event_timestamp(fix)

        if not gps_source:
            if FirestoreService._matches_raw_watch_gps_fix(device_id, fix):
                gps_source = "watch_frame"
            elif FirestoreService._looks_like_watch_frame(fix.get("raw")):
                # Frame-shaped raw that failed validation must not be promoted.
                return None
            elif (
                has_fix_flag
                and valid_coordinate_pair(latitude, longitude) is not None
                and normalize_iso_timestamp(timestamp)
            ):
                # Writer-shaped docs without gps_source are trusted as watch_tcp.
                gps_source = "watch_tcp"

        verified_fix = normalize_verified_gps_fix(
            has_fix_flag,
            latitude,
            longitude,
            timestamp,
            gps_source,
            max_age_seconds=None
        )
        if verified_fix is None:
            return None

        latitude, longitude, timestamp, gps_source = verified_fix
        return {
            "device_id": device_id,
            "timestamp": timestamp,
            "gps_valid": True,
            "gps_fix": True,
            "gps_locked": True,
            "gps_status": "GPS Locked",
            "gps_source": gps_source,
            "latitude": latitude,
            "longitude": longitude,
            "maps_url": f"https://maps.google.com/?q={latitude:.6f},{longitude:.6f}"
        }

    @staticmethod
    def _history_record_event_time(record: Dict[str, Any]) -> Optional[datetime]:
        return parse_iso_timestamp(fix_event_timestamp(record))

    def _sync_firestore_telemetry_history(self, device_id: str, limit: int) -> None:
        """Incrementally import verified fixes from history, telemetry, and logs."""
        if not is_firebase_active():
            return

        with self._firestore_history_sync_lock:
            try:
                db = get_firestore_client()
                if not db:
                    return

                history_sources = (
                    (
                        device_id,
                        "device_history",
                        db.collection("devices").document(device_id).collection("history"),
                        False
                    ),
                    (
                        f"{device_id}::telemetry",
                        "telemetry",
                        db.collection("telemetry"),
                        True
                    ),
                    (
                        f"{device_id}::logs",
                        "logs",
                        db.collection("logs"),
                        True
                    ),
                )
                for sync_id, source_name, collection_ref, is_global in history_sources:
                    now = time.time()
                    if time.monotonic() < self._history_firestore_retry_at.get(
                        sync_id, 0.0
                    ):
                        continue

                    conn = sqlite3.connect(str(LOCAL_DB))
                    try:
                        state = conn.execute(
                            """
                            SELECT watermark, watermark_kind, checked_at
                            FROM firestore_history_sync WHERE device_id = ?
                            """,
                            (sync_id,)
                        ).fetchone()
                    finally:
                        conn.close()

                    if state and now - state[2] < 15:
                        continue

                    watermark, watermark_kind = (
                        (state[0], state[1]) if state else (None, None)
                    )
                    try:
                        query = collection_ref
                        if is_global:
                            query = query.where("device_id", "==", device_id)

                        if is_global:
                            watermark_prefix = (
                                "logs_received_at"
                                if source_name == "logs"
                                else "telemetry_timestamp"
                            )
                            if source_name in {"logs", "telemetry"}:
                                if watermark is not None and not (
                                    watermark_kind or ""
                                ).startswith(watermark_prefix):
                                    watermark = None
                                    watermark_kind = None

                                watermark_field = (
                                    "received_at" if source_name == "logs" else "timestamp"
                                )
                                if watermark is None:
                                    query = query.order_by(
                                        watermark_field,
                                        direction=cloud_firestore.Query.DESCENDING
                                    ).limit(max(limit * 4, 2000))
                                else:
                                    query_value = (
                                        parse_iso_timestamp(watermark)
                                        if (watermark_kind or "").endswith("_datetime")
                                        else watermark
                                    )
                                    query = query.where(
                                        watermark_field, ">", query_value
                                    ).order_by(
                                        watermark_field,
                                        direction=cloud_firestore.Query.ASCENDING
                                    ).limit(500)

                                snapshots = list(query.stream(timeout=10.0))
                            else:
                                snapshots = list(query.stream(timeout=10.0))
                                timestamped = [
                                    (snapshot, self._history_record_event_time(
                                        snapshot.to_dict() or {}
                                    ))
                                    for snapshot in snapshots
                                ]
                                timestamped = [
                                    (snapshot, timestamp)
                                    for snapshot, timestamp in timestamped
                                    if timestamp is not None
                                ]
                                watermark_time = parse_iso_timestamp(watermark)
                                if watermark is not None and watermark_time is None:
                                    app_logger.warning(
                                        "Invalid Firestore history watermark; restarting sync "
                                        "device=%s collection=%s",
                                        device_id,
                                        source_name
                                    )
                                    watermark = None
                                    watermark_kind = None
                                if watermark is None:
                                    timestamped.sort(
                                        key=lambda item: (
                                            item[1],
                                            parse_iso_timestamp(
                                                (item[0].to_dict() or {}).get("received_at")
                                            )
                                            or datetime.min.replace(tzinfo=timezone.utc),
                                        ),
                                        reverse=True,
                                    )
                                else:
                                    timestamped = [
                                        item for item in timestamped
                                        if item[1] > watermark_time
                                    ]
                                    timestamped.sort(
                                        key=lambda item: (
                                            item[1],
                                            parse_iso_timestamp(
                                                (item[0].to_dict() or {}).get("received_at")
                                            )
                                            or datetime.min.replace(tzinfo=timezone.utc),
                                        )
                                    )
                                    timestamped = timestamped[:500]
                                snapshots = [snapshot for snapshot, _ in timestamped]
                        elif watermark is None:
                            snapshots = list(
                                query.order_by(
                                    "timestamp",
                                    direction=cloud_firestore.Query.DESCENDING
                                ).limit(max(limit, 500)).stream(timeout=10.0)
                            )
                        else:
                            query_value = (
                                parse_iso_timestamp(watermark)
                                if watermark_kind == "datetime"
                                else watermark
                            )
                            snapshots = list(
                                query.where("timestamp", ">", query_value)
                                .order_by(
                                    "timestamp",
                                    direction=cloud_firestore.Query.ASCENDING
                                )
                                .limit(500)
                                .stream(timeout=10.0)
                            )

                        records = []
                        skipped_count = 0
                        for snapshot in snapshots:
                            record = snapshot.to_dict() or {}
                            if source_name == "logs" and str(
                                record.get("type") or ""
                            ).strip().lower() not in LOG_LOCATION_TYPES:
                                skipped_count += 1
                                continue
                            verified_fix = self._normalize_verified_fix(device_id, record)
                            if verified_fix is None:
                                skipped_count += 1
                                continue
                            timestamp = verified_fix["timestamp"]
                            latitude = verified_fix["latitude"]
                            longitude = verified_fix["longitude"]
                            battery = _first_present(
                                record.get("battery"),
                                record.get("battery_percent"),
                            )
                            records.append((
                                record.get("device_id") or device_id,
                                timestamp,
                                normalize_iso_timestamp(record.get("received_at")),
                                1,
                                latitude,
                                longitude,
                                float(record.get("speed_kmh") or record.get("speed") or 0.0),
                                battery,
                                record.get("steps"),
                                verified_fix["maps_url"],
                                float(
                                    record.get("heading")
                                    or record.get("bearing")
                                    or record.get("direction_deg")
                                    or 0.0
                                ),
                                float(
                                    record.get("bearing")
                                    or record.get("heading")
                                    or record.get("direction_deg")
                                    or 0.0
                                ),
                                float(
                                    record.get("course")
                                    or record.get("heading")
                                    or record.get("direction_deg")
                                    or 0.0
                                ),
                                int(bool(record.get("is_charging") or record.get("charging"))),
                                int(bool(record.get("charging") or record.get("is_charging"))),
                                verified_fix["gps_source"],
                                snapshot.id
                            ))

                        if watermark is None:
                            records.sort(
                                key=lambda record: parse_iso_timestamp(record[1])
                                or datetime.min.replace(tzinfo=timezone.utc),
                                reverse=True
                            )
                            records = records[:max(limit, 500)]

                        next_watermark = watermark
                        next_kind = watermark_kind
                        if snapshots:
                            watermark_snapshot = snapshots[0] if watermark is None else snapshots[-1]
                            raw_record = watermark_snapshot.to_dict() or {}
                            if source_name == "logs":
                                raw_timestamp = raw_record.get("received_at")
                            elif source_name == "telemetry":
                                raw_timestamp = raw_record.get("timestamp")
                            else:
                                raw_timestamp = fix_event_timestamp(raw_record)
                            if raw_timestamp is None:
                                raw_timestamp = raw_record.get("timestamp")
                            normalized_watermark = normalize_iso_timestamp(raw_timestamp)
                            if normalized_watermark:
                                if source_name in {"logs", "telemetry"}:
                                    watermark_prefix = (
                                        "logs_received_at"
                                        if source_name == "logs"
                                        else "telemetry_timestamp"
                                    )
                                    next_kind = watermark_prefix + (
                                        "_datetime"
                                        if isinstance(raw_timestamp, datetime)
                                        else "_string"
                                    )
                                else:
                                    next_kind = (
                                        "datetime"
                                        if isinstance(raw_timestamp, datetime)
                                        else "string"
                                    )
                                next_watermark = normalized_watermark
                            elif raw_timestamp is not None:
                                next_kind = "string"
                                next_watermark = str(raw_timestamp)

                        conn = sqlite3.connect(str(LOCAL_DB))
                        try:
                            cursor = conn.executemany(
                                """
                                INSERT OR IGNORE INTO telemetry (
                                    device_id, timestamp, received_at, gps_valid, latitude, longitude,
                                    speed_kmh, battery, steps, maps_url, heading, bearing, course,
                                    is_charging, charging, gps_source, firestore_document_id
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """,
                                records
                            )
                            inserted_count = cursor.rowcount
                            conn.execute(
                                """
                                INSERT INTO firestore_history_sync (
                                    device_id, watermark, watermark_kind, checked_at
                                ) VALUES (?, ?, ?, ?)
                                ON CONFLICT(device_id) DO UPDATE SET
                                    watermark = excluded.watermark,
                                    watermark_kind = excluded.watermark_kind,
                                    checked_at = excluded.checked_at
                                """,
                                (sync_id, next_watermark, next_kind, now)
                            )
                            conn.commit()
                        finally:
                            conn.close()
                        self._history_firestore_retry_at.pop(sync_id, None)
                        self._history_firestore_failures.pop(sync_id, None)
                        app_logger.info(
                            "Firestore GPS history sync succeeded device=%s collection=%s "
                            "scanned=%d verified=%d skipped=%d inserted=%d",
                            device_id,
                            source_name,
                            len(snapshots),
                            len(records),
                            skipped_count,
                            inserted_count
                        )
                    except Exception as e:
                        failures = self._history_firestore_failures.get(sync_id, 0) + 1
                        self._history_firestore_failures[sync_id] = failures
                        retry_delay = min(
                            self.FIRESTORE_READ_RETRY_BASE_SECONDS *
                            (2 ** min(failures - 1, 6)),
                            self.FIRESTORE_READ_RETRY_MAX_SECONDS
                        )
                        self._history_firestore_retry_at[sync_id] = (
                            time.monotonic() + retry_delay
                        )
                        app_logger.error(
                            "Firestore GPS history sync failed device=%s collection=%s "
                            "retry_seconds=%d: %s",
                            device_id,
                            source_name,
                            retry_delay,
                            e
                        )
                        conn = sqlite3.connect(str(LOCAL_DB))
                        try:
                            conn.execute(
                                """
                                INSERT INTO firestore_history_sync (
                                    device_id, checked_at
                                ) VALUES (?, ?)
                                ON CONFLICT(device_id) DO UPDATE SET
                                    checked_at = excluded.checked_at
                                """,
                                (sync_id, now)
                            )
                            conn.commit()
                        finally:
                            conn.close()
            except Exception as e:
                app_logger.error(
                    "Firestore telemetry history sync setup failed for %s: %s",
                    device_id,
                    e
                )

    def get_latest_valid_telemetry(self, device_id: str) -> Optional[Dict[str, Any]]:
        """Returns the newest valid local GPS fix by event time."""
        conn = sqlite3.connect(str(LOCAL_DB))
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute(
                """
                SELECT * FROM telemetry
                WHERE device_id = ?
                  AND gps_valid = 1
                  AND gps_source IN ('watch_frame', 'watch_tcp', 'watch_udp', 'gps_ingest_api')
                  AND latitude BETWEEN -90 AND 90
                  AND longitude BETWEEN -180 AND 180
                  AND NOT (latitude = 0 AND longitude = 0)
                  AND julianday(timestamp) IS NOT NULL
                  AND julianday(timestamp) <= julianday('now')
                ORDER BY julianday(timestamp) DESC, id DESC
                LIMIT 1
                """,
                (device_id,)
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def get_latest_valid_location(
        self,
        device_id: str,
        device_data: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """Chooses the newest valid location from the device snapshot and SQLite history."""
        device_data = device_data or {}
        location: Optional[Dict[str, Any]] = None
        for index, candidate in enumerate(
            self._device_location_candidates(device_data)
        ):
            verified_fix = self._normalize_verified_fix(device_id, candidate)
            if verified_fix is None:
                continue
            if location is None or is_newer_timestamp(
                verified_fix["timestamp"], location.get("timestamp")
            ):
                location = {
                    **verified_fix,
                    **self._location_telemetry_fields(candidate),
                    "maps_url": (
                        device_data.get("maps_url")
                        if index == 0 and device_data.get("maps_url")
                        else verified_fix.get("maps_url")
                    ),
                    "is_newer": index > 0,
                }

        telemetry = self.get_latest_valid_telemetry(device_id)
        telemetry_time = telemetry.get("timestamp") if telemetry else None
        telemetry_matches_current_time = (
            telemetry is not None
            and parse_iso_timestamp(telemetry_time) is not None
            and parse_iso_timestamp(telemetry_time) == parse_iso_timestamp(
                location.get("timestamp") if location else None
            )
        )
        if telemetry and (
            location is None
            or is_newer_timestamp(telemetry_time, location.get("timestamp"))
            or (
                telemetry_matches_current_time
                and not location.get("gps_source")
                and bool(telemetry.get("gps_source"))
            )
        ):
            location = {
                "latitude": telemetry["latitude"],
                "longitude": telemetry["longitude"],
                "timestamp": telemetry_time,
                **self._location_telemetry_fields(telemetry),
                "maps_url": telemetry.get("maps_url"),
                "gps_source": telemetry.get("gps_source"),
                "is_newer": True
            }

        if location is not None:
            fix_time = parse_iso_timestamp(location.get("timestamp"))
            is_stale = (
                fix_time is not None
                and (datetime.now(timezone.utc) - fix_time).total_seconds()
                > GPS_FIX_MAX_AGE_SECONDS
            )
            location["gps_status"] = "Location Stale" if is_stale else "GPS Locked"
            location["gps_valid"] = not is_stale
            location["gps_fix"] = not is_stale
            location["gps_locked"] = not is_stale
        return location

    def get_last_known_location(
        self, device_data: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """Return a timestamped, clearly non-current position from the device snapshot."""
        device_data = device_data or {}
        device_id = str(device_data.get("device_id") or "")
        candidates = self._device_location_candidates(device_data)
        telemetry = self.get_latest_valid_telemetry(device_id)
        if telemetry is not None:
            candidates.append(telemetry)
        now = datetime.now(timezone.utc)
        known_locations = []

        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            position = valid_coordinate_pair(
                candidate.get("latitude", candidate.get("lat")),
                candidate.get("longitude", candidate.get("lon", candidate.get("lng")))
            )
            timestamp = normalize_iso_timestamp(fix_event_timestamp(candidate))
            fix_time = parse_iso_timestamp(timestamp)
            if position is None or fix_time is None or fix_time > now:
                continue

            verified_fix = self._normalize_verified_fix(device_id, candidate)
            if verified_fix:
                timestamp = verified_fix["timestamp"]
                fix_time = parse_iso_timestamp(timestamp)
                latitude, longitude = verified_fix["latitude"], verified_fix["longitude"]
            else:
                latitude, longitude = position

            age_seconds = (now - fix_time).total_seconds()
            telemetry_fields = self._location_telemetry_fields(candidate)
            known_locations.append((
                fix_time,
                bool(verified_fix),
                {
                    "latitude": round(latitude, 6),
                    "longitude": round(longitude, 6),
                    "timestamp": timestamp,
                    "age_seconds": max(0, int(age_seconds)),
                    "accuracy_m": telemetry_fields["accuracy_m"],
                    "speed": telemetry_fields["speed"],
                    "speed_kmh": telemetry_fields["speed_kmh"],
                    "heading": telemetry_fields["heading"],
                    "bearing": telemetry_fields["bearing"],
                    "coordinate_source": "last_known",
                    "received_at": normalize_iso_timestamp(candidate.get("received_at")),
                    "maps_url": f"https://maps.google.com/?q={latitude:.6f},{longitude:.6f}",
                    "gps_source": (
                        verified_fix.get("gps_source")
                        if verified_fix
                        else candidate.get("gps_source") or candidate.get("source")
                    ),
                    "label": "Last known",
                    "is_current": False,
                    "is_stale": age_seconds > GPS_FIX_MAX_AGE_SECONDS,
                    "verified": bool(verified_fix)
                }
            ))

        if not known_locations:
            return None
        return max(known_locations, key=lambda item: (item[0], item[1]))[2]

    def get_telemetry_history(self, device_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        self._sync_firestore_telemetry_history(device_id, limit)
        conn = sqlite3.connect(str(LOCAL_DB))
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                """
                SELECT * FROM telemetry
                WHERE device_id = ?
                  AND gps_valid = 1
                  AND gps_source IN ('watch_frame', 'watch_tcp', 'watch_udp', 'gps_ingest_api')
                  AND latitude BETWEEN -90 AND 90
                  AND longitude BETWEEN -180 AND 180
                  AND NOT (latitude = 0 AND longitude = 0)
                  AND julianday(timestamp) IS NOT NULL
                  AND julianday(timestamp) <= julianday('now')
                ORDER BY julianday(timestamp) DESC, id DESC
                LIMIT ?
                """,
                (device_id, limit)
            ).fetchall()
            records = [dict(row) for row in rows]
        finally:
            conn.close()

        unique_records = []
        seen = set()
        for record in records:
            timestamp = normalize_iso_timestamp(record.get("timestamp"))
            position = valid_coordinate_pair(
                record.get("latitude", record.get("lat")),
                record.get("longitude", record.get("lon", record.get("lng")))
            )
            key = (timestamp, position)
            if key in seen:
                continue
            seen.add(key)
            record.setdefault("heading", record.get("bearing", 0.0))
            record.setdefault("bearing", record.get("heading", 0.0))
            record.setdefault("direction_deg", record.get("heading", 0.0))
            record.setdefault("speed", record.get("speed_kmh", 0.0))
            unique_records.append(record)

        unique_records.sort(
            key=lambda record: (
                parse_iso_timestamp(
                    record.get("gps_time") or record.get("timestamp")
                )
                or datetime.min.replace(tzinfo=timezone.utc),
                parse_iso_timestamp(record.get("received_at"))
                or datetime.min.replace(tzinfo=timezone.utc),
            ),
            reverse=True
        )
        return unique_records[:limit]

    # --- ALERTS ---
    def save_alert(self, alert_data: Dict[str, Any]):
        try:
            conn = sqlite3.connect(str(LOCAL_DB))
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO alerts (
                    device_id, timestamp, alert_type, message, latitude, longitude,
                    status, acknowledged_by, resolved_by, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                alert_data["device_id"],
                alert_data["timestamp"],
                alert_data["alert_type"],
                alert_data["message"],
                alert_data.get("latitude"),
                alert_data.get("longitude"),
                alert_data.get("status", "NEW"),
                alert_data.get("acknowledged_by"),
                alert_data.get("resolved_by"),
                alert_data.get("notes")
            ))
            conn.commit()
            conn.close()
        except Exception as e:
            app_logger.error(f"SQLite save_alert error: {e}")

        if is_firebase_active():
            def _bg_sync():
                try:
                    db = get_firestore_client()
                    if db:
                        db.collection("alerts").add(alert_data)
                except Exception as e:
                    app_logger.error(f"Firestore save_alert error: {e}")
            threading.Thread(target=_bg_sync, daemon=True).start()

    def get_recent_alerts(self, limit: int = 20) -> List[Dict[str, Any]]:
        conn = sqlite3.connect(str(LOCAL_DB))
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        rows = cur.execute("SELECT * FROM alerts ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        conn.close()
        formatted = []
        for r in rows:
            d = dict(r)
            d.setdefault("imei", d.get("device_id"))
            d.setdefault("device", d.get("device_id"))
            d.setdefault("date", d.get("timestamp"))
            d.setdefault("created_at", d.get("timestamp"))
            d.setdefault("type", d.get("alert_type"))
            d.setdefault("severity", "warning" if d.get("alert_type") != "SOS" else "critical")
            formatted.append(d)
        return formatted

    # --- JOURNEYS ---
    def save_journey(self, journey_data: Dict[str, Any]):
        """
        Saves journey metadata and route coordinates to SQLite and Firebase Firestore.
        Synchronizes to both top-level 'journeys' and 'devices/{device_id}/journeys'.
        """
        j_id = journey_data["journey_id"]
        dev_id = journey_data["device_id"]
        status = journey_data.get("status", "ACTIVE")
        st = journey_data.get("start_time")
        et = journey_data.get("end_time")

        sl = journey_data.get("start_location") or {}
        el = journey_data.get("end_location") or {}
        s_lat = sl.get("latitude")
        s_lon = sl.get("longitude")
        e_lat = el.get("latitude")
        e_lon = el.get("longitude")

        dist_km = float(journey_data.get("total_distance_km", 0.0))
        dur_sec = int(journey_data.get("duration_seconds", 0))
        dur_fmt = str(journey_data.get("duration_formatted", "00:00:00"))
        pts_count = int(journey_data.get("points_count", len(journey_data.get("route_coordinates", []))))
        route_json = json.dumps(journey_data.get("route_coordinates", []))
        created_at = journey_data.get("created_at") or current_iso_timestamp()
        updated_at = journey_data.get("updated_at") or current_iso_timestamp()

        # 1. Local SQLite Backup
        try:
            conn = sqlite3.connect(str(LOCAL_DB))
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO journeys (
                    journey_id, device_id, status, start_time, end_time,
                    start_latitude, start_longitude, end_latitude, end_longitude,
                    total_distance_km, duration_seconds, duration_formatted,
                    points_count, route_coordinates_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(journey_id) DO UPDATE SET
                    status = excluded.status,
                    end_time = excluded.end_time,
                    start_latitude = COALESCE(excluded.start_latitude, journeys.start_latitude),
                    start_longitude = COALESCE(excluded.start_longitude, journeys.start_longitude),
                    end_latitude = COALESCE(excluded.end_latitude, journeys.end_latitude),
                    end_longitude = COALESCE(excluded.end_longitude, journeys.end_longitude),
                    total_distance_km = excluded.total_distance_km,
                    duration_seconds = excluded.duration_seconds,
                    duration_formatted = excluded.duration_formatted,
                    points_count = excluded.points_count,
                    route_coordinates_json = excluded.route_coordinates_json,
                    updated_at = excluded.updated_at
            """, (
                j_id, dev_id, status, st, et,
                s_lat, s_lon, e_lat, e_lon,
                dist_km, dur_sec, dur_fmt,
                pts_count, route_json, created_at, updated_at
            ))
            conn.commit()
            conn.close()
        except Exception as e:
            app_logger.error(f"SQLite save_journey error: {e}")

        # 2. Async Firestore Sync
        if is_firebase_active():
            def _bg_sync():
                try:
                    db = get_firestore_client()
                    if db:
                        sync_payload = {
                            **journey_data,
                            "id": j_id,
                            "journey_id": j_id,
                            "device_id": dev_id,
                            "status": status,
                            "start_time": st,
                            "end_time": et,
                            "total_distance_km": dist_km,
                            "duration_seconds": dur_sec,
                            "duration_formatted": dur_fmt,
                            "points_count": pts_count,
                            "created_at": created_at,
                            "updated_at": updated_at
                        }
                        # Save in top-level collection and device subcollection
                        db.collection("journeys").document(j_id).set(sync_payload, merge=True)
                        db.collection("devices").document(dev_id).collection("journeys").document(j_id).set(sync_payload, merge=True)
                        # Also update active_journey_id on parent device
                        db.collection("devices").document(dev_id).set({
                            "active_journey_id": j_id if status == "ACTIVE" else None
                        }, merge=True)
                except Exception as e:
                    app_logger.error(f"Firestore save_journey error for {j_id}: {e}")

            threading.Thread(target=_bg_sync, daemon=True).start()

    def get_active_journey(self, device_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves currently ACTIVE journey for a device."""
        if is_firebase_active():
            try:
                db = get_firestore_client()
                if db:
                    dev_doc = db.collection("devices").document(device_id).get(timeout=2.0)
                    if dev_doc.exists:
                        active_id = dev_doc.to_dict().get("active_journey_id")
                        if active_id:
                            j_doc = db.collection("journeys").document(active_id).get(timeout=2.0)
                            if j_doc.exists:
                                d = j_doc.to_dict()
                                d["id"] = j_doc.id
                                return d
            except Exception as e:
                app_logger.error(f"Firestore get_active_journey failed: {e}")

        conn = sqlite3.connect(str(LOCAL_DB))
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        row = cur.execute(
            "SELECT * FROM journeys WHERE device_id = ? AND status = 'ACTIVE' ORDER BY created_at DESC LIMIT 1",
            (device_id,)
        ).fetchone()
        conn.close()
        if row:
            d = dict(row)
            try:
                d["route_coordinates"] = json.loads(d.get("route_coordinates_json") or "[]")
            except Exception:
                d["route_coordinates"] = []
            if d.get("start_latitude") is not None and d.get("start_longitude") is not None:
                d["start_location"] = {
                    "latitude": d["start_latitude"],
                    "longitude": d["start_longitude"],
                    "timestamp": d.get("start_time")
                }
            if d.get("end_latitude") is not None and d.get("end_longitude") is not None:
                d["end_location"] = {
                    "latitude": d["end_latitude"],
                    "longitude": d["end_longitude"],
                    "timestamp": d.get("end_time")
                }
            return d
        return None

    def get_device_journeys(self, device_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Retrieves all journeys for a device in reverse chronological order."""
        if is_firebase_active():
            try:
                db = get_firestore_client()
                if db:
                    docs = db.collection("devices").document(device_id).collection("journeys")\
                        .order_by("created_at", direction="DESCENDING")\
                        .limit(limit)\
                        .stream(timeout=2.0)
                    results = []
                    for d in docs:
                        data = d.to_dict()
                        data["id"] = d.id
                        results.append(data)
                    if results:
                        return results
            except Exception as e:
                app_logger.error(f"Firestore get_device_journeys failed: {e}")

        conn = sqlite3.connect(str(LOCAL_DB))
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        rows = cur.execute(
            "SELECT * FROM journeys WHERE device_id = ? ORDER BY created_at DESC LIMIT ?",
            (device_id, limit)
        ).fetchall()
        conn.close()
        results = []
        for r in rows:
            d = dict(r)
            try:
                d["route_coordinates"] = json.loads(d.get("route_coordinates_json") or "[]")
            except Exception:
                d["route_coordinates"] = []
            if d.get("start_latitude") is not None and d.get("start_longitude") is not None:
                d["start_location"] = {
                    "latitude": d["start_latitude"],
                    "longitude": d["start_longitude"],
                    "timestamp": d.get("start_time")
                }
            if d.get("end_latitude") is not None and d.get("end_longitude") is not None:
                d["end_location"] = {
                    "latitude": d["end_latitude"],
                    "longitude": d["end_longitude"],
                    "timestamp": d.get("end_time")
                }
            results.append(d)
        return results

    def get_journey(self, journey_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves a single journey by ID."""
        if is_firebase_active():
            try:
                db = get_firestore_client()
                if db:
                    doc = db.collection("journeys").document(journey_id).get(timeout=2.0)
                    if doc.exists:
                        data = doc.to_dict()
                        data["id"] = doc.id
                        return data
            except Exception as e:
                app_logger.error(f"Firestore get_journey failed: {e}")

        conn = sqlite3.connect(str(LOCAL_DB))
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        row = cur.execute("SELECT * FROM journeys WHERE journey_id = ?", (journey_id,)).fetchone()
        conn.close()
        if row:
            d = dict(row)
            try:
                d["route_coordinates"] = json.loads(d.get("route_coordinates_json") or "[]")
            except Exception:
                d["route_coordinates"] = []
            if d.get("start_latitude") is not None and d.get("start_longitude") is not None:
                d["start_location"] = {
                    "latitude": d["start_latitude"],
                    "longitude": d["start_longitude"],
                    "timestamp": d.get("start_time")
                }
            if d.get("end_latitude") is not None and d.get("end_longitude") is not None:
                d["end_location"] = {
                    "latitude": d["end_latitude"],
                    "longitude": d["end_longitude"],
                    "timestamp": d.get("end_time")
                }
            return d
        return None


firestore_service = FirestoreService()
