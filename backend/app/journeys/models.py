"""
Database Models & Persistence Layer for Journey Management & Route Monitoring
Provides thread-safe SQLite connection handling, schema migration,
and CRUD operations for journeys, journey_routes, journey_gps_points, and journey_events.
"""

import hashlib
import json
import sqlite3
import threading
import uuid
from pathlib import Path
from typing import Dict, Any, List, Optional
from ..utils.logger import app_logger
from ..utils.datetime_utils import current_iso_timestamp

BACKEND_DIR = Path(__file__).resolve().parent.parent.parent
LOCAL_DB = BACKEND_DIR / "custody_watch.db"
_db_lock = threading.RLock()


def get_db_connection(db_path: Optional[Path] = None) -> sqlite3.Connection:
    """Returns a SQLite connection with row access by column name."""
    target_db = db_path or LOCAL_DB
    conn = sqlite3.connect(str(target_db), timeout=20.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def init_journey_database(db_path: Optional[Path] = None) -> None:
    """
    Initializes or extends existing database tables for journey management and route monitoring.
    Safely adds missing columns to 'journeys' and creates 'journey_routes',
    'journey_gps_points', and 'journey_events' tables without dropping any data.
    """
    with _db_lock:
        conn = get_db_connection(db_path)
        cur = conn.cursor()

        # 1. Ensure journeys table exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS journeys (
                journey_id TEXT PRIMARY KEY,
                device_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'SCHEDULED',
                start_time TEXT,
                end_time TEXT,
                start_latitude REAL,
                start_longitude REAL,
                end_latitude REAL,
                end_longitude REAL,
                total_distance_km REAL DEFAULT 0.0,
                duration_seconds INTEGER DEFAULT 0,
                duration_formatted TEXT DEFAULT '00:00:00',
                points_count INTEGER DEFAULT 0,
                route_coordinates_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)

        # Safely migrate new columns into journeys table if missing
        existing_cols = {
            row["name"] for row in cur.execute("PRAGMA table_info(journeys)").fetchall()
        }

        migration_columns = [
            ("origin", "TEXT"),
            ("origin_name", "TEXT"),
            ("destination", "TEXT"),
            ("destination_name", "TEXT"),
            ("scheduled_departure", "TEXT"),
            ("scheduled_arrival", "TEXT"),
            ("actual_departure", "TEXT"),
            ("actual_arrival", "TEXT"),
            ("selected_route_id", "TEXT"),
            ("route_corridor_meters", "REAL DEFAULT 75.0"),
        ]

        for col_name, col_type in migration_columns:
            if col_name not in existing_cols:
                try:
                    cur.execute(f"ALTER TABLE journeys ADD COLUMN {col_name} {col_type}")
                    app_logger.info(f"[DB MIGRATION] Added column '{col_name}' to 'journeys' table")
                except Exception as exc:
                    app_logger.warning(f"[DB MIGRATION] Column addition '{col_name}' note: {exc}")

        # 2. Create journey_routes table
        # Route ids such as "route_a" are reused by every journey, so the key must be
        # (journey_id, id); a global id key let a new journey overwrite older routes.
        journey_routes_ddl = """
            CREATE TABLE IF NOT EXISTS {name} (
                id TEXT NOT NULL,
                journey_id TEXT NOT NULL,
                route_name TEXT NOT NULL,
                route_geometry TEXT NOT NULL,
                distance_meters REAL,
                duration_seconds REAL,
                is_selected INTEGER DEFAULT 0,
                approved_by TEXT,
                approved_at TEXT,
                safety_assessment TEXT,
                created_at TEXT NOT NULL,
                PRIMARY KEY (journey_id, id)
            )
        """
        route_pk = [
            row["name"] for row in cur.execute("PRAGMA table_info(journey_routes)").fetchall()
            if row["pk"]
        ]
        if route_pk == ["id"]:
            cur.execute(journey_routes_ddl.format(name="journey_routes_v2"))
            cur.execute("INSERT OR IGNORE INTO journey_routes_v2 SELECT id, journey_id, route_name, route_geometry, "
                        "distance_meters, duration_seconds, is_selected, approved_by, approved_at, "
                        "safety_assessment, created_at FROM journey_routes")
            cur.execute("DROP TABLE journey_routes")
            cur.execute("ALTER TABLE journey_routes_v2 RENAME TO journey_routes")
            app_logger.info("[DB MIGRATION] journey_routes key changed to (journey_id, id); rows preserved")
        cur.execute(journey_routes_ddl.format(name="journey_routes"))
        cur.execute("CREATE INDEX IF NOT EXISTS idx_journey_routes_journey_id ON journey_routes(journey_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_journey_routes_selected ON journey_routes(journey_id, is_selected)")

        # 3. Create journey_gps_points table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS journey_gps_points (
                id TEXT PRIMARY KEY,
                journey_id TEXT NOT NULL,
                device_id TEXT NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                gps_timestamp TEXT NOT NULL,
                accuracy_meters REAL DEFAULT 0.0,
                speed_kmh REAL DEFAULT 0.0,
                distance_from_route_meters REAL DEFAULT 0.0,
                on_route INTEGER DEFAULT 1,
                created_at TEXT NOT NULL
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_journey_gps_points_journey ON journey_gps_points(journey_id, gps_timestamp)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_journey_gps_points_device ON journey_gps_points(device_id, gps_timestamp)")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_journey_gps_points_dedup ON journey_gps_points(journey_id, gps_timestamp)")

        # 4. Create journey_events table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS journey_events (
                id TEXT PRIMARY KEY,
                journey_id TEXT NOT NULL,
                device_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                latitude REAL,
                longitude REAL,
                event_time TEXT NOT NULL,
                details TEXT,
                created_at TEXT NOT NULL
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_journey_events_journey ON journey_events(journey_id, event_time)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_journey_events_device ON journey_events(device_id, event_time)")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_journey_events_dedup ON journey_events(journey_id, event_type, event_time)")

        conn.commit()
        conn.close()
        app_logger.info("[DB INIT] Journey and route monitoring tables initialized successfully")


def create_journey_record(journey_data: Dict[str, Any], db_path: Optional[Path] = None) -> Dict[str, Any]:
    """Persists a new journey into SQLite."""
    with _db_lock:
        conn = get_db_connection(db_path)
        cur = conn.cursor()
        now_iso = current_iso_timestamp()

        journey_id = journey_data.get("journey_id") or journey_data.get("id") or f"journey_{uuid.uuid4().hex[:12]}"
        device_id = str(journey_data["device_id"]).strip()
        status = (journey_data.get("status") or "SCHEDULED").upper()

        cur.execute("""
            INSERT INTO journeys (
                journey_id, device_id, status, start_time, end_time,
                start_latitude, start_longitude, end_latitude, end_longitude,
                total_distance_km, duration_seconds, duration_formatted, points_count,
                route_coordinates_json, created_at, updated_at,
                origin, origin_name, destination, destination_name,
                scheduled_departure, scheduled_arrival, actual_departure, actual_arrival,
                selected_route_id, route_corridor_meters
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            journey_id,
            device_id,
            status,
            journey_data.get("start_time"),
            journey_data.get("end_time"),
            journey_data.get("start_latitude"),
            journey_data.get("start_longitude"),
            journey_data.get("end_latitude"),
            journey_data.get("end_longitude"),
            float(journey_data.get("total_distance_km") or 0.0),
            int(journey_data.get("duration_seconds") or 0),
            journey_data.get("duration_formatted") or "00:00:00",
            int(journey_data.get("points_count") or 0),
            journey_data.get("route_coordinates_json") or "[]",
            journey_data.get("created_at") or now_iso,
            journey_data.get("updated_at") or now_iso,
            journey_data.get("origin"),
            journey_data.get("origin_name"),
            journey_data.get("destination"),
            journey_data.get("destination_name"),
            journey_data.get("scheduled_departure"),
            journey_data.get("scheduled_arrival"),
            journey_data.get("actual_departure"),
            journey_data.get("actual_arrival"),
            journey_data.get("selected_route_id"),
            float(journey_data.get("route_corridor_meters") or 75.0),
        ))
        conn.commit()

        row = cur.execute("SELECT * FROM journeys WHERE journey_id = ?", (journey_id,)).fetchone()
        conn.close()
        return dict(row) if row else journey_data


def get_journey_by_id(journey_id: str, db_path: Optional[Path] = None) -> Optional[Dict[str, Any]]:
    """Retrieves a journey by journey_id."""
    conn = get_db_connection(db_path)
    cur = conn.cursor()
    row = cur.execute("SELECT * FROM journeys WHERE journey_id = ?", (journey_id,)).fetchone()
    conn.close()
    if not row:
        return None
    d = dict(row)
    d["id"] = d["journey_id"]
    if d.get("route_coordinates_json"):
        try:
            d["route_coordinates"] = json.loads(d["route_coordinates_json"])
        except Exception:
            d["route_coordinates"] = []
    else:
        d["route_coordinates"] = []
    return d


def get_active_journey_for_device(device_id: str, db_path: Optional[Path] = None) -> Optional[Dict[str, Any]]:
    """Retrieves the currently ACTIVE or SCHEDULED journey for a device."""
    conn = get_db_connection(db_path)
    cur = conn.cursor()
    row = cur.execute("""
        SELECT * FROM journeys
        WHERE device_id = ? AND status IN ('ACTIVE', 'SCHEDULED', 'PAUSED')
        ORDER BY created_at DESC LIMIT 1
    """, (str(device_id).strip(),)).fetchone()
    conn.close()
    if not row:
        return None
    d = dict(row)
    d["id"] = d["journey_id"]
    if d.get("route_coordinates_json"):
        try:
            d["route_coordinates"] = json.loads(d["route_coordinates_json"])
        except Exception:
            d["route_coordinates"] = []
    else:
        d["route_coordinates"] = []
    return d


ROUTE_MONITORED_OPEN_STATUSES = ("ACTIVE", "SCHEDULED", "PAUSED")


def get_route_journey_for_timestamp(
    device_id: str, timestamp: Any, db_path: Optional[Path] = None
) -> Optional[Dict[str, Any]]:
    """
    Returns the route-monitored journey (one with an approved route) whose time window
    contains `timestamp`. Open journeys (ACTIVE/SCHEDULED/PAUSED) cover everything from
    their start onward; closed journeys cover [start, end_time]. Comparison uses parsed
    UTC datetimes, never raw strings.
    """
    from ..utils.datetime_utils import parse_to_utc_datetime

    point_dt = parse_to_utc_datetime(timestamp)
    if point_dt is None:
        return None
    conn = get_db_connection(db_path)
    try:
        rows = conn.execute(
            """
            SELECT * FROM journeys
            WHERE device_id = ? AND selected_route_id IS NOT NULL AND TRIM(selected_route_id) <> ''
              AND status <> 'CANCELLED'
            ORDER BY created_at DESC
            """,
            (str(device_id).strip(),),
        ).fetchall()
    finally:
        conn.close()
    for row in rows:
        journey = dict(row)
        start_dt = parse_to_utc_datetime(
            journey.get("actual_departure") or journey.get("start_time") or journey.get("created_at")
        )
        if start_dt is None or point_dt < start_dt:
            continue
        if str(journey.get("status") or "").upper() in ROUTE_MONITORED_OPEN_STATUSES:
            return journey
        end_dt = parse_to_utc_datetime(journey.get("end_time"))
        if end_dt is not None and point_dt <= end_dt:
            return journey
    return None


def update_journey_record(journey_id: str, updates: Dict[str, Any], db_path: Optional[Path] = None) -> Optional[Dict[str, Any]]:
    """Updates fields on a journey."""
    with _db_lock:
        conn = get_db_connection(db_path)
        cur = conn.cursor()
        existing = cur.execute("SELECT * FROM journeys WHERE journey_id = ?", (journey_id,)).fetchone()
        if not existing:
            conn.close()
            return None

        now_iso = current_iso_timestamp()
        updates["updated_at"] = now_iso

        fields = []
        values = []
        for k, v in updates.items():
            if k in ("journey_id", "id"):
                continue
            fields.append(f"{k} = ?")
            values.append(v)

        values.append(journey_id)
        cur.execute(f"UPDATE journeys SET {', '.join(fields)} WHERE journey_id = ?", values)
        conn.commit()

        updated = cur.execute("SELECT * FROM journeys WHERE journey_id = ?", (journey_id,)).fetchone()
        conn.close()
        if not updated:
            return None
        d = dict(updated)
        d["id"] = d["journey_id"]
        return d


def list_journeys_records(
    status: Optional[str] = None,
    device_id: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    db_path: Optional[Path] = None
) -> List[Dict[str, Any]]:
    """Queries journeys with filtering, strict time-deduplication, and pagination."""
    conn = get_db_connection(db_path)
    cur = conn.cursor()
    query = "SELECT * FROM journeys WHERE 1=1"
    params = []
    if status:
        query += " AND status = ?"
        params.append(status.upper())
    if device_id:
        query += " AND device_id = ?"
        params.append(str(device_id).strip())

    # Query with larger limit to account for time-deduplicated entries
    query += " ORDER BY julianday(start_time) DESC, created_at DESC LIMIT ? OFFSET ?"
    params.extend([limit * 2, offset])

    rows = cur.execute(query, params).fetchall()
    conn.close()
    results = []
    seen_time_signatures = set()
    for r in rows:
        d = dict(r)
        d["id"] = d["journey_id"]
        # Enforce strict deduplication by start time / created time per device
        dev = str(d.get("device_id") or "")
        st = d.get("start_time") or d.get("created_at")
        sig = f"{dev}_{st}" if st else d["id"]
        if sig in seen_time_signatures:
            continue
        seen_time_signatures.add(sig)
        results.append(d)
        if len(results) >= limit:
            break
    return results


def save_journey_route(
    journey_id: str,
    route_name: str,
    geometry: Dict[str, Any],
    distance_meters: Optional[float] = None,
    duration_seconds: Optional[float] = None,
    safety_assessment: Optional[Any] = None,
    is_selected: bool = False,
    route_id: Optional[str] = None,
    db_path: Optional[Path] = None
) -> Dict[str, Any]:
    """Persists a route alternative for a journey."""
    with _db_lock:
        conn = get_db_connection(db_path)
        cur = conn.cursor()
        rid = route_id or f"route_{uuid.uuid4().hex[:10]}"
        now_iso = current_iso_timestamp()

        geom_str = json.dumps(geometry) if isinstance(geometry, dict) else str(geometry)
        safety_str = json.dumps(safety_assessment) if isinstance(safety_assessment, (dict, list)) else (safety_assessment or "")

        cur.execute("""
            INSERT OR REPLACE INTO journey_routes (
                id, journey_id, route_name, route_geometry, distance_meters,
                duration_seconds, is_selected, approved_by, approved_at,
                safety_assessment, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            rid,
            journey_id,
            route_name,
            geom_str,
            distance_meters,
            duration_seconds,
            1 if is_selected else 0,
            None,
            None,
            safety_str,
            now_iso
        ))
        conn.commit()
        row = cur.execute(
            "SELECT * FROM journey_routes WHERE id = ? AND journey_id = ?", (rid, journey_id)
        ).fetchone()
        conn.close()
        d = dict(row)
        d["route_id"] = d["id"]
        try:
            d["geometry"] = json.loads(d["route_geometry"])
        except Exception:
            d["geometry"] = {}
        try:
            d["safety_assessment"] = json.loads(d["safety_assessment"]) if d.get("safety_assessment") else {}
        except Exception:
            pass
        return d


def get_journey_routes(journey_id: str, db_path: Optional[Path] = None) -> List[Dict[str, Any]]:
    """Retrieves all route options for a journey."""
    conn = get_db_connection(db_path)
    cur = conn.cursor()
    rows = cur.execute("SELECT * FROM journey_routes WHERE journey_id = ? ORDER BY is_selected DESC, distance_meters ASC", (journey_id,)).fetchall()
    conn.close()
    routes = []
    for r in rows:
        d = dict(r)
        d["route_id"] = d["id"]
        d["is_selected"] = bool(d["is_selected"])
        try:
            d["geometry"] = json.loads(d["route_geometry"])
        except Exception:
            d["geometry"] = {}
        try:
            d["safety_assessment"] = json.loads(d["safety_assessment"]) if d.get("safety_assessment") else {}
        except Exception:
            pass
        routes.append(d)
    return routes


def select_journey_route(
    journey_id: str,
    route_id: str,
    approved_by: str = "Officer",
    db_path: Optional[Path] = None
) -> Optional[Dict[str, Any]]:
    """Marks a route as the approved/selected route for a journey."""
    with _db_lock:
        conn = get_db_connection(db_path)
        cur = conn.cursor()

        # Check route belongs to this journey
        route_row = cur.execute("SELECT * FROM journey_routes WHERE id = ? AND journey_id = ?", (route_id, journey_id)).fetchone()
        if not route_row:
            # A route id from another journey must never be approved for this one.
            conn.close()
            return None

        now_iso = current_iso_timestamp()

        # Reset selection on all routes for this journey
        cur.execute("UPDATE journey_routes SET is_selected = 0 WHERE journey_id = ?", (journey_id,))

        # Set selection on the target route
        cur.execute("""
            UPDATE journey_routes
            SET is_selected = 1, approved_by = ?, approved_at = ?
            WHERE id = ? AND journey_id = ?
        """, (approved_by, now_iso, route_id, journey_id))

        # Update journeys table
        cur.execute("UPDATE journeys SET selected_route_id = ?, updated_at = ? WHERE journey_id = ?", (route_id, now_iso, journey_id))
        conn.commit()

        updated_route = cur.execute(
            "SELECT * FROM journey_routes WHERE id = ? AND journey_id = ?", (route_id, journey_id)
        ).fetchone()
        conn.close()
        if not updated_route:
            return None
        d = dict(updated_route)
        d["route_id"] = d["id"]
        d["is_selected"] = True
        try:
            d["geometry"] = json.loads(d["route_geometry"])
        except Exception:
            d["geometry"] = {}
        return d


def get_selected_route_for_journey(journey_id: str, db_path: Optional[Path] = None) -> Optional[Dict[str, Any]]:
    """Retrieves the approved selected route for a journey."""
    conn = get_db_connection(db_path)
    cur = conn.cursor()
    row = cur.execute("SELECT * FROM journey_routes WHERE journey_id = ? AND is_selected = 1", (journey_id,)).fetchone()
    conn.close()
    if not row:
        return None
    d = dict(row)
    d["route_id"] = d["id"]
    d["is_selected"] = True
    try:
        d["geometry"] = json.loads(d["route_geometry"])
    except Exception:
        d["geometry"] = {}
    try:
        d["safety_assessment"] = json.loads(d["safety_assessment"]) if d.get("safety_assessment") else {}
    except Exception:
        pass
    return d


def record_journey_gps_point(point_data: Dict[str, Any], db_path: Optional[Path] = None) -> Dict[str, Any]:
    """Persists a verified GPS reading along a journey, with strict deduplication."""
    with _db_lock:
        conn = get_db_connection(db_path)
        cur = conn.cursor()
        jid_val = str(point_data["journey_id"]).strip()
        ts_val = str(point_data["gps_timestamp"]).strip()
        lat_val = float(point_data["latitude"])
        lon_val = float(point_data["longitude"])

        if point_data.get("id"):
            pid = str(point_data["id"])
        else:
            pid = hashlib.sha256(f"{jid_val}|{ts_val}|{lat_val:.6f}|{lon_val:.6f}".encode("utf-8")).hexdigest()[:24]

        now_iso = current_iso_timestamp()

        cur.execute("""
            INSERT OR IGNORE INTO journey_gps_points (
                id, journey_id, device_id, latitude, longitude,
                gps_timestamp, accuracy_meters, speed_kmh,
                distance_from_route_meters, on_route, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            pid,
            jid_val,
            point_data["device_id"],
            lat_val,
            lon_val,
            ts_val,
            float(point_data.get("accuracy_meters") or 0.0),
            float(point_data.get("speed_kmh") or 0.0),
            float(point_data.get("distance_from_route_meters") or 0.0),
            1 if point_data.get("on_route", True) else 0,
            now_iso
        ))
        conn.commit()
        conn.close()
        point_data["id"] = pid
        return point_data


def get_journey_gps_points(
    journey_id: str,
    since: Optional[str] = None,
    limit: int = 2000,
    db_path: Optional[Path] = None
) -> List[Dict[str, Any]]:
    """Retrieves recorded GPS points for a journey with strict timestamp deduplication."""
    conn = get_db_connection(db_path)
    cur = conn.cursor()
    if since:
        rows = cur.execute("""
            SELECT * FROM journey_gps_points
            WHERE journey_id = ? AND gps_timestamp > ?
            ORDER BY gps_timestamp ASC LIMIT ?
        """, (journey_id, since, limit)).fetchall()
    else:
        rows = cur.execute("""
            SELECT * FROM journey_gps_points
            WHERE journey_id = ?
            ORDER BY gps_timestamp ASC LIMIT ?
        """, (journey_id, limit)).fetchall()
    conn.close()
    pts = []
    seen_timestamps = set()
    for r in rows:
        d = dict(r)
        ts = d.get("gps_timestamp")
        # Ensure no duplicate timestamps are ever passed
        if ts and ts in seen_timestamps:
            continue
        if ts:
            seen_timestamps.add(ts)
        d["on_route"] = bool(d["on_route"])
        d["gps_valid"] = True
        d["gps_fix"] = True
        d["location_is_current"] = True
        d["valid"] = True
        d["timestamp"] = ts
        d["gps_time"] = ts
        pts.append(d)
    return pts


def record_journey_event(event_data: Dict[str, Any], db_path: Optional[Path] = None) -> Dict[str, Any]:
    """Persists a journey lifecycle or deviation event, with strict deduplication."""
    with _db_lock:
        conn = get_db_connection(db_path)
        cur = conn.cursor()
        jid_val = str(event_data["journey_id"]).strip()
        etype_val = str(event_data["event_type"]).strip()
        etime_val = str(event_data.get("event_time") or current_iso_timestamp()).strip()

        if event_data.get("id"):
            eid = str(event_data["id"])
        else:
            eid = hashlib.sha256(f"{jid_val}|{etype_val}|{etime_val}".encode("utf-8")).hexdigest()[:24]

        now_iso = current_iso_timestamp()

        details = event_data.get("details")
        if isinstance(details, (dict, list)):
            details_str = json.dumps(details)
        else:
            details_str = str(details or "")

        cur.execute("""
            INSERT OR IGNORE INTO journey_events (
                id, journey_id, device_id, event_type, latitude, longitude,
                event_time, details, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            eid,
            jid_val,
            event_data["device_id"],
            etype_val,
            event_data.get("latitude"),
            event_data.get("longitude"),
            etime_val,
            details_str,
            now_iso
        ))
        conn.commit()
        conn.close()
        event_data["id"] = eid
        return event_data


def get_journey_events(journey_id: str, limit: int = 500, db_path: Optional[Path] = None) -> List[Dict[str, Any]]:
    """Retrieves event history for a journey."""
    conn = get_db_connection(db_path)
    cur = conn.cursor()
    rows = cur.execute("""
        SELECT * FROM journey_events
        WHERE journey_id = ?
        ORDER BY event_time ASC LIMIT ?
    """, (journey_id, limit)).fetchall()
    conn.close()
    events = []
    for r in rows:
        d = dict(r)
        if d.get("details"):
            try:
                d["details"] = json.loads(d["details"])
            except Exception:
                pass
        events.append(d)
    return events
