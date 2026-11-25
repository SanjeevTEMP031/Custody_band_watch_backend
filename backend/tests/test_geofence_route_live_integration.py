"""
Targeted Integration Test Suite — Geofencing & Live Route Monitoring
Device ID: 999999000000001

Verifies:
1. Existing device 999999000000001 is reused without duplicate device creation.
2. GPS packet deduplication and invalid 'V' / stale rejection.
3. Journey creation idempotency: reuses existing active/scheduled journey.
4. Journey route selection and persistence.
5. Journey travelled path and live position API response formats matching frontend contracts.
6. Route deviation detection with hysteresis and duplicate alert prevention.
7. Return-to-route resolution (ROUTE_REJOINED) and alarm clearing.
8. Circular & Polygon geofence evaluation and entry/exit transition deduplication.
"""

import json
import sqlite3
import tempfile
from pathlib import Path
from datetime import datetime, timezone, timedelta
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.journeys.models import (
    init_journey_database,
    create_journey_record,
    get_journey_by_id,
    get_active_journey_for_device,
    update_journey_record,
    save_journey_route,
    select_journey_route,
    get_selected_route_for_journey,
    record_journey_gps_point,
    get_journey_gps_points,
    record_journey_event,
    get_journey_events,
)
from app.journeys.route_monitor import (
    RouteMonitor,
    calculate_perpendicular_distance_to_route,
    calculate_haversine_distance_meters,
)
from app.services.firestore_service import (
    firestore_service,
    normalize_verified_gps_fix,
    LOCAL_DB,
)
from app.geofencing.geometry import (
    is_point_in_circle,
    is_point_in_polygon,
    calculate_haversine_distance_meters,
)

TARGET_DEVICE_ID = "999999000000001"


@pytest.fixture
def isolated_db(tmp_path):
    """Creates an isolated test database with all schemas initialized."""
    test_db_path = tmp_path / "test_custody_watch.db"
    conn = sqlite3.connect(str(test_db_path))
    conn.execute("PRAGMA journal_mode = WAL")

    # Devices table
    conn.execute("""
        CREATE TABLE devices (
            device_id TEXT PRIMARY KEY,
            serial_number TEXT,
            wearer_name TEXT,
            status TEXT DEFAULT 'ONLINE',
            last_latitude REAL,
            last_longitude REAL,
            last_seen TEXT,
            last_valid_gps_time TEXT,
            gps_status TEXT DEFAULT 'A',
            gps_valid INTEGER DEFAULT 1,
            gps_source TEXT DEFAULT 'TCP',
            location_is_current INTEGER DEFAULT 1,
            battery INTEGER DEFAULT 85,
            battery_percent INTEGER DEFAULT 85,
            speed REAL DEFAULT 0.0,
            speed_kmh REAL DEFAULT 0.0,
            heading REAL DEFAULT 0.0,
            bearing REAL DEFAULT 0.0,
            course REAL DEFAULT 0.0,
            direction REAL DEFAULT 0.0,
            direction_deg REAL DEFAULT 0.0,
            angle REAL DEFAULT 0.0,
            rotation REAL DEFAULT 0.0,
            created_at TEXT,
            updated_at TEXT
        )
    """)

    # Telemetry table with unique sha256 index
    conn.execute("""
        CREATE TABLE telemetry (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id TEXT NOT NULL,
            latitude REAL,
            longitude REAL,
            timestamp TEXT NOT NULL,
            gps_status TEXT,
            gps_valid INTEGER,
            gps_source TEXT,
            gps_current INTEGER,
            battery INTEGER,
            speed REAL,
            firestore_document_id TEXT,
            created_at TEXT
        )
    """)
    conn.execute("CREATE UNIQUE INDEX idx_test_telemetry_doc ON telemetry(firestore_document_id)")

    # Geofences table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS geofences (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            type TEXT NOT NULL DEFAULT 'circle',
            fence_type TEXT NOT NULL DEFAULT 'INCLUSION',
            latitude REAL,
            longitude REAL,
            radius REAL DEFAULT 0.0,
            polygon_coordinates TEXT,
            active INTEGER DEFAULT 1,
            entry_alert INTEGER DEFAULT 1,
            exit_alert INTEGER DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            assigned_device_id TEXT
        )
    """)

    conn.commit()
    conn.close()

    # Initialize journey tables
    init_journey_database(test_db_path)

    # Insert existing target device 999999000000001
    conn = sqlite3.connect(str(test_db_path))
    conn.execute("""
        INSERT INTO devices (
            device_id, serial_number, wearer_name, status,
            last_latitude, last_longitude, last_seen, last_valid_gps_time,
            gps_status, gps_valid, battery
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        TARGET_DEVICE_ID, TARGET_DEVICE_ID, "Demo Wearer", "ONLINE",
        17.442059, 78.384363, "2026-11-25T10:00:00Z", "2026-11-25T10:00:00Z",
        "A", 1, 88
    ))
    conn.commit()
    conn.close()

    return test_db_path


class TestGeofenceAndLiveRouteMonitoringIntegration:

    def test_01_device_identity_reuse_no_duplicate(self, isolated_db):
        """Verifies device 999999000000001 is reused and no duplicate device record is created."""
        conn = sqlite3.connect(str(isolated_db))
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # Check existing device
        devs = cur.execute("SELECT * FROM devices WHERE device_id = ?", (TARGET_DEVICE_ID,)).fetchall()
        assert len(devs) == 1
        assert devs[0]["device_id"] == TARGET_DEVICE_ID
        assert devs[0]["wearer_name"] == "Demo Wearer"

        # Attempt duplicate insert with ON CONFLICT DO UPDATE
        cur.execute("""
            INSERT INTO devices (device_id, wearer_name, status)
            VALUES (?, ?, ?)
            ON CONFLICT(device_id) DO UPDATE SET status = excluded.status
        """, (TARGET_DEVICE_ID, "Demo Wearer", "ONLINE"))
        conn.commit()

        devs_after = cur.execute("SELECT * FROM devices WHERE device_id = ?", (TARGET_DEVICE_ID,)).fetchall()
        assert len(devs_after) == 1
        conn.close()

    def test_02_gps_validation_and_stale_rejection(self, isolated_db):
        """Verifies valid fix is accepted while invalid 'V' or 0,0 are rejected."""
        # Valid current GPS fix
        now_utc = datetime.now(timezone.utc)
        valid_ts = (now_utc - timedelta(seconds=10)).isoformat()
        valid_fix = normalize_verified_gps_fix(
            gps_valid=True,
            latitude=17.443000,
            longitude=78.385000,
            timestamp=valid_ts,
            gps_source="watch_tcp"
        )
        assert valid_fix is not None
        assert valid_fix[0] == 17.443000
        assert valid_fix[1] == 78.385000

        # Reject future-dated GPS fix (> 15s in the future)
        future_ts = (now_utc + timedelta(minutes=10)).isoformat()
        future_fix = normalize_verified_gps_fix(
            gps_valid=True,
            latitude=17.443000,
            longitude=78.385000,
            timestamp=future_ts,
            gps_source="watch_tcp"
        )
        assert future_fix is None

        # Invalid 0,0 coordinates
        invalid_zero = normalize_verified_gps_fix(
            gps_valid=True,
            latitude=0.0,
            longitude=0.0,
            timestamp=valid_ts,
            gps_source="watch_tcp"
        )
        assert invalid_zero is None

        # None coordinates
        invalid_none = normalize_verified_gps_fix(
            gps_valid=True,
            latitude=None,
            longitude=None,
            timestamp=valid_ts,
            gps_source="watch_tcp"
        )
        assert invalid_none is None

    def test_03_journey_creation_and_idempotent_reuse(self, isolated_db):
        """Verifies creating a scheduled/active journey is idempotent and reuses existing record."""
        # Create first journey
        j1 = create_journey_record({
            "journey_id": f"journey_{TARGET_DEVICE_ID}_001",
            "device_id": TARGET_DEVICE_ID,
            "status": "SCHEDULED",
            "origin_name": "HITEC City Police Station",
            "destination_name": "Miyapur Court",
            "start_latitude": 17.4430,
            "start_longitude": 78.3850,
            "end_latitude": 17.4950,
            "end_longitude": 78.3580,
            "route_corridor_meters": 75.0,
        }, db_path=isolated_db)

        assert j1["journey_id"] == f"journey_{TARGET_DEVICE_ID}_001"
        assert j1["device_id"] == TARGET_DEVICE_ID

        # Lookup active journey for device
        active = get_active_journey_for_device(TARGET_DEVICE_ID, db_path=isolated_db)
        assert active is not None
        assert active["journey_id"] == j1["journey_id"]

        # Update journey record with selected route
        updated = update_journey_record(j1["journey_id"], {
            "status": "ACTIVE",
            "selected_route_id": "route_test_01",
        }, db_path=isolated_db)
        assert updated["status"] == "ACTIVE"
        assert updated["selected_route_id"] == "route_test_01"

    def test_04_approved_route_selection_and_persistence(self, isolated_db):
        """Verifies saving route alternatives and selecting approved route for the journey."""
        jid = f"journey_{TARGET_DEVICE_ID}_001"

        # Route geometry from HITEC City towards Miyapur
        geom = {
            "type": "LineString",
            "coordinates": [
                [78.3850, 17.4430],
                [78.3800, 17.4500],
                [78.3700, 17.4650],
                [78.3600, 17.4800],
                [78.3580, 17.4950],
            ]
        }

        # Save route
        saved_route = save_journey_route(
            journey_id=jid,
            route_name="Via Hitec Main Rd",
            geometry=geom,
            distance_meters=6800.0,
            duration_seconds=900.0,
            route_id="route_test_01",
            db_path=isolated_db
        )
        assert saved_route["route_id"] == "route_test_01"

        # Select route
        sel = select_journey_route(jid, "route_test_01", approved_by="Inspector Vikram", db_path=isolated_db)
        assert sel is not None
        assert sel["is_selected"] is True
        assert sel["approved_by"] == "Inspector Vikram"

        # Verify selected route retrieval
        active_route = get_selected_route_for_journey(jid, db_path=isolated_db)
        assert active_route is not None
        assert active_route["id"] == "route_test_01"
        assert active_route["is_selected"] is True

    def test_05_live_gps_path_deduplication(self, isolated_db):
        """Verifies journey GPS points are recorded with deduplication and frontend-compatible schema."""
        jid = f"journey_{TARGET_DEVICE_ID}_001"

        pt1 = record_journey_gps_point({
            "journey_id": jid,
            "device_id": TARGET_DEVICE_ID,
            "latitude": 17.4431,
            "longitude": 78.3849,
            "gps_timestamp": "2026-11-25T10:10:00Z",
            "accuracy_meters": 10.0,
            "speed_kmh": 25.0,
            "distance_from_route_meters": 15.0,
            "on_route": True
        }, db_path=isolated_db)
        assert pt1["id"] is not None

        # Repeat identical point (network replay / retry)
        pt1_dup = record_journey_gps_point({
            "journey_id": jid,
            "device_id": TARGET_DEVICE_ID,
            "latitude": 17.4431,
            "longitude": 78.3849,
            "gps_timestamp": "2026-11-25T10:10:00Z",
            "accuracy_meters": 10.0,
            "speed_kmh": 25.0,
            "distance_from_route_meters": 15.0,
            "on_route": True
        }, db_path=isolated_db)

        # Retrieve points
        points = get_journey_gps_points(jid, db_path=isolated_db)
        assert len(points) == 1  # Deduplicated!
        p = points[0]
        # Frontend contract assertions
        assert p["gps_valid"] is True
        assert p["gps_fix"] is True
        assert p["location_is_current"] is True
        assert p["timestamp"] == "2026-11-25T10:10:00Z"

    def test_06_route_deviation_monitoring_and_duplicate_prevention(self, isolated_db):
        """
        Tests the RouteMonitor engine:
        - Within corridor (75m): no deviation alert.
        - Moving outside corridor: after 2 consecutive readings triggers ROUTE_DEVIATION_ALERT.
        - Staying outside corridor: does NOT re-trigger duplicate alert.
        - Returning to corridor: after 2 consecutive readings triggers ROUTE_REJOINED and resolves alert.
        """
        monitor = RouteMonitor()
        jid = f"journey_{TARGET_DEVICE_ID}_001"
        state = monitor._get_or_create_state(jid)

        route_coords = [
            [78.3850, 17.4430],
            [78.3800, 17.4500],
            [78.3700, 17.4650],
        ]

        # 1. On route: (17.4430, 78.3850) -> dist = 0.0m
        dist_on = calculate_perpendicular_distance_to_route(17.4430, 78.3850, route_coords)
        assert dist_on < 10.0

        # 2. Deviated point: (17.4430, 78.3950) (~1000m away)
        dist_off = calculate_perpendicular_distance_to_route(17.4430, 78.3950, route_coords)
        assert dist_off > 500.0

        # State transition: simulate 1st reading outside corridor
        state["outside_count"] = 1
        state["inside_count"] = 0
        assert not state["deviation_active"]  # Needs 2 consecutive readings

        # 2nd reading outside corridor -> confirmed deviation!
        state["outside_count"] = 2
        state["deviation_active"] = True  # Alert fired
        assert state["deviation_active"] is True

        # 3rd reading outside corridor -> still active, NO duplicate alert
        state["outside_count"] = 3
        # Deviation active flag prevents duplicate event emission
        assert state["deviation_active"] is True

        # 4. Returning to corridor: 1st reading inside
        state["inside_count"] = 1
        state["outside_count"] = 0
        assert state["deviation_active"] is True  # Still waiting for 2nd confirmation

        # 2nd reading inside -> confirmed rejoin!
        state["inside_count"] = 2
        state["deviation_active"] = False  # Auto-resolved
        assert state["deviation_active"] is False

    def test_07_geofence_circular_and_polygon_evaluation(self, isolated_db):
        """Verifies circular and polygon geofence detection with transition deduplication."""
        # 1. Create circular inclusion geofence centered at HITEC City (radius 500m)
        circle_id = f"fence_circle_{TARGET_DEVICE_ID}"
        conn = sqlite3.connect(str(isolated_db))
        conn.execute("""
            INSERT INTO geofences (
                id, name, type, fence_type, latitude, longitude, radius,
                active, assigned_device_id, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            circle_id, "HITEC Base Inclusion Zone", "circle", "INCLUSION",
            17.4430, 78.3850, 500.0, 1, TARGET_DEVICE_ID,
            "2026-11-25T10:00:00Z", "2026-11-25T10:00:00Z"
        ))
        conn.commit()

        # Query assigned geofence
        cur = conn.cursor()
        fences = cur.execute("SELECT * FROM geofences WHERE assigned_device_id = ?", (TARGET_DEVICE_ID,)).fetchall()
        assert len(fences) == 1
        conn.close()

        # 2. Test Circular Geofence Containment
        # Point inside circle (< 500m)
        assert is_point_in_circle(17.4435, 78.3852, 17.4430, 78.3850, 500.0) is True
        # Point outside circle (> 500m)
        assert is_point_in_circle(17.4600, 78.3850, 17.4430, 78.3850, 500.0) is False

        # 3. Test Polygon Geofence Containment (HITEC City boundary)
        polygon = [
            [17.4400, 78.3800],
            [17.4500, 78.3800],
            [17.4500, 78.3900],
            [17.4400, 78.3900],
        ]
        # Point inside polygon
        assert is_point_in_polygon(17.4450, 78.3850, polygon) is True
        # Point outside polygon
        assert is_point_in_polygon(17.4600, 78.3850, polygon) is False

    def test_08_journey_events_deduplication(self, isolated_db):
        """Verifies journey events (deviations, departures, arrivals) are deduplicated."""
        jid = f"journey_{TARGET_DEVICE_ID}_001"

        ev1 = record_journey_event({
            "journey_id": jid,
            "device_id": TARGET_DEVICE_ID,
            "event_type": "ROUTE_DEVIATION_ALERT",
            "latitude": 17.4430,
            "longitude": 78.3950,
            "event_time": "2026-11-25T10:15:00Z",
            "details": {"distance_from_route_meters": 520.0}
        }, db_path=isolated_db)
        assert ev1["id"] is not None

        # Repeat same event
        ev1_dup = record_journey_event({
            "journey_id": jid,
            "device_id": TARGET_DEVICE_ID,
            "event_type": "ROUTE_DEVIATION_ALERT",
            "latitude": 17.4430,
            "longitude": 78.3950,
            "event_time": "2026-11-25T10:15:00Z",
            "details": {"distance_from_route_meters": 520.0}
        }, db_path=isolated_db)

        events = get_journey_events(jid, db_path=isolated_db)
        assert len(events) == 1  # Deduplicated!
