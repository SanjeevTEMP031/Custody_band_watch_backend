import os
import sys
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

BACKEND_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BACKEND_DIR))

from app.utils.datetime_utils import (
    parse_to_utc_datetime,
    to_utc_iso,
    to_ist_iso,
    format_utc_to_ist,
    current_iso_timestamp,
)
from app.services.alert_service import AlertService, ws_manager
from app.geofencing.alerts import (
    trigger_geofence_alert,
    acknowledge_geofence_alert,
    silence_geofence_alert,
    resolve_geofence_alert,
    list_geofence_alerts,
)
from app.geofencing.models import init_geofence_database
from app.services.firestore_service import (
    normalize_verified_gps_fix,
    valid_coordinate_pair,
)
from app.geofencing.evaluator import evaluate_geofence
from app.journeys.route_monitor import RouteMonitor, route_monitor


class AlertLifecycleSyncTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "test_custody.db"

        # Patch LOCAL_DB across all modules to use our isolated test database
        self.patches = [
            patch("app.services.alert_service.LOCAL_DB", self.db_path),
            patch("app.geofencing.models.LOCAL_DB", self.db_path),
            patch("app.services.firestore_service.LOCAL_DB", self.db_path),
            patch("app.journeys.models.LOCAL_DB", self.db_path),
            patch("app.services.alert_service.is_firebase_active", return_value=False),
            patch("app.geofencing.models.is_firebase_active", return_value=False),
            patch("app.services.firestore_service.is_firebase_active", return_value=False),
        ]
        for p in self.patches:
            p.start()

        # Initialize schema via FirestoreService and AlertService and init_geofence_database
        from app.services.firestore_service import FirestoreService
        self.firestore_service = FirestoreService()
        self.alert_service = AlertService()
        init_geofence_database()

        # Insert a test device
        conn = sqlite3.connect(str(self.db_path))
        conn.execute("""
            INSERT OR REPLACE INTO devices (
                device_id, wearer_name, case_number, status, connectivity_status,
                last_latitude, last_longitude, last_valid_gps_time, gps_valid
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, ("DEV_TEST_8637", "Test Offender", "CASE-999", "ONLINE", "ONLINE", 12.9716, 77.5946, datetime.now(timezone.utc).isoformat(), 1))
        conn.commit()
        conn.close()

    def tearDown(self):
        for p in reversed(self.patches):
            p.stop()
        try:
            self.temp_dir.cleanup()
        except (PermissionError, OSError):
            pass

    def test_geofence_alert_bidirectional_linkage(self):
        """Verify geofence alert creates linked entries in both geofence_alerts and alerts tables."""
        event_time = datetime.now(timezone.utc).isoformat()
        alert = trigger_geofence_alert(
            event_id="EVT_001",
            device_id="DEV_TEST_8637",
            geofence_id="GEO_001",
            geofence_name="Safe Zone",
            event_type="GEOFENCE_EXIT",
            latitude=12.9720,
            longitude=77.5950,
            timestamp=event_time,
            severity="HIGH",
            fence_type="INCLUSION"
        )

        geo_alert_id = alert["id"]
        core_alert_id = alert.get("core_alert_id")
        self.assertIsNotNone(core_alert_id, "core_alert_id must be assigned and linked")
        self.assertTrue(core_alert_id.startswith("alt_DEV_TEST_8637"))
        self.assertTrue(alert["requires_alarm"])

        # Check geofence_alerts table
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        geo_row = cur.execute("SELECT * FROM geofence_alerts WHERE id = ?", (geo_alert_id,)).fetchone()
        self.assertIsNotNone(geo_row)
        self.assertEqual(geo_row["core_alert_id"], core_alert_id)
        self.assertEqual(geo_row["requires_alarm"], 1)
        self.assertEqual(geo_row["status"], "ACTIVE")

        # Check alerts table
        core_row = cur.execute("SELECT * FROM alerts WHERE alert_id = ?", (core_alert_id,)).fetchone()
        self.assertIsNotNone(core_row)
        self.assertEqual(core_row["geofence_alert_id"], geo_alert_id)
        self.assertEqual(core_row["requires_alarm"], 1)
        self.assertEqual(core_row["status"], "ACTIVE")
        conn.close()

    def test_silence_via_core_alert_syncs_both_tables(self):
        """Silencing via alert_service updates both alerts and geofence_alerts records."""
        event_time = datetime.now(timezone.utc).isoformat()
        alert = trigger_geofence_alert(
            event_id="EVT_002",
            device_id="DEV_TEST_8637",
            geofence_id="GEO_001",
            geofence_name="Safe Zone",
            event_type="GEOFENCE_EXIT",
            latitude=12.9720,
            longitude=77.5950,
            timestamp=event_time,
            severity="HIGH",
            fence_type="INCLUSION"
        )
        core_alert_id = alert["core_alert_id"]
        geo_alert_id = alert["id"]

        # Silence via core alert
        silenced = self.alert_service.silence_alert(core_alert_id, silenced_by="officer@law.gov")
        self.assertIsNotNone(silenced)
        self.assertFalse(silenced["requires_alarm"])
        self.assertEqual(silenced["status"], "ACKNOWLEDGED")

        # Verify in SQLite alerts table
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        c_row = cur.execute("SELECT * FROM alerts WHERE alert_id = ?", (core_alert_id,)).fetchone()
        self.assertEqual(c_row["status"], "ACKNOWLEDGED")
        self.assertEqual(c_row["requires_alarm"], 0)

        # Verify in SQLite geofence_alerts table
        g_row = cur.execute("SELECT * FROM geofence_alerts WHERE id = ?", (geo_alert_id,)).fetchone()
        self.assertEqual(g_row["status"], "ACKNOWLEDGED")
        self.assertEqual(g_row["requires_alarm"], 0)
        conn.close()

    def test_acknowledge_via_geofence_endpoint_syncs_both_tables(self):
        """Acknowledging via geofence alert endpoint updates both tables."""
        event_time = datetime.now(timezone.utc).isoformat()
        alert = trigger_geofence_alert(
            event_id="EVT_003",
            device_id="DEV_TEST_8637",
            geofence_id="GEO_001",
            geofence_name="Safe Zone",
            event_type="GEOFENCE_EXIT",
            latitude=12.9720,
            longitude=77.5950,
            timestamp=event_time,
            severity="HIGH",
            fence_type="INCLUSION"
        )
        geo_alert_id = alert["id"]
        core_alert_id = alert["core_alert_id"]

        # Acknowledge via geofence endpoint
        ack_res = acknowledge_geofence_alert(geo_alert_id, officer_email="officer_smith")
        self.assertIsNotNone(ack_res)
        self.assertFalse(ack_res["requires_alarm"])
        self.assertEqual(ack_res["status"], "ACKNOWLEDGED")

        # Verify both rows in DB
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        g_row = cur.execute("SELECT * FROM geofence_alerts WHERE id = ?", (geo_alert_id,)).fetchone()
        self.assertEqual(g_row["status"], "ACKNOWLEDGED")
        self.assertEqual(g_row["requires_alarm"], 0)

        c_row = cur.execute("SELECT * FROM alerts WHERE alert_id = ?", (core_alert_id,)).fetchone()
        self.assertEqual(c_row["status"], "ACKNOWLEDGED")
        self.assertEqual(c_row["requires_alarm"], 0)
        conn.close()

    def test_resolution_syncs_both_tables(self):
        """Resolving a geofence alert resolves both tables with requires_alarm=0."""
        event_time = datetime.now(timezone.utc).isoformat()
        alert = trigger_geofence_alert(
            event_id="EVT_004",
            device_id="DEV_TEST_8637",
            geofence_id="GEO_001",
            geofence_name="Safe Zone",
            event_type="GEOFENCE_EXIT",
            latitude=12.9720,
            longitude=77.5950,
            timestamp=event_time,
            severity="HIGH",
            fence_type="INCLUSION"
        )
        geo_alert_id = alert["id"]
        core_alert_id = alert["core_alert_id"]

        res = resolve_geofence_alert(geo_alert_id, officer_email="officer_jones", notes="Offender returned")
        self.assertIsNotNone(res)
        self.assertFalse(res["requires_alarm"])
        self.assertEqual(res["status"], "RESOLVED")

        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        g_row = cur.execute("SELECT * FROM geofence_alerts WHERE id = ?", (geo_alert_id,)).fetchone()
        self.assertEqual(g_row["status"], "RESOLVED")
        self.assertEqual(g_row["requires_alarm"], 0)

        c_row = cur.execute("SELECT * FROM alerts WHERE alert_id = ?", (core_alert_id,)).fetchone()
        self.assertEqual(c_row["status"], "RESOLVED")
        self.assertEqual(c_row["requires_alarm"], 0)
        conn.close()

    def test_device_alert_status_requires_alarm_sync(self):
        """Test device alert status endpoint aggregates requires_alarm correctly."""
        # 1. Trigger alert
        alert = trigger_geofence_alert(
            event_id="EVT_005",
            device_id="DEV_TEST_8637",
            geofence_id="GEO_001",
            geofence_name="Safe Zone",
            event_type="GEOFENCE_EXIT",
            latitude=12.9720,
            longitude=77.5950,
            timestamp=datetime.now(timezone.utc).isoformat(),
            severity="HIGH"
        )
        status = self.alert_service.get_device_alert_status("DEV_TEST_8637")
        self.assertTrue(status["requires_alarm"])
        self.assertGreaterEqual(status["active_alert_count"], 1)

        # 2. Silence alert
        self.alert_service.silence_alert(alert["core_alert_id"])
        status_silenced = self.alert_service.get_device_alert_status("DEV_TEST_8637")
        self.assertFalse(status_silenced["requires_alarm"])

    def test_silence_geofence_alert_endpoint_and_listing(self):
        """Silencing a geofence alert via silence_geofence_alert updates both tables and list returns requires_alarm=False."""
        alert = trigger_geofence_alert(
            event_id="EVT_006",
            device_id="DEV_TEST_8637",
            geofence_id="GEO_001",
            geofence_name="Safe Zone",
            event_type="GEOFENCE_EXIT",
            latitude=12.9720,
            longitude=77.5950,
            timestamp=datetime.now(timezone.utc).isoformat(),
            severity="HIGH"
        )
        geo_id = alert["id"]
        core_id = alert["core_alert_id"]

        silenced = silence_geofence_alert(geo_id, officer_email="officer_alex", notes="Silenced via endpoint")
        self.assertIsNotNone(silenced)
        self.assertFalse(silenced["requires_alarm"])

        # Check list_geofence_alerts
        alerts_list = list_geofence_alerts(device_id="DEV_TEST_8637")
        matched = [a for a in alerts_list if a["id"] == geo_id]
        self.assertTrue(len(matched) > 0)
        self.assertFalse(matched[0]["requires_alarm"])
        self.assertTrue(matched[0]["acknowledged"])

        # Check core alert by id
        core_alert = self.alert_service.get_alert_by_id(geo_id)
        self.assertIsNotNone(core_alert)
        self.assertFalse(core_alert["requires_alarm"])

    def test_utc_ist_timestamp_handling(self):
        """Universal parser handles UTC, offset, and textual IST accurately."""
        # UTC ISO string
        utc_str = "2026-11-24T12:00:00Z"
        dt_utc = parse_to_utc_datetime(utc_str)
        self.assertIsNotNone(dt_utc)
        self.assertEqual(dt_utc.tzinfo, timezone.utc)
        self.assertEqual(dt_utc.hour, 12)

        # IST ISO string (+05:30)
        ist_str = "2026-11-24T17:30:00+05:30"
        dt_ist = parse_to_utc_datetime(ist_str)
        self.assertIsNotNone(dt_ist)
        # 17:30 IST is exactly 12:00 UTC
        self.assertEqual(dt_ist.hour, 12)
        self.assertEqual(dt_ist.minute, 0)
        self.assertEqual(dt_utc, dt_ist)

        # Textual IST string
        textual_ist = "2026-11-24 17:30:00 IST"
        dt_text = parse_to_utc_datetime(textual_ist)
        self.assertIsNotNone(dt_text)
        self.assertEqual(dt_text.hour, 12)
        self.assertEqual(dt_text.minute, 0)

        # Format to IST
        formatted_ist = format_utc_to_ist(utc_str)
        self.assertIn("05:30:00 PM IST", formatted_ist)
        self.assertEqual(to_ist_iso(utc_str), "2026-11-24T17:30:00+05:30")

    def test_future_gps_rejection(self):
        """GPS readings more than 15s in the future are strictly rejected."""
        now = datetime.now(timezone.utc)
        future_ts = (now + timedelta(seconds=60)).isoformat()

        # 1. normalize_verified_gps_fix rejects future fix
        res = normalize_verified_gps_fix(
            gps_valid=1,
            latitude=12.9716,
            longitude=77.5946,
            timestamp=future_ts,
            gps_source="watch_frame"
        )
        self.assertIsNone(res, "Future GPS fix (>15s) must return None")

        # 2. Tolerates slight clock skew (< 15s)
        slight_skew = (now + timedelta(seconds=5)).isoformat()
        res_skew = normalize_verified_gps_fix(
            gps_valid=1,
            latitude=12.9716,
            longitude=77.5946,
            timestamp=slight_skew,
            gps_source="watch_frame"
        )
        self.assertIsNotNone(res_skew, "Slight clock skew (<15s) should be tolerated")

        # 3. capture_location_snapshot rejects future timestamp from Priority 1
        snap = self.alert_service.capture_location_snapshot(
            device_id="DEV_TEST_8637",
            event_lat=12.9716,
            event_lon=77.5946,
            event_gps_time=future_ts,
            is_valid_gps=True,
            event_time_iso=now.isoformat()
        )
        self.assertNotEqual(snap["location_source"], "GPS_FRESH")

        # 4. Evaluator rejects future GPS points (>15s)
        eval_events = evaluate_geofence(
            device_id="DEV_TEST_8637",
            latitude=12.9716,
            longitude=77.5946,
            timestamp=future_ts
        )
        self.assertEqual(eval_events, [], "Evaluator must return empty list for future GPS timestamp")

        # 5. RouteMonitor rejects future GPS points (>15s)
        rm_res = route_monitor.evaluate_gps_position(
            device_id="DEV_TEST_8637",
            latitude=12.9716,
            longitude=77.5946,
            timestamp=future_ts
        )
        self.assertIsNone(rm_res, "RouteMonitor must ignore future GPS point")

    def test_gps_freshness_calculation(self):
        """Location freshness is accurately calculated: <=120s is GPS_FRESH, >120s is HISTORICAL."""
        now = datetime.now(timezone.utc)

        # 30 seconds old -> FRESH
        t_fresh = (now - timedelta(seconds=30)).isoformat()
        snap_fresh = self.alert_service.capture_location_snapshot(
            device_id="DEV_TEST_8637",
            event_lat=12.9716,
            event_lon=77.5946,
            event_gps_time=t_fresh,
            is_valid_gps=True,
            event_time_iso=now.isoformat()
        )
        self.assertEqual(snap_fresh["location_source"], "GPS_FRESH")
        self.assertFalse(snap_fresh["location_is_stale"])
        self.assertAlmostEqual(snap_fresh["location_age_seconds"], 30, delta=2)

        # 150 seconds old -> HISTORICAL
        t_stale = (now - timedelta(seconds=150)).isoformat()
        snap_stale = self.alert_service.capture_location_snapshot(
            device_id="DEV_TEST_8637",
            event_lat=12.9716,
            event_lon=77.5946,
            event_gps_time=t_stale,
            is_valid_gps=True,
            event_time_iso=now.isoformat()
        )
        self.assertEqual(snap_stale["location_source"], "HISTORICAL")
        self.assertTrue(snap_stale["location_is_stale"])
        self.assertAlmostEqual(snap_stale["location_age_seconds"], 150, delta=2)


if __name__ == "__main__":
    unittest.main()
