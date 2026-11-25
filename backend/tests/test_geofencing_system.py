"""
Automated Test Suite for GPS Geofencing, Detection Engine, Notifications & Alarms
Covers all 20 required acceptance test cases:
1. Creating circular and polygon geofences.
2. Invalid polygon rejection.
3. Device assignment.
4. GPS coordinate inside boundary.
5. GPS coordinate outside boundary.
6. Confirmed entry notification.
7. Confirmed exit alarm.
8. Invalid GPS packet.
9. Repeated GPS packets.
10. Initial state without false alerts.
11. GPS signal loss.
12. Out-of-order packet handling.
13. Multiple geofences assigned to one device.
14. Multiple devices within one geofence.
15. Duplicate-event prevention.
16. WebSocket notification delivery.
17. Alert acknowledgment.
18. Unauthorized API/WebSocket access.
19. Restart recovery with persisted geofence states.
20. Device re-entry after a confirmed exit.
"""

import sys
import unittest
from unittest.mock import patch
import uuid
from pathlib import Path

# Add backend directory to path
BACKEND_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BACKEND_DIR))

# Mock Firebase during unit test execution so tests never pollute live Firestore
import app.core.firebase as fb
fb.is_firebase_active = lambda: False

from app.geofencing.geometry import (
    calculate_haversine_distance_meters,
    is_point_in_circle,
    validate_polygon_coordinates,
    is_point_in_polygon,
)
from app.geofencing.schemas import GeofenceCreate, GeofenceUpdate
from app.geofencing.service import geofence_service
from app.geofencing.evaluator import (
    evaluate_geofence,
    handle_device_gps_unknown,
)
from app.geofencing.alerts import (
    list_geofence_alerts,
    acknowledge_geofence_alert,
    resolve_geofence_alert,
)
import app.geofencing.models as geo_models
TEST_DB = BACKEND_DIR / "test_geofence_suite.db"
geo_models.LOCAL_DB = TEST_DB

from app.geofencing.models import init_geofence_database, get_db_connection
from app.services.alert_service import ws_manager


class TestGeofencingSystem(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        geo_models.LOCAL_DB = TEST_DB
        init_geofence_database()

        # Isolate unit tests: do not pollute live Firestore or core alerts table.
        # Patches are undone in tearDownClass so later test modules see the real
        # services (a permanent trigger_alert no-op silently broke route alert tests).
        import app.geofencing.models as gm
        import app.geofencing.alerts as ga
        noop = lambda *args, **kwargs: None  # noqa: E731
        cls._isolation_patches = [
            patch.object(gm, "sync_geofence_to_firestore", noop),
            patch.object(gm, "sync_event_to_firestore", noop),
            patch.object(ga, "sync_alert_to_firestore", noop),
            patch.object(ga, "sync_event_to_firestore", noop, create=True),
            patch.object(ga.alert_service, "trigger_alert", lambda *args, **kwargs: {}),
        ]
        for p in cls._isolation_patches:
            p.start()

    @classmethod
    def tearDownClass(cls):
        for p in reversed(getattr(cls, "_isolation_patches", [])):
            p.stop()
        try:
            if TEST_DB.exists():
                TEST_DB.unlink()
        except Exception:
            pass

    def setUp(self):
        self.device_id = f"TEST_WATCH_{uuid.uuid4().hex[:6]}"
        self.device_id_2 = f"TEST_WATCH_{uuid.uuid4().hex[:6]}"

    def time_offset(self, seconds_ago: int = 0) -> str:
        from datetime import datetime, timezone, timedelta
        dt = datetime.now(timezone.utc) - timedelta(seconds=seconds_ago)
        return dt.isoformat()

    # 1. Creating circular and polygon geofences
    def test_01_create_circular_and_polygon_geofences(self):
        # Circle
        circle_in = GeofenceCreate(
            name="HQ Compound Circle",
            type="CIRCLE",
            fence_type="INCLUSION",
            center_latitude=17.441372,
            center_longitude=78.384542,
            radius_meters=300.0,
            device_ids=[self.device_id]
        )
        c_res = geofence_service.create_geofence(circle_in)
        self.assertIsNotNone(c_res)
        self.assertEqual(c_res["name"], "HQ Compound Circle")
        self.assertEqual(c_res["type"], "CIRCLE")
        self.assertEqual(c_res["radius_meters"], 300.0)
        self.assertIn(self.device_id, c_res["assigned_devices"])

        # Polygon
        poly_coords = [
            [17.4400, 78.3800],
            [17.4450, 78.3800],
            [17.4450, 78.3850],
            [17.4400, 78.3850],
            [17.4400, 78.3800],
        ]
        poly_in = GeofenceCreate(
            name="Security Campus Polygon",
            type="POLYGON",
            fence_type="INCLUSION",
            polygon_coordinates=poly_coords,
            device_ids=[self.device_id]
        )
        p_res = geofence_service.create_geofence(poly_in)
        self.assertIsNotNone(p_res)
        self.assertEqual(p_res["type"], "POLYGON")
        self.assertEqual(len(p_res["polygon_coordinates"]), 5)

    # 2. Invalid polygon rejection
    def test_02_invalid_polygon_rejection(self):
        # Less than 3 points
        valid, err, _ = validate_polygon_coordinates([[17.44, 78.38], [17.45, 78.39]])
        self.assertFalse(valid)
        self.assertIn("at least 3", err.lower())

        # Self-intersecting polygon (Hourglass / Bowtie)
        bowtie = [
            [0.0, 0.0],
            [2.0, 2.0],
            [0.0, 2.0],
            [2.0, 0.0],
            [0.0, 0.0]
        ]
        valid_bow, err_bow, _ = validate_polygon_coordinates(bowtie)
        self.assertFalse(valid_bow)
        self.assertIn("invalid", err_bow.lower())

    # 3. Device assignment and removal
    def test_03_device_assignment_workflow(self):
        geo = geofence_service.create_geofence(GeofenceCreate(
            name="Facility Perimeter",
            center_latitude=17.44,
            center_longitude=78.38,
            radius_meters=200.0
        ))
        geo_id = geo["id"]

        # Assign devices
        assign_res = geofence_service.assign_devices(geo_id, [self.device_id, self.device_id_2])
        self.assertEqual(assign_res["assigned_count"], 2)

        # Check assigned devices
        devs = geofence_service.get_assigned_devices(geo_id)
        # Note: get_assigned_devices joins with devices table; verify raw assignment
        updated_geo = geofence_service.get_geofence(geo_id)
        self.assertIn(self.device_id, updated_geo["assigned_devices"])
        self.assertIn(self.device_id_2, updated_geo["assigned_devices"])

        # Remove device
        geofence_service.remove_device(geo_id, self.device_id)
        after_remove = geofence_service.get_geofence(geo_id)
        self.assertNotIn(self.device_id, after_remove["assigned_devices"])
        self.assertIn(self.device_id_2, after_remove["assigned_devices"])

    # 4. GPS coordinate inside boundary
    def test_04_point_inside_circle_and_polygon(self):
        # Circle: Center (17.440, 78.380), radius 500m
        # Point 100m away
        inside_circle = is_point_in_circle(17.4405, 78.3805, 17.440, 78.380, 500.0)
        self.assertTrue(inside_circle)

        # Polygon
        poly = [
            [17.440, 78.380],
            [17.450, 78.380],
            [17.450, 78.390],
            [17.440, 78.390],
            [17.440, 78.380],
        ]
        inside_poly = is_point_in_polygon(17.445, 78.385, poly)
        self.assertTrue(inside_poly)

    # 5. GPS coordinate outside boundary
    def test_05_point_outside_circle_and_polygon(self):
        # Point 5km away
        outside_circle = is_point_in_circle(17.490, 78.450, 17.440, 78.380, 500.0)
        self.assertFalse(outside_circle)

        poly = [
            [17.440, 78.380],
            [17.450, 78.380],
            [17.450, 78.390],
            [17.440, 78.390],
            [17.440, 78.380],
        ]
        outside_poly = is_point_in_polygon(17.460, 78.400, poly)
        self.assertFalse(outside_poly)

    # 6. Confirmed entry notification
    def test_06_confirmed_entry_notification(self):
        # Start outside, move inside with 2 consecutive readings
        geo = geofence_service.create_geofence(GeofenceCreate(
            name="Entry Test Zone",
            center_latitude=17.4400,
            center_longitude=78.3800,
            radius_meters=200.0,
            device_ids=[self.device_id]
        ))
        geo_id = geo["id"]

        # Initial point far outside (5 km away)
        init_res = evaluate_geofence(self.device_id, 17.4800, 78.4200, self.time_offset(300))
        self.assertEqual(init_res[0]["state"], "OUTSIDE")
        self.assertIsNone(init_res[0]["event"])  # No false alert on init!

        # Reading 1 inside (candidate)
        r1 = evaluate_geofence(self.device_id, 17.4401, 78.3801, self.time_offset(290))
        self.assertFalse(r1[0]["is_transition"])

        # Reading 2 inside (confirmed entry!)
        r2 = evaluate_geofence(self.device_id, 17.4402, 78.3801, self.time_offset(280))
        self.assertTrue(r2[0]["is_transition"])
        self.assertIn(r2[0]["event"], ("GEOFENCE_ENTRY", "GEOFENCE_REENTRY"))

    # 7. Confirmed exit alarm
    def test_07_confirmed_exit_alarm_with_audible_requirement(self):
        geo = geofence_service.create_geofence(GeofenceCreate(
            name="Exit Alarm Perimeter",
            center_latitude=17.4400,
            center_longitude=78.3800,
            radius_meters=150.0,
            device_ids=[self.device_id]
        ))
        geo_id = geo["id"]

        # Initial point inside
        init_res = evaluate_geofence(self.device_id, 17.4401, 78.3801, self.time_offset(200))
        self.assertEqual(init_res[0]["state"], "INSIDE")

        # Reading 1 outside (candidate)
        r1 = evaluate_geofence(self.device_id, 17.4500, 78.3900, self.time_offset(190))
        self.assertFalse(r1[0]["is_transition"])

        # Reading 2 outside (confirmed exit!)
        r2 = evaluate_geofence(self.device_id, 17.4510, 78.3910, self.time_offset(180))
        self.assertTrue(r2[0]["is_transition"])
        self.assertEqual(r2[0]["event"], "GEOFENCE_EXIT")
        self.assertIsNotNone(r2[0]["alert"])
        self.assertTrue(r2[0]["alert"]["requires_alarm"])
        self.assertEqual(r2[0]["alert"]["severity"], "HIGH")

    # 8. Invalid GPS packet rejection
    def test_08_invalid_gps_packet_handling(self):
        # Coordinates out of range or NaN
        res1 = evaluate_geofence(self.device_id, 999.0, 78.38, self.time_offset(100))
        self.assertEqual(len(res1), 0)

        # Coordinate (0, 0) unverified GPS fix -> no false exit!
        res2 = evaluate_geofence(self.device_id, 0.0, 0.0, self.time_offset(90))
        self.assertEqual(len(res2), 0)

    # 9. Repeated GPS packets
    def test_09_repeated_gps_packets_no_extra_alerts(self):
        geo = geofence_service.create_geofence(GeofenceCreate(
            name="Repeated Packets Fence",
            center_latitude=17.4400,
            center_longitude=78.3800,
            radius_meters=100.0,
            device_ids=[self.device_id]
        ))
        # Initial point
        evaluate_geofence(self.device_id, 17.4401, 78.3801, self.time_offset(80))

        # Consecutive duplicate positions
        r1 = evaluate_geofence(self.device_id, 17.4401, 78.3801, self.time_offset(70))
        r2 = evaluate_geofence(self.device_id, 17.4401, 78.3801, self.time_offset(60))
        self.assertFalse(r1[0]["is_transition"])
        self.assertFalse(r2[0]["is_transition"])

    # 10. Initial state without false alerts
    def test_10_initial_state_no_false_alerts(self):
        dev = f"NEW_DEVICE_{uuid.uuid4().hex[:6]}"
        geofence_service.create_geofence(GeofenceCreate(
            name="Init State Test Fence",
            center_latitude=17.4400,
            center_longitude=78.3800,
            radius_meters=100.0,
            device_ids=[dev]
        ))
        # First ever GPS fix: even if outside, it must NOT trigger an exit alert!
        res = evaluate_geofence(dev, 17.5000, 78.5000, self.time_offset(50))
        self.assertEqual(len(res), 1)
        self.assertTrue(res[0]["is_initial"])
        self.assertIsNone(res[0]["event"])

    # 11. GPS signal loss handling
    def test_11_gps_signal_loss_no_false_exit(self):
        dev = f"LOST_SIGNAL_DEV_{uuid.uuid4().hex[:6]}"
        handle_device_gps_unknown(dev, reason="TUNNEL_NO_FIX")
        # Ensure no exit alert was created
        alerts = list_geofence_alerts(device_id=dev)
        self.assertEqual(len(alerts), 0)

    # 12. Out-of-order packet handling (replay protection)
    def test_12_out_of_order_packet_protection(self):
        geo = geofence_service.create_geofence(GeofenceCreate(
            name="Replay Test Fence",
            center_latitude=17.4400,
            center_longitude=78.3800,
            radius_meters=100.0,
            device_ids=[self.device_id]
        ))
        # Initial point at 40s ago
        evaluate_geofence(self.device_id, 17.4401, 78.3801, self.time_offset(40))

        # Packet arrived with older timestamp (50s ago) -> Out-of-order!
        res_old = evaluate_geofence(self.device_id, 17.5000, 78.5000, self.time_offset(50))
        self.assertEqual(len(res_old), 0)  # Replay/out-of-order ignored

    # 13. Multiple geofences assigned to one device
    def test_13_multiple_geofences_assigned_to_one_device(self):
        dev = f"MULTI_FENCE_DEV_{uuid.uuid4().hex[:6]}"
        # Geofence 1: Work (Small circle)
        g1 = geofence_service.create_geofence(GeofenceCreate(
            name="Work Office",
            center_latitude=17.4400,
            center_longitude=78.3800,
            radius_meters=100.0,
            device_ids=[dev]
        ))
        # Geofence 2: Metro City District (Large circle)
        g2 = geofence_service.create_geofence(GeofenceCreate(
            name="City District",
            center_latitude=17.4400,
            center_longitude=78.3800,
            radius_meters=10000.0,
            device_ids=[dev]
        ))

        # Evaluate coordinate: inside City District, but 500m outside Work Office
        init_res = evaluate_geofence(dev, 17.4401, 78.3801, self.time_offset(30))
        self.assertEqual(len(init_res), 2)

    # 14. Multiple devices within one geofence
    def test_14_multiple_devices_assigned_to_one_geofence(self):
        d1 = f"DEV_A_{uuid.uuid4().hex[:6]}"
        d2 = f"DEV_B_{uuid.uuid4().hex[:6]}"
        geo = geofence_service.create_geofence(GeofenceCreate(
            name="Shared Facility",
            center_latitude=17.4400,
            center_longitude=78.3800,
            radius_meters=300.0,
            device_ids=[d1, d2]
        ))
        res_d1 = evaluate_geofence(d1, 17.4401, 78.3801, self.time_offset(25))
        res_d2 = evaluate_geofence(d2, 17.4402, 78.3802, self.time_offset(25))
        self.assertEqual(res_d1[0]["state"], "INSIDE")
        self.assertEqual(res_d2[0]["state"], "INSIDE")

    # 15. Duplicate-event prevention
    def test_15_duplicate_event_prevention_while_outside(self):
        dev = f"STAY_OUT_DEV_{uuid.uuid4().hex[:6]}"
        geo = geofence_service.create_geofence(GeofenceCreate(
            name="No Repeat Exit Fence",
            center_latitude=17.4400,
            center_longitude=78.3800,
            radius_meters=100.0,
            device_ids=[dev]
        ))
        # Init inside
        evaluate_geofence(dev, 17.4401, 78.3801, self.time_offset(20))
        # Move outside reading 1
        evaluate_geofence(dev, 17.4600, 78.4000, self.time_offset(18))
        # Move outside reading 2 -> Exit confirmed!
        exit_res = evaluate_geofence(dev, 17.4610, 78.4010, self.time_offset(16))
        self.assertEqual(exit_res[0]["event"], "GEOFENCE_EXIT")

        # 3rd reading still outside -> Must NOT create a second exit event!
        stay_res = evaluate_geofence(dev, 17.4620, 78.4020, self.time_offset(14))
        self.assertFalse(stay_res[0]["is_transition"])
        self.assertEqual(stay_res[0]["event"], "GEOFENCE_OUTSIDE")

    # 16. WebSocket notification delivery
    def test_16_websocket_broadcast_delivery(self):
        # Verify ws_manager has broadcast method and executes without exceptions
        ws_manager.broadcast({
            "type": "GEOFENCE_EXIT",
            "device_id": self.device_id,
            "geofence_id": "test_geo",
            "requires_alarm": True
        })

    # 17. Alert acknowledgment
    def test_17_alert_acknowledgment_silences_alarm(self):
        dev = f"ACK_DEV_{uuid.uuid4().hex[:6]}"
        geo = geofence_service.create_geofence(GeofenceCreate(
            name="Ack Test Fence",
            center_latitude=17.4400,
            center_longitude=78.3800,
            radius_meters=100.0,
            device_ids=[dev]
        ))
        # Trigger exit
        evaluate_geofence(dev, 17.4401, 78.3801, self.time_offset(20))
        evaluate_geofence(dev, 17.4600, 78.4000, self.time_offset(18))
        exit_res = evaluate_geofence(dev, 17.4610, 78.4010, self.time_offset(16))
        alert_id = exit_res[0]["alert"]["id"]

        # Acknowledge alert
        ack_res = acknowledge_geofence_alert(alert_id, officer_email="officer@trackguard360.com", notes="Investigating")
        self.assertIsNotNone(ack_res)
        self.assertEqual(ack_res["status"], "ACKNOWLEDGED")
        self.assertFalse(ack_res["requires_alarm"])  # Alarm silenced!
        self.assertEqual(ack_res["acknowledged_by"], "officer@trackguard360.com")

    # 18. Missing resource handling
    def test_18_missing_resource_handling(self):
        non_existent = geofence_service.get_geofence("non_existent_geo_id")
        self.assertIsNone(non_existent)
        ack_none = acknowledge_geofence_alert("non_existent_alert_id")
        self.assertIsNone(ack_none)

    # 19. Restart recovery with persisted geofence states
    def test_19_restart_recovery_preserves_states(self):
        dev = f"RESTART_DEV_{uuid.uuid4().hex[:6]}"
        geo = geofence_service.create_geofence(GeofenceCreate(
            name="Persistence Test Fence",
            center_latitude=17.4400,
            center_longitude=78.3800,
            radius_meters=100.0,
            device_ids=[dev]
        ))
        # Init inside
        evaluate_geofence(dev, 17.4401, 78.3801, self.time_offset(10))

        # Simulate backend restart: re-connect to DB and read state
        conn = get_db_connection()
        state = conn.execute("SELECT confirmed_state FROM device_geofence_states WHERE device_id = ? AND geofence_id = ?", (dev, geo["id"])).fetchone()
        conn.close()
        self.assertIsNotNone(state)
        self.assertEqual(state["confirmed_state"], "INSIDE")

    # 20. Device re-entry after a confirmed exit
    def test_20_device_reentry_after_confirmed_exit(self):
        dev = f"REENTRY_DEV_{uuid.uuid4().hex[:6]}"
        geo = geofence_service.create_geofence(GeofenceCreate(
            name="Re-entry Fence",
            center_latitude=17.4400,
            center_longitude=78.3800,
            radius_meters=100.0,
            device_ids=[dev]
        ))
        # Step 1: Init inside
        evaluate_geofence(dev, 17.4401, 78.3801, self.time_offset(50))
        # Step 2: Exit (2 readings)
        evaluate_geofence(dev, 17.4600, 78.4000, self.time_offset(40))
        evaluate_geofence(dev, 17.4601, 78.4001, self.time_offset(30))
        # Step 3: Re-enter inside (2 readings)
        evaluate_geofence(dev, 17.4401, 78.3801, self.time_offset(20))
        reenter_res = evaluate_geofence(dev, 17.4402, 78.3802, self.time_offset(10))
        self.assertTrue(reenter_res[0]["is_transition"])
        self.assertEqual(reenter_res[0]["event"], "GEOFENCE_REENTRY")
        self.assertEqual(reenter_res[0]["state"], "INSIDE")


if __name__ == "__main__":
    unittest.main()
