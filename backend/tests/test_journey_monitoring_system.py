"""
Comprehensive Automated Test Suite for Geofencing & Route Monitoring System
Covers all 15 required acceptance test scenarios:
1. Inside -> Outside geofence transition.
2. Outside -> Inside geofence transition.
3. INCLUSION and EXCLUSION alert severity.
4. Route selection and persistence.
5. Valid movement along the approved route.
6. Confirmed route deviation.
7. Device returning to the route.
8. Unauthorized early departure.
9. Delayed journey start.
10. Overdue arrival.
11. GPS signal loss.
12. Duplicate GPS packets.
13. Out-of-order GPS timestamps.
14. Multiple devices and simultaneous journeys.
15. WebSocket notification and database persistence.
"""

import sys
import unittest
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Add backend directory to path
BACKEND_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BACKEND_DIR))

# Mock Firebase during unit test execution so tests never pollute live Firestore
import app.core.firebase as fb
fb.is_firebase_active = lambda: False

import app.geofencing.models as geo_models
import app.journeys.models as journey_models
import app.geofencing.alerts as ga

TEST_DB = BACKEND_DIR / "test_journey_suite.db"
geo_models.LOCAL_DB = TEST_DB
journey_models.LOCAL_DB = TEST_DB

from app.geofencing.models import init_geofence_database
from app.journeys.models import (
    init_journey_database,
    create_journey_record,
    get_journey_by_id,
    update_journey_record,
    save_journey_route,
    get_journey_routes,
    select_journey_route,
    get_selected_route_for_journey,
    get_journey_gps_points,
    get_journey_events,
)
from app.geofencing.schemas import GeofenceCreate
from app.geofencing.service import geofence_service
from app.geofencing.evaluator import evaluate_geofence
from app.services.osrm_service import osrm_service
from app.journeys.route_monitor import (
    route_monitor,
    calculate_perpendicular_distance_to_route,
)
from app.services.alert_service import ws_manager, alert_service


class TestJourneyMonitoringSystem(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        geo_models.LOCAL_DB = TEST_DB
        journey_models.LOCAL_DB = TEST_DB
        import app.services.alert_service as as_module
        as_module.LOCAL_DB = TEST_DB
        init_geofence_database()
        init_journey_database(TEST_DB)
        alert_service._ensure_tables()

        # Isolate unit tests
        import app.geofencing.models as gm
        gm.sync_geofence_to_firestore = lambda *args, **kwargs: None
        gm.sync_event_to_firestore = lambda *args, **kwargs: None
        ga.sync_alert_to_firestore = lambda *args, **kwargs: None
        ga.sync_event_to_firestore = lambda *args, **kwargs: None

    @classmethod
    def tearDownClass(cls):
        try:
            if TEST_DB.exists():
                TEST_DB.unlink()
        except Exception:
            pass

    def setUp(self):
        self.device_id = f"WATCH_{uuid.uuid4().hex[:6]}"
        self.device_id_2 = f"WATCH_{uuid.uuid4().hex[:6]}"
        self.ws_messages = []
        self._orig_broadcast = ws_manager.broadcast
        ws_manager.broadcast = lambda msg: self.ws_messages.append(msg)

    def tearDown(self):
        ws_manager.broadcast = self._orig_broadcast

    def time_iso(self, offset_seconds: int = 0) -> str:
        dt = datetime.now(timezone.utc) + timedelta(seconds=offset_seconds)
        return dt.isoformat()

    # -------------------------------------------------------------------------
    # Scenario 1: Inside -> Outside geofence transition
    # -------------------------------------------------------------------------
    def test_01_inside_to_outside_geofence_transition(self):
        geo = geofence_service.create_geofence(GeofenceCreate(
            name="HQ Compound",
            center_latitude=17.4400,
            center_longitude=78.3800,
            radius_meters=200.0,
            device_ids=[self.device_id],
            fence_type="INCLUSION"
        ))
        t0 = self.time_iso(-100)
        # Initial reading inside
        init_res = evaluate_geofence(self.device_id, 17.4401, 78.3801, t0)
        self.assertEqual(init_res[0]["state"], "INSIDE")
        self.assertIsNone(init_res[0]["event"])

        # Reading 1 outside (candidate)
        t1 = self.time_iso(-80)
        r1 = evaluate_geofence(self.device_id, 17.4500, 78.3900, t1)
        self.assertFalse(r1[0]["is_transition"])

        # Reading 2 outside (confirmed exit!)
        t2 = self.time_iso(-60)
        r2 = evaluate_geofence(self.device_id, 17.4510, 78.3910, t2)
        self.assertTrue(r2[0]["is_transition"])
        self.assertEqual(r2[0]["event"], "GEOFENCE_EXIT")
        self.assertEqual(r2[0]["state"], "OUTSIDE")

        # Notification display title check
        self.assertIsNotNone(r2[0]["alert"])
        self.assertEqual(r2[0]["alert"]["title"], "Outside Geofence")
        self.assertTrue(r2[0]["alert"]["requires_alarm"])

    # -------------------------------------------------------------------------
    # Scenario 2: Outside -> Inside geofence transition
    # -------------------------------------------------------------------------
    def test_02_outside_to_inside_geofence_transition(self):
        geo = geofence_service.create_geofence(GeofenceCreate(
            name="Safe Station",
            center_latitude=17.4400,
            center_longitude=78.3800,
            radius_meters=200.0,
            device_ids=[self.device_id],
            fence_type="INCLUSION"
        ))
        t0 = self.time_iso(-100)
        # Initial reading outside
        init_res = evaluate_geofence(self.device_id, 17.4600, 78.4000, t0)
        self.assertEqual(init_res[0]["state"], "OUTSIDE")

        # Reading 1 inside (candidate)
        t1 = self.time_iso(-80)
        r1 = evaluate_geofence(self.device_id, 17.4401, 78.3801, t1)
        self.assertFalse(r1[0]["is_transition"])

        # Reading 2 inside (confirmed entry/reentry!)
        t2 = self.time_iso(-60)
        r2 = evaluate_geofence(self.device_id, 17.4402, 78.3802, t2)
        self.assertTrue(r2[0]["is_transition"])
        self.assertIn(r2[0]["event"], ("GEOFENCE_ENTRY", "GEOFENCE_REENTRY"))
        self.assertEqual(r2[0]["state"], "INSIDE")

        # Notification display title check
        self.assertIsNotNone(r2[0]["alert"])
        self.assertEqual(r2[0]["alert"]["title"], "Inside Geofence")

    # -------------------------------------------------------------------------
    # Scenario 3: INCLUSION and EXCLUSION alert severity
    # -------------------------------------------------------------------------
    def test_03_inclusion_and_exclusion_alert_severity(self):
        # 1. INCLUSION: Exiting is a violation (HIGH severity + alarm)
        dev_inc = f"INC_{uuid.uuid4().hex[:4]}"
        geofence_service.create_geofence(GeofenceCreate(
            name="Inclusion Zone",
            center_latitude=17.4400,
            center_longitude=78.3800,
            radius_meters=200.0,
            device_ids=[dev_inc],
            fence_type="INCLUSION"
        ))
        evaluate_geofence(dev_inc, 17.4401, 78.3801, self.time_iso(-50))
        evaluate_geofence(dev_inc, 17.4600, 78.4000, self.time_iso(-40))
        res_inc = evaluate_geofence(dev_inc, 17.4605, 78.4005, self.time_iso(-30))
        self.assertEqual(res_inc[0]["event"], "GEOFENCE_EXIT")
        self.assertEqual(res_inc[0]["severity"], "HIGH")
        self.assertTrue(res_inc[0]["alert"]["requires_alarm"])

        # 2. EXCLUSION: Entering is a violation (HIGH severity + alarm)
        dev_exc = f"EXC_{uuid.uuid4().hex[:4]}"
        geofence_service.create_geofence(GeofenceCreate(
            name="Exclusion Zone",
            center_latitude=17.4400,
            center_longitude=78.3800,
            radius_meters=200.0,
            device_ids=[dev_exc],
            fence_type="EXCLUSION"
        ))
        # Initial outside (safe)
        evaluate_geofence(dev_exc, 17.4600, 78.4000, self.time_iso(-50))
        # Enters exclusion zone (breach!)
        evaluate_geofence(dev_exc, 17.4401, 78.3801, self.time_iso(-40))
        res_exc = evaluate_geofence(dev_exc, 17.4402, 78.3802, self.time_iso(-30))
        self.assertIn(res_exc[0]["event"], ("GEOFENCE_ENTRY", "GEOFENCE_REENTRY"))
        self.assertEqual(res_exc[0]["severity"], "HIGH")
        self.assertTrue(res_exc[0]["alert"]["requires_alarm"])

    # -------------------------------------------------------------------------
    # Scenario 4: Route selection and persistence
    # -------------------------------------------------------------------------
    def test_04_route_selection_and_persistence(self):
        jid = f"journey_test_{uuid.uuid4().hex[:8]}"
        create_journey_record({
            "journey_id": jid,
            "device_id": self.device_id,
            "origin_name": "Hitech City",
            "destination_name": "Bachupally",
            "status": "SCHEDULED"
        })

        routes = osrm_service.get_alternative_routes(
            origin_lat=17.4435,
            origin_lon=78.3812,
            dest_lat=17.5418,
            dest_lon=78.3654,
        )
        self.assertGreaterEqual(len(routes), 2)

        for r in routes:
            save_journey_route(
                journey_id=jid,
                route_name=r["name"],
                geometry=r["geometry"],
                distance_meters=r["distance_meters"],
                duration_seconds=r["duration_seconds"],
                safety_assessment=r.get("safety_assessment"),
                route_id=r["route_id"]
            )

        # Select Route A (Via Nizampet)
        selected = select_journey_route(jid, "route_a", approved_by="Officer Raman")
        self.assertIsNotNone(selected)
        self.assertEqual(selected["route_id"], "route_a")
        self.assertTrue(selected["is_selected"])

        # Verify persisted state in DB
        j_db = get_journey_by_id(jid)
        self.assertEqual(j_db["selected_route_id"], "route_a")
        approved_route = get_selected_route_for_journey(jid)
        self.assertIsNotNone(approved_route)
        self.assertEqual(approved_route["route_id"], "route_a")
        self.assertEqual(approved_route["approved_by"], "Officer Raman")

    # -------------------------------------------------------------------------
    # Scenario 5: Valid movement along the approved route
    # -------------------------------------------------------------------------
    def test_05_valid_movement_along_approved_route(self):
        jid = f"journey_valid_{uuid.uuid4().hex[:8]}"
        create_journey_record({
            "journey_id": jid,
            "device_id": self.device_id,
            "status": "ACTIVE",
            "route_corridor_meters": 75.0,
            "start_time": self.time_iso(-100)
        })

        # Simple 2-point route
        route_geom = {
            "type": "LineString",
            "coordinates": [[78.3800, 17.5000], [78.3800, 17.5200]]
        }
        save_journey_route(jid, "Main Road", route_geom, is_selected=True, route_id="route_valid")
        select_journey_route(jid, "route_valid")

        # Point precisely on route (0m deviation)
        p1 = route_monitor.evaluate_gps_position(
            device_id=self.device_id,
            latitude=17.5050,
            longitude=78.3800,
            timestamp=self.time_iso(-50),
            speed_kmh=40.0
        )
        self.assertIsNotNone(p1)
        self.assertTrue(p1["on_selected_route"])
        self.assertLess(p1["distance_from_route_meters"], 5.0)

        # Point slightly offset but within corridor (30m away < 75m)
        # 0.0003 deg lon ~ 31m at lat 17.5
        p2 = route_monitor.evaluate_gps_position(
            device_id=self.device_id,
            latitude=17.5100,
            longitude=78.3803,
            timestamp=self.time_iso(-40),
            speed_kmh=42.0
        )
        self.assertTrue(p2["on_selected_route"])
        self.assertLessEqual(p2["distance_from_route_meters"], 75.0)
        self.assertFalse(p2["deviation_active"])

    # -------------------------------------------------------------------------
    # Scenario 6: Confirmed route deviation
    # -------------------------------------------------------------------------
    def test_06_confirmed_route_deviation(self):
        jid = f"journey_dev_{uuid.uuid4().hex[:8]}"
        create_journey_record({
            "journey_id": jid,
            "device_id": self.device_id,
            "status": "ACTIVE",
            "route_corridor_meters": 75.0,
            "start_time": self.time_iso(-100)
        })
        route_geom = {
            "type": "LineString",
            "coordinates": [[78.3800, 17.5000], [78.3800, 17.5200]]
        }
        save_journey_route(jid, "Main Corridor", route_geom, is_selected=True, route_id="route_c")
        select_journey_route(jid, "route_c")

        # Reading 1: Deviated > 150m (0.002 deg lon ~ 212m) - candidate
        p1 = route_monitor.evaluate_gps_position(
            device_id=self.device_id,
            latitude=17.5050,
            longitude=78.3820,
            timestamp=self.time_iso(-30)
        )
        self.assertFalse(p1["on_selected_route"])
        self.assertGreater(p1["distance_from_route_meters"], 75.0)
        self.assertFalse(p1["deviation_active"])  # 1st reading not confirmed yet

        # Reading 2: Still deviated (2nd consecutive reading -> confirmed deviation!)
        p2 = route_monitor.evaluate_gps_position(
            device_id=self.device_id,
            latitude=17.5060,
            longitude=78.3822,
            timestamp=self.time_iso(-20)
        )
        self.assertFalse(p2["on_selected_route"])
        self.assertTrue(p2["deviation_active"])

        # Check recorded event in DB
        events = get_journey_events(jid)
        dev_events = [e for e in events if e["event_type"] == "ROUTE_DEVIATION_ALERT"]
        self.assertGreaterEqual(len(dev_events), 1)

    # -------------------------------------------------------------------------
    # Scenario 7: Device returning to the route (ROUTE_REJOINED)
    # -------------------------------------------------------------------------
    def test_07_device_returning_to_the_route(self):
        jid = f"journey_rejoin_{uuid.uuid4().hex[:8]}"
        create_journey_record({
            "journey_id": jid,
            "device_id": self.device_id,
            "status": "ACTIVE",
            "route_corridor_meters": 75.0,
            "start_time": self.time_iso(-100)
        })
        route_geom = {
            "type": "LineString",
            "coordinates": [[78.3800, 17.5000], [78.3800, 17.5200]]
        }
        save_journey_route(jid, "Rejoin Route", route_geom, is_selected=True, route_id="route_r")
        select_journey_route(jid, "route_r")

        # Establish deviation
        route_monitor.evaluate_gps_position(self.device_id, 17.5050, 78.3820, self.time_iso(-50))
        route_monitor.evaluate_gps_position(self.device_id, 17.5060, 78.3820, self.time_iso(-40))

        # Device returns to route corridor: Reading 1 inside (candidate)
        p1 = route_monitor.evaluate_gps_position(self.device_id, 17.5070, 78.3801, self.time_iso(-30))
        self.assertTrue(p1["on_selected_route"])
        self.assertTrue(p1["deviation_active"])  # Still active until confirmed

        # Reading 2 inside (confirmed rejoin!)
        p2 = route_monitor.evaluate_gps_position(self.device_id, 17.5080, 78.3800, self.time_iso(-20))
        self.assertTrue(p2["on_selected_route"])
        self.assertFalse(p2["deviation_active"])  # Cleared!

        events = get_journey_events(jid)
        rejoined_events = [e for e in events if e["event_type"] == "ROUTE_REJOINED"]
        self.assertGreaterEqual(len(rejoined_events), 1)

    # -------------------------------------------------------------------------
    # Scenario 8: Unauthorized early departure
    # -------------------------------------------------------------------------
    def test_08_unauthorized_early_departure(self):
        jid = f"journey_early_{uuid.uuid4().hex[:8]}"
        sched_dep = self.time_iso(1800)  # Scheduled 30 minutes in the future
        create_journey_record({
            "journey_id": jid,
            "device_id": self.device_id,
            "status": "SCHEDULED",
            "start_latitude": 17.4435,
            "start_longitude": 78.3812,
            "scheduled_departure": sched_dep
        })

        # Device moves 1 km away from origin before scheduled departure time
        now_ts = self.time_iso(0)
        route_monitor.evaluate_gps_position(
            device_id=self.device_id,
            latitude=17.4600,
            longitude=78.3900,
            timestamp=now_ts
        )

        events = get_journey_events(jid)
        early_events = [e for e in events if e["event_type"] == "UNAUTHORIZED_EARLY_DEPARTURE"]
        self.assertGreaterEqual(len(early_events), 1)

    # -------------------------------------------------------------------------
    # Scenario 9: Delayed journey start
    # -------------------------------------------------------------------------
    def test_09_delayed_journey_start(self):
        jid = f"journey_delay_{uuid.uuid4().hex[:8]}"
        sched_dep = self.time_iso(-600)  # Scheduled 10 minutes ago
        create_journey_record({
            "journey_id": jid,
            "device_id": self.device_id,
            "status": "SCHEDULED",
            "start_latitude": 17.4435,
            "start_longitude": 78.3812,
            "scheduled_departure": sched_dep
        })

        # Device is still at origin 10 minutes after scheduled departure
        now_ts = self.time_iso(0)
        route_monitor.evaluate_gps_position(
            device_id=self.device_id,
            latitude=17.4435,
            longitude=78.3812,
            timestamp=now_ts
        )

        events = get_journey_events(jid)
        delayed_events = [e for e in events if e["event_type"] == "JOURNEY_START_DELAYED"]
        self.assertGreaterEqual(len(delayed_events), 1)

    # -------------------------------------------------------------------------
    # Scenario 10: Overdue arrival
    # -------------------------------------------------------------------------
    def test_10_overdue_arrival(self):
        jid = f"journey_overdue_{uuid.uuid4().hex[:8]}"
        sched_arr = self.time_iso(-300)  # Scheduled arrival was 5 minutes ago
        create_journey_record({
            "journey_id": jid,
            "device_id": self.device_id,
            "status": "ACTIVE",
            "start_time": self.time_iso(-1800),
            "end_latitude": 17.5418,
            "end_longitude": 78.3654,
            "scheduled_arrival": sched_arr
        })

        # Device is still at midpoint (not yet at destination) after arrival deadline
        route_monitor.evaluate_gps_position(
            device_id=self.device_id,
            latitude=17.5000,
            longitude=78.3750,
            timestamp=self.time_iso(0)
        )

        events = get_journey_events(jid)
        overdue_events = [e for e in events if e["event_type"] == "JOURNEY_OVERDUE"]
        self.assertGreaterEqual(len(overdue_events), 1)

    # -------------------------------------------------------------------------
    # Scenario 11: GPS signal loss
    # -------------------------------------------------------------------------
    def test_11_gps_signal_loss(self):
        jid = f"journey_loss_{uuid.uuid4().hex[:8]}"
        create_journey_record({
            "journey_id": jid,
            "device_id": self.device_id,
            "status": "ACTIVE",
            "start_time": self.time_iso(-500)
        })

        # Handle signal loss
        route_monitor.handle_device_gps_loss(self.device_id, reason="TUNNEL_TRANSIT")

        events = get_journey_events(jid)
        loss_events = [e for e in events if e["event_type"] == "GPS_SIGNAL_LOSS"]
        self.assertGreaterEqual(len(loss_events), 1)

        # Confirm NO false route deviation was created!
        dev_events = [e for e in events if e["event_type"] == "ROUTE_DEVIATION_ALERT"]
        self.assertEqual(len(dev_events), 0)

    # -------------------------------------------------------------------------
    # Scenario 12: Duplicate GPS packets
    # -------------------------------------------------------------------------
    def test_12_duplicate_gps_packets(self):
        jid = f"journey_dup_{uuid.uuid4().hex[:8]}"
        create_journey_record({
            "journey_id": jid,
            "device_id": self.device_id,
            "status": "ACTIVE",
            "start_time": self.time_iso(-500)
        })

        ts = self.time_iso(-100)
        # First packet accepted
        p1 = route_monitor.evaluate_gps_position(self.device_id, 17.4500, 78.3800, ts)
        self.assertIsNotNone(p1)

        # Duplicate packet with same timestamp ignored
        p2 = route_monitor.evaluate_gps_position(self.device_id, 17.4500, 78.3800, ts)
        self.assertIsNone(p2)

        # Points in DB should be exactly 1
        points = get_journey_gps_points(jid)
        self.assertEqual(len(points), 1)

    # -------------------------------------------------------------------------
    # Scenario 13: Out-of-order GPS timestamps
    # -------------------------------------------------------------------------
    def test_13_out_of_order_gps_timestamps(self):
        jid = f"journey_ooo_{uuid.uuid4().hex[:8]}"
        create_journey_record({
            "journey_id": jid,
            "device_id": self.device_id,
            "status": "ACTIVE",
            "start_time": self.time_iso(-500)
        })

        t_new = self.time_iso(-50)
        t_old = self.time_iso(-80)

        # Ingest newer packet first
        p1 = route_monitor.evaluate_gps_position(self.device_id, 17.4500, 78.3800, t_new)
        self.assertIsNotNone(p1)

        # Ingest older packet arriving late
        p2 = route_monitor.evaluate_gps_position(self.device_id, 17.4490, 78.3790, t_old)
        self.assertIsNone(p2)

        # Points in DB should be exactly 1
        points = get_journey_gps_points(jid)
        self.assertEqual(len(points), 1)

    # -------------------------------------------------------------------------
    # Scenario 14: Multiple devices and simultaneous journeys
    # -------------------------------------------------------------------------
    def test_14_multiple_devices_and_simultaneous_journeys(self):
        dev_a = f"DEV_A_{uuid.uuid4().hex[:4]}"
        dev_b = f"DEV_B_{uuid.uuid4().hex[:4]}"

        jid_a = f"journey_{dev_a}_sim"
        jid_b = f"journey_{dev_b}_sim"

        create_journey_record({
            "journey_id": jid_a,
            "device_id": dev_a,
            "status": "ACTIVE",
            "route_corridor_meters": 75.0,
            "start_time": self.time_iso(-500)
        })
        create_journey_record({
            "journey_id": jid_b,
            "device_id": dev_b,
            "status": "ACTIVE",
            "route_corridor_meters": 75.0,
            "start_time": self.time_iso(-500)
        })

        route_a = {"type": "LineString", "coordinates": [[78.3800, 17.5000], [78.3800, 17.5200]]}
        route_b = {"type": "LineString", "coordinates": [[78.4200, 17.4000], [78.4200, 17.4200]]}

        save_journey_route(jid_a, "Route A", route_a, is_selected=True, route_id="ra")
        save_journey_route(jid_b, "Route B", route_b, is_selected=True, route_id="rb")
        select_journey_route(jid_a, "ra")
        select_journey_route(jid_b, "rb")

        # Dev A moves on its route
        pa = route_monitor.evaluate_gps_position(dev_a, 17.5050, 78.3800, self.time_iso(-50))
        self.assertTrue(pa["on_selected_route"])

        # Dev B moves on its route
        pb = route_monitor.evaluate_gps_position(dev_b, 17.4050, 78.4200, self.time_iso(-50))
        self.assertTrue(pb["on_selected_route"])

        # Verify point isolation
        pts_a = get_journey_gps_points(jid_a)
        pts_b = get_journey_gps_points(jid_b)
        self.assertEqual(len(pts_a), 1)
        self.assertEqual(len(pts_b), 1)
        self.assertEqual(pts_a[0]["device_id"], dev_a)
        self.assertEqual(pts_b[0]["device_id"], dev_b)

    # -------------------------------------------------------------------------
    # Scenario 15: WebSocket notification and database persistence
    # -------------------------------------------------------------------------
    def test_15_websocket_notification_and_database_persistence(self):
        jid = f"journey_ws_{uuid.uuid4().hex[:8]}"
        create_journey_record({
            "journey_id": jid,
            "device_id": self.device_id,
            "status": "ACTIVE",
            "route_corridor_meters": 75.0,
            "start_time": self.time_iso(-500)
        })
        route_geom = {
            "type": "LineString",
            "coordinates": [[78.3800, 17.5000], [78.3800, 17.5200]]
        }
        save_journey_route(jid, "WS Corridor", route_geom, is_selected=True, route_id="r_ws")
        select_journey_route(jid, "r_ws")

        self.ws_messages.clear()

        # Generate movement and confirmed deviation
        route_monitor.evaluate_gps_position(self.device_id, 17.5050, 78.3830, self.time_iso(-30))
        route_monitor.evaluate_gps_position(self.device_id, 17.5060, 78.3830, self.time_iso(-20))

        # Check WebSocket messages were broadcast
        ws_types = [m.get("type") for m in self.ws_messages]
        self.assertIn("JOURNEY_LIVE_UPDATE", ws_types)
        self.assertIn("ROUTE_DEVIATION_ALERT", ws_types)

        # Check database persistence
        events = get_journey_events(jid)
        self.assertGreaterEqual(len(events), 1)
        pts = get_journey_gps_points(jid)
        self.assertEqual(len(pts), 2)

    # -------------------------------------------------------------------------
    # Scenario 16: Audible Alarm (requires_alarm: True), Silence and Rejoin Clearing
    # -------------------------------------------------------------------------
    def test_16_audible_alarm_requires_alarm_flag_and_silence_workflow(self):
        """
        Verifies:
        1. Geofence violation generates alert with requires_alarm=True.
        2. Route deviation generates alert with requires_alarm=True.
        3. Silence endpoint silences the alarm (requires_alarm=False) while retaining alert in history.
        4. Route rejoin clears the deviation alarm and broadcasts requires_alarm=False.
        """
        dev = f"AUDIBLE_{uuid.uuid4().hex[:6]}"
        self.ws_messages.clear()

        # 1. Geofence violation audible alarm
        geo_alert = ga.trigger_geofence_alert(
            event_id=f"evt_{uuid.uuid4().hex[:6]}",
            device_id=dev,
            geofence_id="geo_test_1",
            geofence_name="Secure Yard",
            event_type="GEOFENCE_EXIT",
            latitude=17.4410,
            longitude=78.3610,
            timestamp=self.time_iso(-100),
            severity="HIGH",
            fence_type="INCLUSION"
        )
        self.assertTrue(geo_alert["requires_alarm"])

        # Check WebSocket message has requires_alarm: True
        geo_ws = [m for m in self.ws_messages if m.get("type") == "GEOFENCE_EXIT"]
        self.assertTrue(len(geo_ws) > 0)
        self.assertTrue(geo_ws[-1]["requires_alarm"])

        # Check geofence alert listing has requires_alarm: True
        active_geo_alerts = ga.list_geofence_alerts(device_id=dev, status="ACTIVE")
        self.assertTrue(len(active_geo_alerts) > 0)
        self.assertTrue(active_geo_alerts[0]["requires_alarm"])

        # 2. Silence geofence alert -> requires_alarm becomes False, alert retained in history
        silenced_geo = ga.silence_geofence_alert(geo_alert["id"], officer_email="officer@police.gov")
        self.assertIsNotNone(silenced_geo)
        self.assertFalse(silenced_geo["requires_alarm"])
        self.assertEqual(silenced_geo["status"], "ACKNOWLEDGED")

        # Verify it still exists in alert history
        all_geo = ga.list_geofence_alerts(device_id=dev)
        self.assertTrue(any(a["id"] == geo_alert["id"] for a in all_geo))

        # Check WebSocket broadcast for silence
        silence_ws = [m for m in self.ws_messages if m.get("type") == "GEOFENCE_ALERT_ACKNOWLEDGED"]
        self.assertTrue(len(silence_ws) > 0)
        self.assertFalse(silence_ws[-1]["requires_alarm"])

        # 3. Route deviation audible alarm
        jid = f"journey_aud_{uuid.uuid4().hex[:8]}"
        create_journey_record({
            "journey_id": jid,
            "device_id": dev,
            "status": "ACTIVE",
            "route_corridor_meters": 75.0,
            "start_time": self.time_iso(-500)
        })
        route_geom = {
            "type": "LineString",
            "coordinates": [[78.3800, 17.5000], [78.3800, 17.5200]]
        }
        save_journey_route(jid, "Audible Corridor", route_geom, is_selected=True, route_id="r_aud")
        select_journey_route(jid, "r_aud")

        self.ws_messages.clear()

        # Two consecutive readings outside corridor -> confirmed deviation
        route_monitor.evaluate_gps_position(dev, 17.5050, 78.3840, self.time_iso(-60))
        route_monitor.evaluate_gps_position(dev, 17.5060, 78.3840, self.time_iso(-50))

        # Check WebSocket route deviation message has requires_alarm: True
        dev_ws = [m for m in self.ws_messages if m.get("type") == "ROUTE_DEVIATION_ALERT"]
        self.assertTrue(len(dev_ws) > 0)
        self.assertTrue(dev_ws[-1].get("requires_alarm"))

        # Check alert_service has active deviation alert with requires_alarm: True
        active_alerts = alert_service.list_alerts(device_id=dev, status="ACTIVE", alert_type="ROUTE_DEVIATION_ALERT")
        self.assertTrue(len(active_alerts) > 0)
        self.assertTrue(active_alerts[0]["requires_alarm"])
        route_alert_id = active_alerts[0]["id"]

        # Silence the route deviation alert
        silenced_route = alert_service.silence_alert(route_alert_id, silenced_by="dispatcher@police.gov")
        self.assertIsNotNone(silenced_route)
        self.assertFalse(silenced_route["requires_alarm"])
        self.assertEqual(silenced_route["status"], "ACKNOWLEDGED")

        # 4. Device returns to corridor -> ROUTE_REJOINED with requires_alarm: False
        self.ws_messages.clear()
        route_monitor.evaluate_gps_position(dev, 17.5070, 78.3800, self.time_iso(-30))
        route_monitor.evaluate_gps_position(dev, 17.5080, 78.3800, self.time_iso(-20))

        rejoin_ws = [m for m in self.ws_messages if m.get("type") == "ROUTE_REJOINED"]
        self.assertTrue(len(rejoin_ws) > 0)
        self.assertFalse(rejoin_ws[-1].get("requires_alarm", True))

    # -------------------------------------------------------------------------
    # Scenario 17: Deduplication Prevents Duplicate Audible Alarms
    # -------------------------------------------------------------------------
    def test_17_deduplication_prevents_duplicate_ongoing_alarms(self):
        """Verifies ongoing violation does not flood duplicates or multiple active records."""
        dev = f"DEDUP_{uuid.uuid4().hex[:6]}"
        jid = f"journey_dedup_{uuid.uuid4().hex[:8]}"
        create_journey_record({
            "journey_id": jid,
            "device_id": dev,
            "status": "ACTIVE",
            "route_corridor_meters": 75.0,
            "start_time": self.time_iso(-500)
        })
        route_geom = {
            "type": "LineString",
            "coordinates": [[78.3800, 17.5000], [78.3800, 17.5200]]
        }
        save_journey_route(jid, "Dedup Corridor", route_geom, is_selected=True, route_id="r_dedup")
        select_journey_route(jid, "r_dedup")

        self.ws_messages.clear()

        # Send 5 consecutive GPS points far off the route
        for i in range(5):
            route_monitor.evaluate_gps_position(dev, 17.5050 + (i * 0.001), 78.3850, self.time_iso(-100 + (i * 10)))

        # Count ROUTE_DEVIATION_ALERT WebSocket broadcasts: exactly 1 confirmed
        dev_broadcasts = [m for m in self.ws_messages if m.get("type") == "ROUTE_DEVIATION_ALERT"]
        self.assertEqual(len(dev_broadcasts), 1)

        # Count active alerts in database: exactly 1
        alerts = alert_service.list_alerts(device_id=dev, alert_type="ROUTE_DEVIATION_ALERT")
        self.assertEqual(len(alerts), 1)


if __name__ == "__main__":
    unittest.main()
