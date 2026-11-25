"""
Regression: approved-route journeys must not be overwritten, shadowed or
auto-completed by the automatic journey tracker, and GPS points that arrive via
the Firestore history sync must reach the route monitor.

Synthetic device IDs only; the isolated temp DB comes from tests/conftest.py.
"""

import sqlite3
import tempfile
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from app.journeys import models as jm
from app.journeys.models import (
    create_journey_record,
    get_active_journey_for_device,
    get_journey_by_id,
    get_journey_gps_points,
    get_selected_route_for_journey,
    init_journey_database,
    save_journey_route,
    select_journey_route,
    update_journey_record,
)
from app.journeys.route_monitor import route_monitor
from app.services.alert_service import ws_manager
from app.services.firestore_service import firestore_service
from app.services.journey_service import journey_service

ROUTE = {"type": "LineString", "coordinates": [[78.3800, 17.5000], [78.3800, 17.5200]]}
ON_ROUTE = (17.5050, 78.3800)
OFF_ROUTE = [(17.5060, 78.3850), (17.5070, 78.3852), (17.5080, 78.3855)]  # ~530 m east


def iso(offset_seconds: float) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=offset_seconds)).isoformat()


class RouteJourneyIsolationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Other test modules reassign module-level LOCAL_DB globals at import time and
        # never restore them; pin every module used here to one private temp DB.
        # import_module returns the real module; `import app.services.x as m` can return
        # the same-named singleton re-exported by app/services/__init__.py.
        from importlib import import_module
        from app.services.alert_service import alert_service

        fs_module = import_module("app.services.firestore_service")
        modules = [
            jm,
            import_module("app.geofencing.models"),
            import_module("app.services.alert_service"),
            fs_module,
            import_module("app.services.journey_service"),
        ]
        cls._tmp = tempfile.TemporaryDirectory(prefix="route_journey_")
        cls.db = Path(cls._tmp.name) / "route_journey.db"
        cls._patches = [patch.object(module, "LOCAL_DB", cls.db) for module in modules]
        for p in cls._patches:
            p.start()
        fs_module.firestore_service._init_local_db()
        import_module("app.geofencing.models").init_geofence_database()
        init_journey_database()
        alert_service._ensure_tables()

    @classmethod
    def tearDownClass(cls):
        for p in reversed(cls._patches):
            p.stop()
        cls._tmp.cleanup()

    def setUp(self):
        firestore_service._active_journey_cache.clear()
        self.dev = f"SYNTH_RJ_{uuid.uuid4().hex[:8]}"
        self.ws = []
        self.ws_patch = patch.object(ws_manager, "broadcast", lambda msg: self.ws.append(msg))
        self.ws_patch.start()

    def tearDown(self):
        self.ws_patch.stop()
        journey_service._active_journeys.pop(self.dev, None)

    # ---------- helpers ----------
    def _route_journey(self, status="ACTIVE", start_offset=-600, route_id="route_a", **extra):
        jid = f"journey_{self.dev}_{uuid.uuid4().hex[:8]}"
        create_journey_record({
            "journey_id": jid, "device_id": self.dev, "status": status,
            "route_corridor_meters": 75.0, "start_time": iso(start_offset), **extra,
        })
        save_journey_route(jid, "Approved", ROUTE, is_selected=True, route_id=route_id)
        select_journey_route(jid, route_id)
        return jid

    def _insert_synced_telemetry(self, points):
        conn = sqlite3.connect(str(self.db))
        for (lat, lon), ts in points:
            conn.execute(
                "INSERT INTO telemetry (device_id, timestamp, received_at, gps_valid, latitude, longitude, speed_kmh, gps_source)"
                " VALUES (?, ?, ?, 1, ?, ?, 12.0, 'watch_frame')",
                (self.dev, ts, ts, lat, lon),
            )
        conn.commit()
        conn.close()

    def _telemetry_journey_ids(self):
        conn = sqlite3.connect(str(self.db))
        ids = {r[0] for r in conn.execute("SELECT journey_id FROM telemetry WHERE device_id = ?", (self.dev,))}
        conn.close()
        return ids

    def _auto_journeys(self):
        conn = sqlite3.connect(str(self.db))
        rows = conn.execute(
            "SELECT journey_id FROM journeys WHERE device_id = ? AND (selected_route_id IS NULL OR selected_route_id = '')",
            (self.dev,),
        ).fetchall()
        conn.close()
        return rows

    # ---------- route storage ----------
    def test_same_route_id_in_two_journeys_does_not_overwrite(self):
        first = self._route_journey(status="COMPLETED", end_time=iso(-60))
        second = self._route_journey()
        self.assertIsNotNone(get_selected_route_for_journey(first))
        self.assertIsNotNone(get_selected_route_for_journey(second))
        self.assertEqual(get_selected_route_for_journey(first)["route_id"], "route_a")

    def test_route_of_another_journey_cannot_be_selected(self):
        other = self._route_journey(route_id="route_only_other")
        jid = f"journey_{self.dev}_{uuid.uuid4().hex[:8]}"
        create_journey_record({"journey_id": jid, "device_id": self.dev, "status": "SCHEDULED"})
        self.assertIsNone(select_journey_route(jid, "route_only_other"))
        self.assertIsNotNone(get_selected_route_for_journey(other))

    def test_migration_rekeys_legacy_table_and_keeps_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "legacy.db"
            conn = sqlite3.connect(str(db))
            conn.execute("""CREATE TABLE journey_routes (id TEXT PRIMARY KEY, journey_id TEXT NOT NULL,
                route_name TEXT NOT NULL, route_geometry TEXT NOT NULL, distance_meters REAL,
                duration_seconds REAL, is_selected INTEGER DEFAULT 0, approved_by TEXT, approved_at TEXT,
                safety_assessment TEXT, created_at TEXT NOT NULL)""")
            conn.execute("INSERT INTO journey_routes (id, journey_id, route_name, route_geometry, is_selected, created_at)"
                         " VALUES ('route_a', 'J_OLD', 'Legacy', '{}', 1, ?)", (iso(0),))
            conn.commit()
            conn.close()
            init_journey_database(db)
            conn = sqlite3.connect(str(db))
            pk = [r[1] for r in conn.execute("PRAGMA table_info(journey_routes)") if r[5]]
            rows = conn.execute("SELECT id, journey_id FROM journey_routes").fetchall()
            conn.close()
            self.assertEqual(sorted(pk), ["id", "journey_id"])
            self.assertEqual(rows, [("route_a", "J_OLD")])

    # ---------- automatic tracker must not touch route journeys ----------
    def test_idle_route_journey_is_not_auto_completed_by_active_lookup(self):
        jid = self._route_journey(start_offset=-7200)
        update_journey_record(jid, {"updated_at": iso(-7200)})
        firestore_service._active_journey_cache.pop(self.dev, None)
        journey = journey_service.get_active_journey(self.dev)
        self.assertEqual(get_journey_by_id(jid)["status"], "ACTIVE")
        self.assertIsNotNone(journey)
        self.assertEqual(journey["journey_id"], jid)

    def test_live_point_does_not_create_auto_journey_over_scheduled_route_journey(self):
        jid = self._route_journey(status="SCHEDULED", start_offset=-60)
        result = journey_service.record_journey_point(self.dev, ON_ROUTE[0], ON_ROUTE[1], iso(-5), speed_kmh=10)
        self.assertIsNone(result)
        self.assertEqual(self._auto_journeys(), [])
        self.assertEqual(get_active_journey_for_device(self.dev)["journey_id"], jid)

    def test_officer_completion_is_respected(self):
        jid = self._route_journey()
        self.assertEqual(journey_service.get_active_journey(self.dev)["journey_id"], jid)
        update_journey_record(jid, {"status": "COMPLETED", "end_time": iso(0)})
        self.assertIsNone(journey_service.get_active_journey(self.dev))

    # ---------- Firestore-synced points reach the route monitor ----------
    def test_synced_points_in_active_route_journey_raise_deviation(self):
        jid = self._route_journey(start_offset=-600)
        route_monitor.reset_journey_state(jid)
        points = [(ON_ROUTE, iso(-120))] + [(p, iso(-100 + i * 10)) for i, p in enumerate(OFF_ROUTE)]
        self._insert_synced_telemetry(points)

        journey_service.process_unassigned_telemetry(self.dev)

        self.assertEqual(self._auto_journeys(), [], "synced points must not create an automatic journey")
        self.assertEqual(self._telemetry_journey_ids(), {jid})
        recorded = get_journey_gps_points(jid)
        self.assertEqual(len(recorded), 4)
        self.assertTrue(recorded[0]["on_route"])
        self.assertFalse(recorded[-1]["on_route"])
        deviation = [m for m in self.ws if m.get("type") == "ROUTE_DEVIATION_ALERT"]
        self.assertEqual(len(deviation), 1)
        self.assertEqual(deviation[0]["journey_id"], jid)
        # Red-dot inputs: verified, current deviation coordinates on the live broadcast...
        self.assertIs(deviation[0]["gps_valid"], True)
        self.assertIs(deviation[0]["location_is_stale"], False)
        self.assertEqual((deviation[0]["latitude"], deviation[0]["longitude"]), OFF_ROUTE[1])
        # ...and on the persisted alert returned by the API.
        from app.services.alert_service import alert_service
        stored = alert_service.list_alerts(device_id=self.dev, alert_type="ROUTE_DEVIATION_ALERT")
        self.assertEqual(len(stored), 1)
        self.assertIs(stored[0]["gps_valid"], True)
        self.assertIs(stored[0]["location_is_stale"], False)
        self.assertEqual((stored[0]["latitude"], stored[0]["longitude"]), OFF_ROUTE[1])

    def test_old_history_in_completed_route_journey_is_recorded_without_alarm(self):
        # Fixes older than GPS_FIX_MAX_AGE_SECONDS (900 s) are history, never alarms.
        jid = self._route_journey(status="COMPLETED", start_offset=-3600, end_time=iso(-1500))
        points = [(ON_ROUTE, iso(-3000))] + [(p, iso(-2900 + i * 10)) for i, p in enumerate(OFF_ROUTE)]
        self._insert_synced_telemetry(points)

        journey_service.process_unassigned_telemetry(self.dev)

        self.assertEqual(self._auto_journeys(), [])
        recorded = get_journey_gps_points(jid)
        self.assertEqual([p["on_route"] for p in recorded], [True, False, False, False])
        self.assertFalse([m for m in self.ws if m.get("type") == "ROUTE_DEVIATION_ALERT"])

    def test_current_points_synced_after_completion_still_raise_one_deviation(self):
        # Officer completed the journey before the Firestore sync delivered its last points.
        jid = self._route_journey(status="COMPLETED", start_offset=-600, end_time=iso(-30))
        route_monitor.reset_journey_state(jid)
        points = [(ON_ROUTE, iso(-200))] + [(p, iso(-150 + i * 10)) for i, p in enumerate(OFF_ROUTE)]
        self._insert_synced_telemetry(points)

        journey_service.process_unassigned_telemetry(self.dev)

        deviation = [m for m in self.ws if m.get("type") == "ROUTE_DEVIATION_ALERT"]
        self.assertEqual(len(deviation), 1)
        self.assertEqual(deviation[0]["journey_id"], jid)
        self.assertEqual(
            [e["event_type"] for e in jm.get_journey_events(jid)], ["ROUTE_DEVIATION_ALERT"]
        )

    def _geofence_around_start(self):
        from app.geofencing.schemas import GeofenceCreate
        from app.geofencing.service import geofence_service

        return geofence_service.create_geofence(GeofenceCreate(
            name="Synthetic Start Zone", center_latitude=17.4400, center_longitude=78.3800,
            radius_meters=100.0, device_ids=[self.dev], fence_type="INCLUSION",
        ))

    def _geofence_events(self):
        conn = sqlite3.connect(str(self.db))
        rows = [r[0] for r in conn.execute(
            "SELECT event_type FROM geofence_events WHERE device_id = ? ORDER BY event_time", (self.dev,)
        )]
        conn.close()
        return rows

    def test_synced_current_points_raise_geofence_exit(self):
        import app.geofencing.alerts as ga
        import app.geofencing.models as gm

        self._geofence_around_start()
        with patch.object(gm, "sync_event_to_firestore", lambda *a, **k: None), \
                patch.object(ga, "sync_alert_to_firestore", lambda *a, **k: None):
            self._insert_synced_telemetry([
                ((17.4401, 78.3801), iso(-90)),   # inside
                ((17.4500, 78.3900), iso(-80)),   # outside (candidate)
                ((17.4510, 78.3910), iso(-70)),   # outside (confirmed)
            ])
            journey_service.process_unassigned_telemetry(self.dev)
        self.assertEqual(self._geofence_events(), ["GEOFENCE_EXIT"])

    def test_synced_old_history_does_not_raise_geofence_events(self):
        self._geofence_around_start()
        self._insert_synced_telemetry([
            ((17.4401, 78.3801), iso(-3000)),
            ((17.4500, 78.3900), iso(-2990)),
            ((17.4510, 78.3910), iso(-2980)),
        ])
        journey_service.process_unassigned_telemetry(self.dev)
        self.assertEqual(self._geofence_events(), [])

    def test_sync_worker_pulls_only_devices_with_open_route_journeys(self):
        open_dev, closed_dev = self.dev, f"{self.dev}_CLOSED"
        self._route_journey(status="ACTIVE")
        jid = f"journey_{closed_dev}_x"
        create_journey_record({"journey_id": jid, "device_id": closed_dev, "status": "COMPLETED"})
        save_journey_route(jid, "Approved", ROUTE, is_selected=True, route_id="route_a")
        select_journey_route(jid, "route_a")
        calls = []
        with patch.object(firestore_service, "_sync_firestore_telemetry_history",
                          lambda device_id, limit, min_interval_seconds=None: calls.append((device_id, min_interval_seconds))):
            firestore_service.sync_route_journey_devices()
        self.assertIn((open_dev, firestore_service.ROUTE_JOURNEY_SYNC_INTERVAL_SECONDS), calls)
        self.assertNotIn(closed_dev, [device for device, _ in calls])

    def test_single_off_route_reading_is_not_a_confirmed_deviation(self):
        jid = self._route_journey(start_offset=-600)
        route_monitor.reset_journey_state(jid)
        self._insert_synced_telemetry([(ON_ROUTE, iso(-60)), (OFF_ROUTE[0], iso(-50)), (ON_ROUTE, iso(-40))])

        journey_service.process_unassigned_telemetry(self.dev)

        self.assertFalse([m for m in self.ws if m.get("type") == "ROUTE_DEVIATION_ALERT"])

    def test_points_outside_route_journeys_still_create_auto_journey(self):
        self._route_journey(status="COMPLETED", start_offset=-900, end_time=iso(-800))
        self._insert_synced_telemetry([((17.4400, 78.3600), iso(-200)), ((17.4410, 78.3610), iso(-190))])

        journey_service.process_unassigned_telemetry(self.dev)

        self.assertEqual(len(self._auto_journeys()), 1)


if __name__ == "__main__":
    unittest.main()
