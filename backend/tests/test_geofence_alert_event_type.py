"""
Regression: geofence alert history must carry event_type.

list_geofence_alerts previously derived event_type only from a LEFT JOIN on
geofence_events; when the event row was absent the label came back empty.
Uses the isolated temp DB from conftest.py and synthetic device IDs only.
"""

import sqlite3
import unittest
import uuid
from datetime import datetime, timezone
from unittest.mock import patch

import app.geofencing.alerts as ga
import app.geofencing.models as gm
from app.geofencing.alerts import list_geofence_alerts, trigger_geofence_alert
from app.geofencing.models import get_db_connection, init_geofence_database


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class GeofenceAlertEventTypeTests(unittest.TestCase):
    def setUp(self):
        init_geofence_database()
        self.device_id = f"SYNTH_GEO_{uuid.uuid4().hex[:8]}"
        self.patches = [
            patch.object(ga, "sync_alert_to_firestore", lambda *a, **k: None),
            patch.object(ga.ws_manager, "broadcast", lambda *a, **k: None),
            patch.object(ga.alert_service, "trigger_alert", lambda *a, **k: {"id": "core_synth"}),
        ]
        for p in self.patches:
            p.start()

    def tearDown(self):
        for p in reversed(self.patches):
            p.stop()

    def _insert_event(self, event_id: str, event_type: str) -> None:
        conn = get_db_connection()
        conn.execute(
            "INSERT INTO geofence_events (id, device_id, geofence_id, event_type, latitude, longitude, event_time, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (event_id, self.device_id, "geo_synth", event_type, 17.44, 78.38, _now(), _now()),
        )
        conn.commit()
        conn.close()

    def _trigger(self, event_id: str, event_type: str) -> None:
        trigger_geofence_alert(
            event_id=event_id, device_id=self.device_id, geofence_id="geo_synth",
            geofence_name="Synthetic Zone", event_type=event_type,
            latitude=17.44, longitude=78.38, timestamp=_now(), fence_type="INCLUSION",
        )

    def _only_alert(self):
        alerts = list_geofence_alerts(device_id=self.device_id)
        self.assertEqual(len(alerts), 1)
        return alerts[0]

    def test_event_type_from_joined_event(self):
        eid = f"geoevent_{uuid.uuid4().hex[:12]}"
        self._insert_event(eid, "GEOFENCE_EXIT")
        self._trigger(eid, "GEOFENCE_EXIT")
        alert = self._only_alert()
        self.assertEqual(alert["event_type"], "GEOFENCE_EXIT")
        self.assertIsNotNone(alert["event_time"])
        self.assertNotIn("resolved_event_type", alert)

    def test_event_type_survives_missing_event_row(self):
        self._trigger(f"geoevent_{uuid.uuid4().hex[:12]}", "GEOFENCE_REENTRY")
        alert = self._only_alert()
        self.assertEqual(alert["event_type"], "GEOFENCE_REENTRY")
        self.assertIsNotNone(alert["event_time"])

    def test_legacy_unmatched_alert_is_not_given_a_fabricated_type(self):
        conn = get_db_connection()
        conn.execute(
            "INSERT INTO geofence_alerts (id, event_id, device_id, geofence_id, severity, status, title, message, created_at, updated_at)"
            " VALUES (?, ?, ?, 'geo_synth', 'HIGH', 'ACTIVE', 'Outside Geofence', 'legacy', ?, ?)",
            (f"alert_geo_{uuid.uuid4().hex[:10]}", "evt_missing", self.device_id, _now(), _now()),
        )
        conn.commit()
        conn.close()
        alert = self._only_alert()
        self.assertIsNone(alert["event_type"])

    def test_migration_backfills_legacy_alert_from_existing_event(self):
        eid = f"geoevent_{uuid.uuid4().hex[:12]}"
        self._insert_event(eid, "GEOFENCE_ENTRY")
        conn = get_db_connection()
        conn.execute(
            "INSERT INTO geofence_alerts (id, event_id, device_id, geofence_id, severity, status, title, message, created_at, updated_at)"
            " VALUES (?, ?, ?, 'geo_synth', 'INFO', 'ACTIVE', 'Inside Geofence', 'legacy', ?, ?)",
            (f"alert_geo_{uuid.uuid4().hex[:10]}", eid, self.device_id, _now(), _now()),
        )
        conn.commit()
        conn.close()
        init_geofence_database()  # re-run migration
        conn = sqlite3.connect(str(gm.LOCAL_DB))
        stored = conn.execute(
            "SELECT event_type FROM geofence_alerts WHERE event_id = ?", (eid,)
        ).fetchone()[0]
        conn.close()
        self.assertEqual(stored, "GEOFENCE_ENTRY")


if __name__ == "__main__":
    unittest.main()
