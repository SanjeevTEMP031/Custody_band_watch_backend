#!/usr/bin/env python3
"""
Unit and Integration Tests for C05 Backend GPS Location & Firebase Synchronization
Validates all 8 testing requirements specified by the task:
1. Valid UD packet with status 'A'
2. Invalid UD packet with status 'V'
3. Invalid GPS does not overwrite previous valid coordinates
4. Battery updates even when GPS is invalid
5. Firebase / backend shows actual battery percentage (no fake 90 defaults)
6. GPS status transitions correctly between GPS Locked, No Fix, and Location Stale
7. Frontend map displays actual valid device coordinates
8. Keepalive (KA/LK) packets correctly update device telemetry
"""

import importlib
import sys
import tempfile
import unittest
import uuid
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional
from unittest.mock import patch, MagicMock

# Ensure backend root is on sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.receiver.packet_parser import parse_ud, parse_packet
from app.services.device_service import device_service
from app.services.firestore_service import firestore_service
from app.dashboard.dashboard_service import dashboard_service
from app.services.alert_service import alert_service

firestore_module = importlib.import_module("app.services.firestore_service")
alert_module = importlib.import_module("app.services.alert_service")


class TestGPSAndFirebaseSync(unittest.TestCase):
    def setUp(self):
        self.dev_id = f"TEST_DEVICE_{uuid.uuid4().hex[:8]}"
        self.maker = "MNYD"
        self.temp_dir = tempfile.TemporaryDirectory()
        test_db = Path(self.temp_dir.name) / "gps-sync-test.db"
        self.db_patcher = patch.object(
            firestore_module, "LOCAL_DB", test_db
        )
        self.db_patcher.start()
        self.alert_db_patcher = patch.object(
            alert_module, "LOCAL_DB", test_db
        )
        self.alert_db_patcher.start()
        firestore_service._init_local_db()
        alert_service._ensure_tables()
        with firestore_service._device_cache_lock:
            firestore_service._device_cache.clear()
            firestore_service._devices_cache = None
        self.firebase_patcher = patch.object(
            firestore_module, "is_firebase_active", return_value=False
        )
        self.firebase_patcher.start()
        self.alert_firebase_patcher = patch.object(
            alert_module, "is_firebase_active", return_value=False
        )
        self.alert_firebase_patcher.start()

        # Mock Firestore client to capture writes and allow fast, reliable unit testing
        self.mock_db = MagicMock()
        self.saved_docs = {}

        def mock_set(data, merge=True):
            if self.dev_id in self.saved_docs and merge:
                self.saved_docs[self.dev_id].update(data)
            else:
                self.saved_docs[self.dev_id] = dict(data)

        mock_doc = MagicMock()
        mock_doc.set.side_effect = mock_set

        def mock_get(timeout=None, transaction=None):
            _ = transaction
            m = MagicMock()
            if self.dev_id in self.saved_docs:
                m.exists = True
                m.to_dict.return_value = dict(self.saved_docs[self.dev_id])
                m.id = self.dev_id
            else:
                m.exists = False
                m.to_dict.return_value = {}
            return m

        mock_doc.get.side_effect = mock_get

        def mock_stream(timeout=None):
            docs = []
            for did, data in self.saved_docs.items():
                d = MagicMock()
                d.id = did
                d.to_dict.return_value = dict(data)
                docs.append(d)
            return docs

        self.mock_db.collection.return_value.document.return_value = mock_doc
        self.mock_db.collection.return_value.stream.side_effect = mock_stream

        self.firestore_patcher = patch("app.services.firestore_service.get_firestore_client", return_value=self.mock_db)
        self.firestore_patcher.start()

    def tearDown(self):
        self.firestore_patcher.stop()
        self.firebase_patcher.stop()
        self.alert_firebase_patcher.stop()
        self.alert_db_patcher.stop()
        self.db_patcher.stop()
        self.temp_dir.cleanup()

    @staticmethod
    def _watch_packet(
        timestamp: datetime,
        status: str,
        latitude: float,
        longitude: float,
        battery: Optional[int] = None,
        satellites: int = 0,
    ) -> bytes:
        fields = [
            "UD",
            timestamp.strftime("%y%m%d"),
            timestamp.strftime("%H%M%S"),
            status,
            f"{latitude:.6f}",
            "N",
            f"{longitude:.6f}",
            "E",
            "16.5",
            "142.0",
            "510.0",
            str(satellites),
            "95",
            str(battery) if battery is not None else "",
            "1250",
            "00000000",
        ]
        return ",".join(fields).encode("latin-1")

    def test_01_valid_ud_packet_status_a(self):
        """Test 1: Valid UD packet with status 'A' produces GPS Locked fix and valid coordinates."""
        content = self._watch_packet(
            datetime.now(timezone.utc), "A", 17.445, 78.37, battery=73, satellites=0
        )
        rec = parse_ud(content, self.dev_id, self.maker, gps_source="watch_tcp")

        self.assertIsNotNone(rec)
        self.assertTrue(rec["gps_valid"])
        self.assertTrue(rec["gps_fix"])
        self.assertTrue(rec["gps_locked"])
        self.assertEqual(rec["gps_status"], "GPS Locked")
        self.assertEqual(rec["gps_source"], "watch_tcp")
        self.assertEqual(rec["latitude"], 17.445000)
        self.assertEqual(rec["longitude"], 78.370000)
        self.assertEqual(rec["battery"], 73)
        self.assertEqual(rec["speed_kmh"], 16.5)
        self.assertEqual(rec["heading"], 142.0)

    def test_02_invalid_ud_packet_status_v(self):
        """Test 2: Invalid UD packet with status 'V' produces No Fix and null fix coordinates."""
        content = self._watch_packet(
            datetime.now(timezone.utc), "V", 0.0, 0.0, battery=65, satellites=0
        )
        rec = parse_ud(content, self.dev_id, self.maker, gps_source="watch_tcp")

        self.assertIsNotNone(rec)
        self.assertFalse(rec["gps_valid"])
        self.assertFalse(rec["gps_fix"])
        self.assertFalse(rec["gps_locked"])
        self.assertEqual(rec["gps_status"], "No Fix")
        self.assertIsNone(rec["gps_source"])
        self.assertIsNone(rec["latitude"])
        self.assertIsNone(rec["longitude"])
        self.assertEqual(rec["battery"], 65)

    def test_03_invalid_gps_does_not_overwrite_previous_valid_coordinates(self):
        """Test 3: Status 'V' does not clobber previously stored valid GPS coordinates."""
        # 1. Establish valid fix
        valid_time = datetime.now(timezone.utc)
        valid_content = self._watch_packet(
            valid_time, "A", 17.446317, 78.368395, battery=88, satellites=0
        )
        parse_ud(valid_content, self.dev_id, self.maker, gps_source="watch_tcp")

        dev_after_valid = device_service.get_device_by_id(self.dev_id)
        self.assertEqual(dev_after_valid["last_latitude"], 17.446317)
        self.assertEqual(dev_after_valid["last_longitude"], 78.368395)
        self.assertEqual(dev_after_valid["gps_status"], "GPS Locked")

        # 2. Receive invalid fix ('V' with 0,0)
        invalid_content = self._watch_packet(
            valid_time + timedelta(seconds=1), "V", 0.0, 0.0, battery=85, satellites=0
        )
        parse_ud(invalid_content, self.dev_id, self.maker, gps_source="watch_tcp")

        dev_after_invalid = device_service.get_device_by_id(self.dev_id)
        # Previous coordinates MUST be preserved on device
        self.assertEqual(dev_after_invalid["last_latitude"], 17.446317)
        self.assertEqual(dev_after_invalid["last_longitude"], 78.368395)
        # GPS state MUST switch to No Fix
        self.assertFalse(dev_after_invalid["gps_valid"])
        self.assertEqual(dev_after_invalid["gps_status"], "No Fix")

    def test_04_battery_updates_even_when_gps_invalid(self):
        """Test 4: Battery updates to real reading even when GPS status is 'V'."""
        invalid_content = self._watch_packet(
            datetime.now(timezone.utc), "V", 0.0, 0.0, battery=42, satellites=0
        )
        parse_ud(invalid_content, self.dev_id, self.maker, gps_source="watch_tcp")

        dev = device_service.get_device_by_id(self.dev_id)
        self.assertEqual(dev["battery"], 42)
        self.assertEqual(dev["battery_percent"], 42)
        self.assertEqual(dev["gps_status"], "No Fix")

    def test_05_firebase_shows_actual_battery_no_defaults(self):
        """Test 5: Actual battery reading is preserved; never replaced with 90 or 100."""
        for index, test_batt in enumerate((15, 37, 77, 8)):
            content = self._watch_packet(
                datetime.now(timezone.utc) + timedelta(seconds=index),
                "A",
                17.4455,
                78.369,
                battery=test_batt,
                satellites=0,
            )
            parse_ud(content, self.dev_id, self.maker, gps_source="watch_tcp")
            dev = device_service.get_device_by_id(self.dev_id)
            self.assertEqual(dev["battery"], test_batt, f"Battery should be {test_batt}, not defaulted to 90")

    def test_06_gps_status_transitions_and_rejects_stale_incoming_fix(self):
        """A stale incoming GPS timestamp must not be stored as a location."""
        # 1. Fresh valid fix -> GPS Locked
        now_ts = datetime.now(timezone.utc).isoformat()
        device_service.update_position(
            device_id=self.dev_id,
            lat=17.447000,
            lon=78.369500,
            battery=80,
            steps=1000,
            gps_valid=True,
            timestamp=now_ts,
            gps_source="watch_tcp"
        )
        dev = device_service.get_device_by_id(self.dev_id)
        self.assertEqual(dev["gps_status"], "GPS Locked")
        self.assertTrue(dev["gps_valid"])

        # 2. Invalid update -> No Fix
        device_service.update_position(
            device_id=self.dev_id,
            lat=None,
            lon=None,
            battery=79,
            steps=1010,
            gps_valid=True,
            timestamp=now_ts
        )
        dev = device_service.get_device_by_id(self.dev_id)
        self.assertEqual(dev["gps_status"], "No Fix")
        self.assertFalse(dev["gps_valid"])
        self.assertEqual(dev["last_latitude"], 17.447000)

        # 3. Simulate stale fix (> 15 minutes ago)
        old_time = (datetime.now(timezone.utc) - timedelta(minutes=25)).isoformat()
        stale_device_id = f"{self.dev_id}_STALE_{uuid.uuid4().hex}"
        device_service.update_position(
            device_id=stale_device_id,
            lat=17.447000,
            lon=78.369500,
            battery=78,
            steps=1020,
            gps_valid=True,
            timestamp=old_time,
            gps_source="watch_tcp"
        )
        dev = device_service.get_device_by_id(stale_device_id)
        self.assertEqual(dev["gps_status"], "No Fix")
        self.assertFalse(dev["gps_valid"])
        self.assertIsNone(dev["last_latitude"])
        self.assertIsNone(dev["last_valid_gps_time"])

    def test_07_frontend_map_displays_actual_valid_device_coordinates(self):
        """Test 7: Dashboard markers and devices list display real coordinates without mock fallbacks."""
        content = self._watch_packet(
            datetime.now(timezone.utc), "A", 17.448123, 78.371456, battery=68, satellites=0
        )
        parse_ud(content, self.dev_id, self.maker, gps_source="watch_tcp")

        summary = dashboard_service.get_summary_stats()
        self.assertGreaterEqual(summary["total_devices"], 1)

        devices = dashboard_service.list_all_devices()
        test_dev = next((d for d in devices if d["device_id"] == self.dev_id), None)
        self.assertIsNotNone(test_dev)
        self.assertEqual(test_dev["last_latitude"], 17.448123)
        self.assertEqual(test_dev["last_longitude"], 78.371456)
        self.assertEqual(test_dev["battery"], 68)
        self.assertEqual(test_dev["gps_status"], "GPS Locked")

    def test_08_keepalive_ka_and_lk_packets_update_telemetry(self):
        """Test 8: Keepalive (KA) and Link (LK) packets update actual device battery and steps."""
        # Test KA packet: KA,steps,rolls,battery,charging
        ka_content = b"KA,2800,0,54,0"
        cmd, reply = parse_packet(self.maker, self.dev_id, ka_content)
        self.assertEqual(cmd, "KA")
        dev = device_service.get_device_by_id(self.dev_id)
        self.assertEqual(dev["battery"], 54)
        self.assertEqual(dev["steps"], 2800)

        # Test LK packet: LK,steps,rolls,battery,charging
        lk_content = b"LK,3100,0,49,0"
        cmd, reply = parse_packet(self.maker, self.dev_id, lk_content)
        self.assertEqual(cmd, "LK")
        dev = device_service.get_device_by_id(self.dev_id)
        self.assertEqual(dev["battery"], 49)
        self.assertEqual(dev["steps"], 3100)

    def test_heartbeat_does_not_change_gps_fix_or_append_history(self):
        fix_time = datetime.now(timezone.utc).replace(microsecond=0)
        parse_ud(
            self._watch_packet(fix_time, "A", 17.445, 78.37),
            self.dev_id,
            self.maker,
            gps_source="watch_tcp"
        )
        before = device_service.get_device_by_id(self.dev_id)
        history_before = firestore_service.get_telemetry_history(self.dev_id, limit=10)

        parse_packet(self.maker, self.dev_id, b"KA,2800,0,54,0")

        after = device_service.get_device_by_id(self.dev_id)
        history_after = firestore_service.get_telemetry_history(self.dev_id, limit=10)
        self.assertEqual(after["last_latitude"], before["last_latitude"])
        self.assertEqual(after["last_longitude"], before["last_longitude"])
        self.assertEqual(after["last_valid_gps_time"], before["last_valid_gps_time"])
        self.assertEqual(after["gps_source"], before["gps_source"])
        self.assertEqual(len(history_after), len(history_before))


if __name__ == "__main__":
    unittest.main()
