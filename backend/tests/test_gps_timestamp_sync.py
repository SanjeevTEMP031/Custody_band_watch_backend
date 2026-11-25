import asyncio
import importlib
import sqlite3
import sys
import tempfile
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.gps import gps_routes
from app.receiver.packet_parser import parse_ud
from app.services.device_service import DeviceService
from app.services.firestore_service import (
    FirestoreService,
    firestore_service,
    normalize_iso_timestamp
)
from app.services.gps_service import gps_service

firestore_module = importlib.import_module("app.services.firestore_service")


def iso_at(offset: timedelta = timedelta()) -> str:
    return (datetime.now(timezone.utc) + offset).isoformat()


class GPSPositionRulesTests(unittest.TestCase):
    def setUp(self):
        self.device_id = "GPS_TIMESTAMP_TEST"
        self.device_state = {}
        self.device_get_patcher = patch.object(
            firestore_service,
            "get_local_device",
            side_effect=lambda device_id: (
                dict(self.device_state)
                if self.device_state.get("device_id") == device_id else None
            )
        )
        self.device_save_patcher = patch.object(
            firestore_service,
            "save_device",
            side_effect=lambda data, **_kwargs: self.device_state.update(data)
        )
        self.device_get_patcher.start()
        self.device_save_patcher.start()
        self.service = DeviceService()

    def tearDown(self):
        self.device_get_patcher.stop()
        self.device_save_patcher.stop()

    def test_valid_fix_then_no_fix_preserves_location_and_updates_battery(self):
        valid_time = iso_at(timedelta(seconds=-30))
        self.service.update_position(
            self.device_id, lat=17.44, lon=78.36, battery=78,
            gps_valid=True, timestamp=valid_time, gps_source="watch_tcp"
        )
        self.assertEqual(self.device_state["last_latitude"], 17.44)
        self.assertEqual(self.device_state["last_longitude"], 78.36)
        self.assertTrue(self.device_state["gps_valid"])
        self.assertEqual(self.device_state["gps_status"], "GPS Locked")

        self.service.update_position(
            self.device_id, lat=None, lon=None, battery=41, steps=120,
            is_charging=True, gps_valid=True, timestamp=iso_at()
        )
        self.assertEqual(self.device_state["last_latitude"], 17.44)
        self.assertEqual(self.device_state["last_longitude"], 78.36)
        self.assertFalse(self.device_state["gps_valid"])
        self.assertEqual(self.device_state["gps_status"], "No Fix")
        self.assertEqual(self.device_state["battery"], 41)
        self.assertEqual(self.device_state["steps"], 120)
        self.assertTrue(self.device_state["is_charging"])
        self.assertEqual(self.device_state["status"], "ONLINE")

    def test_invalid_coordinates_and_delayed_fix_do_not_replace_newer_fix(self):
        newer_time = iso_at(timedelta(seconds=-10))
        self.service.update_position(
            self.device_id, lat=18.0, lon=79.0, gps_valid=True, timestamp=newer_time,
            gps_source="watch_tcp"
        )
        self.service.update_position(
            self.device_id, lat=25.0, lon=79.0, gps_valid=True,
            timestamp=iso_at(timedelta(seconds=-20)), gps_source="watch_tcp"
        )
        self.assertEqual(self.device_state["last_latitude"], 18.0)
        self.assertEqual(self.device_state["last_longitude"], 79.0)
        self.assertEqual(self.device_state["last_valid_gps_time"], newer_time.replace("+00:00", "+00:00"))

        self.service.update_position(
            self.device_id, lat=91.0, lon=79.0, gps_valid=True,
            timestamp=iso_at(), gps_source="watch_tcp"
        )
        self.assertEqual(self.device_state["last_latitude"], 18.0)
        self.assertEqual(self.device_state["last_longitude"], 79.0)

        self.service.update_position(
            self.device_id, lat=0.0, lon=0.0, gps_valid=True,
            timestamp=iso_at(), gps_source="watch_tcp"
        )
        self.assertEqual(self.device_state["last_latitude"], 18.0)
        self.assertEqual(self.device_state["last_longitude"], 79.0)

    def test_location_becomes_stale_after_fifteen_minutes(self):
        old_time = iso_at(timedelta(minutes=-16))
        self.device_state.update({
            "device_id": self.device_id,
            "last_latitude": 17.44,
            "last_longitude": 78.36,
            "last_valid_gps_time": old_time,
            "gps_valid": True,
            "gps_source": "watch_tcp",
            "gps_status": "GPS Locked"
        })
        self.service.update_position(
            self.device_id, lat=None, lon=None, gps_valid=True, timestamp=iso_at()
        )
        self.assertEqual(self.device_state["last_latitude"], 17.44)
        self.assertEqual(self.device_state["last_longitude"], 78.36)
        self.assertFalse(self.device_state["gps_valid"])
        self.assertEqual(self.device_state["gps_status"], "Location Stale")

    def test_battery_only_update_preserves_location_and_zero_is_not_fabricated(self):
        self.device_state.update({
            "device_id": self.device_id,
            "last_latitude": 17.44,
            "last_longitude": 78.36,
            "last_valid_gps_time": iso_at(timedelta(seconds=-30)),
            "gps_valid": True,
            "gps_status": "GPS Locked"
        })
        self.service.update_position(
            self.device_id, battery=0, steps=800, is_charging=False, gps_valid=False
        )
        self.assertEqual(self.device_state["last_latitude"], 17.44)
        self.assertEqual(self.device_state["last_longitude"], 78.36)
        self.assertEqual(self.device_state["battery"], 0)
        self.assertEqual(self.device_state["steps"], 800)
        self.assertFalse(self.device_state["is_charging"])

    def test_position_requires_trusted_source_and_watch_timestamp(self):
        self.service.update_position(
            self.device_id,
            lat=17.44,
            lon=78.36,
            gps_valid=True,
            timestamp=iso_at(),
            gps_source=None
        )
        self.assertFalse(self.device_state["gps_valid"])
        self.assertNotIn("last_latitude", self.device_state)
        self.assertNotIn("last_valid_gps_time", self.device_state)

        self.service.update_position(
            self.device_id,
            lat=17.44,
            lon=78.36,
            gps_valid=True,
            timestamp=None,
            gps_source="watch_tcp"
        )
        self.assertFalse(self.device_state["gps_valid"])
        self.assertNotIn("last_latitude", self.device_state)
        self.assertNotIn("last_valid_gps_time", self.device_state)


class SQLiteGPSFallbackTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "gps-test.db"
        self.db_patcher = patch.object(firestore_module, "LOCAL_DB", self.db_path)
        self.firebase_patcher = patch.object(firestore_module, "is_firebase_active", return_value=False)
        self.db_patcher.start()
        self.firebase_patcher.start()
        self.service = FirestoreService()

    def tearDown(self):
        self.firebase_patcher.stop()
        self.db_patcher.stop()
        try:
            self.temp_dir.cleanup()
        except (PermissionError, OSError):
            pass

    def test_timestamp_ordered_latest_fix_and_complete_history(self):
        older_time = iso_at(timedelta(minutes=-2))
        newer_time = iso_at(timedelta(minutes=-1))
        self.service.save_telemetry({
            "device_id": "SQLITE_GPS_TEST",
            "timestamp": newer_time,
            "gps_valid": True,
            "gps_source": "watch_tcp",
            "latitude": 18.0,
            "longitude": 79.0,
            "battery": 60
        })
        self.service.save_telemetry({
            "device_id": "SQLITE_GPS_TEST",
            "timestamp": older_time,
            "gps_valid": True,
            "gps_source": "watch_tcp",
            "latitude": 17.0,
            "longitude": 78.0
        })

        latest = self.service.get_latest_valid_telemetry("SQLITE_GPS_TEST")
        history = self.service.get_telemetry_history("SQLITE_GPS_TEST", limit=10)
        self.assertEqual((latest["latitude"], latest["longitude"]), (18.0, 79.0))
        self.assertEqual(len(history), 2)
        self.assertEqual(history[0]["battery"], 60)

    def test_sqlite_device_position_rejects_late_old_fix(self):
        self.service.save_device({
            "device_id": "SQLITE_DEVICE_TEST",
            "last_seen": iso_at(),
            "last_latitude": 18.0,
            "last_longitude": 79.0,
            "last_valid_gps_time": iso_at(timedelta(minutes=-1)),
            "gps_valid": True,
            "gps_source": "watch_tcp",
            "gps_status": "GPS Locked"
        })
        self.service.save_device({
            "device_id": "SQLITE_DEVICE_TEST",
            "last_seen": iso_at(),
            "last_latitude": 17.0,
            "last_longitude": 78.0,
            "last_valid_gps_time": iso_at(timedelta(minutes=-2)),
            "gps_valid": True,
            "gps_source": "watch_tcp",
            "gps_status": "GPS Locked"
        })
        device = self.service.get_device("SQLITE_DEVICE_TEST")
        self.assertEqual(device["last_latitude"], 18.0)
        self.assertEqual(device["last_longitude"], 79.0)

    def test_history_merges_and_orders_firestore_records_with_local_records(self):
        local_time = iso_at(timedelta(minutes=-2))
        remote_time = iso_at(timedelta(minutes=-1))
        self.service.save_telemetry({
            "device_id": "MERGED_HISTORY_TEST",
            "timestamp": local_time,
            "gps_valid": True,
            "gps_source": "watch_tcp",
            "latitude": 17.0,
            "longitude": 78.0
        })
        remote_record = {
            "device_id": "MERGED_HISTORY_TEST",
            "timestamp": remote_time,
            "gps_valid": True,
            "latitude": 18.0,
            "longitude": 79.0,
            "gps_source": "watch_tcp"
        }

        remote_synced = False

        def sync_remote_history(_device_id, _limit):
            nonlocal remote_synced
            if not remote_synced:
                self.service.save_telemetry(remote_record)
                remote_synced = True

        with patch.object(
            self.service, "_sync_firestore_telemetry_history", side_effect=sync_remote_history
        ):
            history = self.service.get_telemetry_history("MERGED_HISTORY_TEST", limit=10)
            latest = self.service.get_latest_valid_telemetry("MERGED_HISTORY_TEST")

        self.assertEqual(len(history), 2)
        self.assertEqual(history[0]["timestamp"], remote_time)
        self.assertEqual(history[0]["gps_source"], "watch_tcp")
        self.assertEqual((latest["latitude"], latest["longitude"]), (18.0, 79.0))

    def test_firestore_snapshot_fix_is_not_backfilled_into_history(self):
        fix_time = iso_at(timedelta(seconds=-10))
        latest_fix = {
            "lat": 18.0,
            "lon": 79.0,
            "gps_time": fix_time,
            "gps_source": "watch_tcp",
            "gps_valid": True
        }
        db = MagicMock()
        device_ref = db.collection.return_value.document.return_value
        device_ref.collection.return_value.order_by.return_value.limit.return_value.stream.return_value = []
        device_ref.get.return_value = FakeSnapshot(
            {"latest": {"location": latest_fix}}, "FIRESTORE_HISTORY_TEST"
        )

        with (
            patch.object(firestore_module, "is_firebase_active", return_value=True),
            patch.object(firestore_module, "get_firestore_client", return_value=db)
        ):
            history = self.service.get_telemetry_history(
                "FIRESTORE_VERIFIED_HISTORY_TEST", limit=10
            )
            self.assertEqual(history, [])

            device_ref.get.return_value = FakeSnapshot(
                {"last_fix": {"at": fix_time, "lat": 18.0, "lon": 79.0}},
                "FIRESTORE_UNVERIFIED_HISTORY_TEST"
            )
            history = self.service.get_telemetry_history(
                "FIRESTORE_UNVERIFIED_HISTORY_TEST", limit=10
            )

        self.assertEqual(history, [])

    def test_history_sync_merges_both_firestore_locations_without_duplicates(self):
        shared_time = iso_at(timedelta(seconds=-20))
        global_time = iso_at(timedelta(seconds=-10))
        device_history = MagicMock()
        device_history_query = MagicMock()
        device_history.order_by.return_value = device_history_query
        device_history_query.limit.return_value = device_history_query
        device_history_query.stream.return_value = [
            FakeSnapshot({
                "timestamp": shared_time,
                "gps_valid": True,
                "gps_source": "watch_tcp",
                "latitude": 18.0,
                "longitude": 79.0
            }, "device-history-copy")
        ]
        device_collection = MagicMock()
        device_collection.document.return_value.collection.return_value = device_history
        global_telemetry = MagicMock()
        telemetry_query = MagicMock()
        global_telemetry.where.return_value = telemetry_query
        telemetry_query.where.return_value = telemetry_query
        telemetry_query.order_by.return_value = telemetry_query
        telemetry_query.limit.return_value = telemetry_query
        telemetry_query.stream.return_value = [
            FakeSnapshot({
                "device_id": "FIRESTORE_MERGED_HISTORY_TEST",
                "timestamp": shared_time,
                "gps_valid": True,
                "gps_source": "watch_frame",
                "latitude": 18.0,
                "longitude": 79.0
            }, "global-duplicate"),
            FakeSnapshot({
                "device_id": "FIRESTORE_MERGED_HISTORY_TEST",
                "timestamp": global_time,
                "gps_valid": True,
                "gps_source": "watch_frame",
                "latitude": 18.1,
                "longitude": 79.1
            }, "global-only-fix")
        ]
        empty_logs = MagicMock()
        logs_query = MagicMock()
        empty_logs.where.return_value = logs_query
        logs_query.where.return_value = logs_query
        logs_query.order_by.return_value = logs_query
        logs_query.limit.return_value = logs_query
        logs_query.stream.return_value = []
        db = MagicMock()
        db.collection.side_effect = lambda name: (
            device_collection if name == "devices"
            else empty_logs if name == "logs"
            else global_telemetry
        )

        with (
            patch.object(firestore_module, "is_firebase_active", return_value=True),
            patch.object(firestore_module, "get_firestore_client", return_value=db)
        ):
            history = self.service.get_telemetry_history(
                "FIRESTORE_MERGED_HISTORY_TEST", limit=10
            )

        self.assertEqual(len(history), 2)
        self.assertEqual(
            {record["timestamp"] for record in history},
            {shared_time, global_time}
        )
        self.assertEqual(
            (history[0]["latitude"], history[0]["longitude"]),
            (18.1, 79.1)
        )

    def test_history_sync_does_not_let_invalid_recent_records_hide_old_valid_fixes(self):
        device_id = "FIRESTORE_OLD_VALID_FIX_TEST"
        remote_records = [
            FakeSnapshot({
                "device_id": device_id,
                "timestamp": iso_at(timedelta(seconds=-(index + 1))),
                "gps_valid": False,
                "gps_source": "watch_tcp",
                "latitude": 18.0,
                "longitude": 79.0
            }, f"invalid-{index}")
            for index in range(500)
        ]
        old_valid_time = iso_at(timedelta(seconds=-501))
        remote_records.append(FakeSnapshot({
            "device_id": device_id,
            "timestamp": old_valid_time,
            "gps_valid": True,
            "gps_source": "watch_tcp",
            "latitude": 18.1,
            "longitude": 79.1
        }, "old-valid-fix"))
        device_history = MagicMock()
        device_history_query = MagicMock()
        device_history.order_by.return_value = device_history_query
        device_history_query.limit.return_value = device_history_query
        device_history_query.stream.return_value = []
        device_collection = MagicMock()
        device_collection.document.return_value.collection.return_value = device_history
        global_telemetry = MagicMock()
        telemetry_query = MagicMock()
        global_telemetry.where.return_value = telemetry_query
        telemetry_query.where.return_value = telemetry_query
        telemetry_query.order_by.return_value = telemetry_query
        telemetry_query.limit.return_value = telemetry_query
        telemetry_query.stream.return_value = remote_records
        empty_logs = MagicMock()
        logs_query = MagicMock()
        empty_logs.where.return_value = logs_query
        logs_query.where.return_value = logs_query
        logs_query.order_by.return_value = logs_query
        logs_query.limit.return_value = logs_query
        logs_query.stream.return_value = []
        db = MagicMock()
        db.collection.side_effect = lambda name: (
            device_collection if name == "devices"
            else empty_logs if name == "logs"
            else global_telemetry
        )

        with (
            patch.object(firestore_module, "is_firebase_active", return_value=True),
            patch.object(firestore_module, "get_firestore_client", return_value=db)
        ):
            history = self.service.get_telemetry_history(device_id, limit=1)

        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["timestamp"], old_valid_time)
        self.assertEqual((history[0]["latitude"], history[0]["longitude"]), (18.1, 79.1))

    def test_latest_location_prefers_timestamp_newer_than_device_snapshot(self):
        self.service.save_telemetry({
            "device_id": "SQLITE_LOCATION_TEST",
            "timestamp": iso_at(),
            "gps_valid": True,
            "gps_source": "watch_tcp",
            "latitude": 19.0,
            "longitude": 80.0
        })
        location = self.service.get_latest_valid_location(
            "SQLITE_LOCATION_TEST",
            {
                "last_latitude": 17.0,
                "last_longitude": 78.0,
                "last_valid_gps_time": iso_at(timedelta(minutes=-1)),
                "gps_valid": True,
                "gps_source": "watch_tcp"
            }
        )
        self.assertEqual((location["latitude"], location["longitude"]), (19.0, 80.0))
        self.assertTrue(location["is_newer"])

    def test_latest_location_uses_only_source_tagged_valid_firestore_last_fix(self):
        fix_time = iso_at(timedelta(seconds=-20))
        unverified = {
            "last_fix": {
                "at": fix_time,
                "lat": 18.0,
                "lon": 79.0
            }
        }
        verified = {
            "last_fix": {
                "at": fix_time,
                "lat": 18.0,
                "lon": 79.0,
                "gps_source": "watch_tcp",
                "gps_valid": True
            }
        }

        with patch.object(self.service, "get_latest_valid_telemetry", return_value=None):
            self.assertIsNone(
                self.service.get_latest_valid_location("LAST_FIX_TEST", unverified)
            )
            location = self.service.get_latest_valid_location("LAST_FIX_TEST", verified)

        self.assertEqual((location["latitude"], location["longitude"]), (18.0, 79.0))
        self.assertEqual(location["gps_source"], "watch_tcp")
        self.assertTrue(location["gps_valid"])

    def test_last_known_location_returns_unverified_fix_separately(self):
        fix_time = iso_at(timedelta(minutes=-2))
        last_known = self.service.get_last_known_location({
            "gps_status": "No Fix",
            "gps_valid": False,
            "last_fix": {
                "at": fix_time,
                "lat": 17.446886,
                "lon": 78.369267
            }
        })

        self.assertEqual(
            (last_known["latitude"], last_known["longitude"]),
            (17.446886, 78.369267)
        )
        self.assertEqual(last_known["timestamp"], fix_time)
        self.assertEqual(last_known["label"], "Last known")
        self.assertFalse(last_known["is_current"])
        self.assertFalse(last_known["is_stale"])
        self.assertFalse(last_known["verified"])

    def test_latest_location_accepts_matching_valid_raw_watch_frame(self):
        device_id = "RAW_WATCH_FIX_TEST"
        fix_time = datetime.now(timezone.utc).replace(microsecond=0)
        latitude = 17.446946
        longitude = 78.374241
        payload = (
            f"UD,{fix_time.strftime('%y%m%d')},{fix_time.strftime('%H%M%S')},A,"
            f"{latitude:.6f},N,{longitude:.6f},E,9.64,0.0,0.0,0,100,52,2277,"
            "0,00000000,1,255,404,49,19362,231609603,-46,0"
        )
        raw_frame = (
            f"[MNYD*{device_id}*{len(payload.encode('latin-1')):04X}*{payload}]"
        )
        device = {
            "device_id": device_id,
            "gps_valid": False,
            "gps_fix": True,
            "gps_locked": False,
            "gps_status": "No Fix",
            "latest": {
                "location": {
                    "command": "UD",
                    "gps_fix": True,
                    "gps_time": fix_time,
                    "lat": latitude,
                    "lon": longitude,
                    "raw": raw_frame
                }
            }
        }

        with patch.object(self.service, "get_latest_valid_telemetry", return_value=None):
            location = self.service.get_latest_valid_location(device_id, device)
            last_known = self.service.get_last_known_location(device)

        self.assertEqual((location["latitude"], location["longitude"]), (latitude, longitude))
        self.assertEqual(location["gps_source"], "watch_frame")
        self.assertTrue(location["gps_valid"])
        self.assertEqual((last_known["latitude"], last_known["longitude"]), (latitude, longitude))
        self.assertEqual(last_known["gps_source"], "watch_frame")
        self.assertTrue(last_known["verified"])

    def test_invalid_raw_watch_frame_is_not_promoted_to_a_fix(self):
        device_id = "RAW_WATCH_NO_FIX_TEST"
        fix_time = datetime.now(timezone.utc).replace(microsecond=0)
        payload = (
            f"UD,{fix_time.strftime('%y%m%d')},{fix_time.strftime('%H%M%S')},V,"
            "0.000000,N,0.000000,E,0.00,0.0,0.0,0,100,52,2277,0,00000000"
        )
        raw_frame = (
            f"[MNYD*{device_id}*{len(payload.encode('latin-1')):04X}*{payload}]"
        )
        device = {
            "device_id": device_id,
            "latest": {
                "location": {
                    "command": "UD",
                    "gps_fix": True,
                    "gps_time": fix_time,
                    "lat": 17.446946,
                    "lon": 78.374241,
                    "raw": raw_frame
                }
            }
        }

        with patch.object(self.service, "get_latest_valid_telemetry", return_value=None):
            location = self.service.get_latest_valid_location(device_id, device)

        self.assertIsNone(location)

    def test_raw_watch_frame_must_match_device_position_timestamp_and_length(self):
        device_id = "RAW_WATCH_MATCH_TEST"
        fix_time = datetime.now(timezone.utc).replace(microsecond=0)
        latitude = 17.446946
        longitude = 78.374241
        payload = (
            f"UD,{fix_time.strftime('%y%m%d')},{fix_time.strftime('%H%M%S')},A,"
            f"{latitude:.6f},N,{longitude:.6f},E,9.64,0.0,0.0,0,100,52,2277,"
            "0,00000000,1,255,404,49,19362,231609603,-46,0"
        )
        raw_frame = (
            f"[MNYD*{device_id}*{len(payload.encode('latin-1')):04X}*{payload}]"
        )
        valid_fix = {
            "gps_fix": True,
            "gps_time": fix_time,
            "lat": latitude,
            "lon": longitude,
            "raw": raw_frame
        }
        invalid_fixes = {
            "different device id": (
                device_id,
                {**valid_fix, "raw": raw_frame.replace(device_id, "OTHER_DEVICE")}
            ),
            "different coordinates": (
                device_id,
                {**valid_fix, "lat": latitude + 0.001}
            ),
            "different timestamp": (
                device_id,
                {**valid_fix, "gps_time": fix_time + timedelta(seconds=2)}
            ),
            "incorrect frame length": (
                device_id,
                {**valid_fix, "raw": raw_frame.replace(f"*{len(payload):04X}*", "*0000*")}
            )
        }

        for reason, (candidate_device_id, candidate) in invalid_fixes.items():
            with self.subTest(reason=reason):
                self.assertFalse(
                    FirestoreService._matches_raw_watch_gps_fix(candidate_device_id, candidate)
                )


class FirestoreDeviceReadCacheTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_patcher = patch.object(
            firestore_module, "LOCAL_DB", Path(self.temp_dir.name) / "device-cache.db"
        )
        self.db_patcher.start()
        self.service = FirestoreService()
        self.firebase_patcher = patch.object(
            firestore_module, "is_firebase_active", return_value=True
        )
        self.firebase_patcher.start()

    def tearDown(self):
        self.firebase_patcher.stop()
        self.db_patcher.stop()
        try:
            self.temp_dir.cleanup()
        except (PermissionError, OSError):
            pass

    def test_repeated_device_reads_use_cached_firestore_document(self):
        snapshot = MagicMock()
        snapshot.exists = True
        snapshot.to_dict.return_value = {"last_latitude": 17.4, "last_longitude": 78.3}
        document = MagicMock()
        document.get.return_value = snapshot
        db = MagicMock()
        db.collection.return_value.document.return_value = document

        with patch.object(firestore_module, "get_firestore_client", return_value=db):
            first = self.service.get_device("CACHED_DEVICE")
            second = self.service.get_device("CACHED_DEVICE")

        self.assertEqual(first["last_latitude"], 17.4)
        self.assertEqual(second["last_longitude"], 78.3)
        document.get.assert_called_once_with(timeout=2.0)


class FakeSnapshot:
    def __init__(self, data, document_id):
        self.exists = data is not None
        self.id = document_id
        self._data = data or {}

    def to_dict(self):
        return dict(self._data)


class FakeTransaction:
    _max_attempts = 1
    _read_only = False

    def __init__(self):
        self._id = "test-transaction"
        self._writes = []

    def _clean_up(self):
        self._writes = []

    def _begin(self, retry_id=None):
        self._writes = []
        if retry_id is not None:
            self._id = retry_id

    def set(self, document, data, merge=False):
        self._writes.append((document, data, merge))

    def _commit(self):
        for document, data, merge in self._writes:
            document.set(data, merge=merge)

    def _rollback(self):
        self._writes = []


class FakeDocument:
    def __init__(self, database, path):
        self.database = database
        self.path = path
        self.id = path[-1]

    def get(self, **_kwargs):
        _ = _kwargs
        return FakeSnapshot(self.database.documents.get(self.path), self.id)

    def set(self, data, merge=False):
        if merge:
            current = self.database.documents.setdefault(self.path, {})
            current.update(data)
        else:
            self.database.documents[self.path] = dict(data)

    def collection(self, name):
        return FakeCollection(self.database, self.path + (name,))


class FakeCollection:
    def __init__(self, database, path):
        self.database = database
        self.path = path

    def document(self, document_id):
        return FakeDocument(self.database, self.path + (document_id,))

    def add(self, data):
        self.database.history.append((self.path, dict(data)))


class FakeFirestore:
    def __init__(self):
        self.documents = {}
        self.history = []

    def collection(self, name):
        return FakeCollection(self, (name,))

    def transaction(self):
        return FakeTransaction()


class QueuedThread:
    def __init__(self, target, **_kwargs):
        self.target = target
        self.options = _kwargs

    def start(self):
        pass


class FirestoreOrderingTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "firestore-test.db"
        self.db_patcher = patch.object(firestore_module, "LOCAL_DB", self.db_path)
        self.db_patcher.start()
        self.service = FirestoreService()
        self.firestore = FakeFirestore()
        self.queued_threads = []
        self.active_patcher = patch.object(firestore_module, "is_firebase_active", return_value=True)
        self.client_patcher = patch.object(
            firestore_module, "get_firestore_client", return_value=self.firestore
        )
        self.thread_patcher = patch.object(
            firestore_module.threading,
            "Thread",
            side_effect=lambda target, **options: self._queue_thread(target, options)
        )
        self.active_patcher.start()
        self.client_patcher.start()
        self.thread_patcher.start()

    def tearDown(self):
        self.thread_patcher.stop()
        self.client_patcher.stop()
        self.active_patcher.stop()
        self.db_patcher.stop()
        self.temp_dir.cleanup()

    def _queue_thread(self, target, options):
        thread = QueuedThread(target, **options)
        self.queued_threads.append(thread)
        return thread

    def test_late_save_telemetry_cannot_overwrite_newer_save_device(self):
        newer_time = iso_at(timedelta(minutes=-1))
        older_time = iso_at(timedelta(minutes=-2))
        self.service.save_device({
            "device_id": "FIRESTORE_ORDER_TEST",
            "last_seen": iso_at(),
            "last_latitude": 18.0,
            "last_longitude": 79.0,
            "last_valid_gps_time": newer_time,
            "gps_valid": True,
            "gps_status": "GPS Locked",
            "gps_source": "watch_tcp"
        })
        self.service.save_telemetry({
            "device_id": "FIRESTORE_ORDER_TEST",
            "timestamp": older_time,
            "gps_valid": True,
            "latitude": 17.0,
            "longitude": 78.0,
            "gps_source": "watch_tcp"
        })

        self.queued_threads[0].target()
        self.service.sync_pending_firestore_telemetry(force=True)
        document = self.firestore.documents[("devices", "FIRESTORE_ORDER_TEST")]
        self.assertEqual(document["last_latitude"], 18.0)
        self.assertEqual(document["last_longitude"], 79.0)
        self.assertEqual(document["last_valid_gps_time"], newer_time)
        self.assertEqual(document["last_fix"]["gps_source"], "watch_tcp")
        self.assertEqual(document["last_fix"]["gps_source"], "watch_tcp")
        self.assertEqual(document["latest"]["location"]["gps_source"], "watch_tcp")
        history_record = next(
            data for path, data in self.firestore.documents.items()
            if path[:3] == ("devices", "FIRESTORE_ORDER_TEST", "history")
        )
        self.assertEqual(history_record["gps_source"], "watch_tcp")
        self.assertNotIn("battery", document)

    def test_late_save_device_cannot_overwrite_newer_save_telemetry(self):
        newer_time = iso_at(timedelta(minutes=-1))
        older_time = iso_at(timedelta(minutes=-2))
        self.service.save_telemetry({
            "device_id": "FIRESTORE_REVERSE_TEST",
            "timestamp": newer_time,
            "gps_valid": True,
            "latitude": 18.0,
            "longitude": 79.0,
            "gps_source": "watch_tcp"
        })
        self.service.save_device({
            "device_id": "FIRESTORE_REVERSE_TEST",
            "last_seen": iso_at(),
            "last_latitude": 17.0,
            "last_longitude": 78.0,
            "last_valid_gps_time": older_time,
            "gps_valid": True,
            "gps_source": "watch_tcp",
            "gps_status": "GPS Locked"
        })

        self.service.sync_pending_firestore_telemetry(force=True)
        self.queued_threads[0].target()
        document = self.firestore.documents[("devices", "FIRESTORE_REVERSE_TEST")]
        self.assertEqual(document["last_latitude"], 18.0)
        self.assertEqual(document["last_longitude"], 79.0)
        self.assertEqual(document["last_valid_gps_time"], newer_time)

    def test_firestore_telemetry_outbox_retries_after_write_failure(self):
        fix_time = iso_at(timedelta(seconds=-10))
        record = {
            "device_id": "FIRESTORE_OUTBOX_TEST",
            "timestamp": fix_time,
            "received_at": iso_at(),
            "gps_valid": True,
            "latitude": 17.0,
            "longitude": 78.0,
            "gps_source": "watch_tcp",
        }
        self.assertTrue(self.service.save_telemetry(record))
        with patch.object(
            self.service,
            "_sync_firestore_telemetry_payload",
            side_effect=RuntimeError("temporary Firestore failure")
        ):
            self.assertEqual(
                self.service.sync_pending_firestore_telemetry(force=True),
                0
            )

        conn = sqlite3.connect(str(self.db_path))
        try:
            queued = conn.execute(
                """
                SELECT document_id, attempts, last_error
                FROM firestore_telemetry_outbox
                WHERE device_id = ?
                """,
                ("FIRESTORE_OUTBOX_TEST",)
            ).fetchone()
        finally:
            conn.close()
        self.assertIsNotNone(queued)
        self.assertEqual(queued[1], 1)
        self.assertEqual(queued[2], "RuntimeError")

        self.assertEqual(
            self.service.sync_pending_firestore_telemetry(
                document_id=queued[0],
                force=True
            ),
            1
        )
        conn = sqlite3.connect(str(self.db_path))
        try:
            pending = conn.execute(
                "SELECT COUNT(*) FROM firestore_telemetry_outbox"
            ).fetchone()[0]
            local_history = conn.execute(
                "SELECT COUNT(*) FROM telemetry WHERE device_id = ? AND gps_valid = 1",
                ("FIRESTORE_OUTBOX_TEST",)
            ).fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(pending, 0)
        self.assertEqual(local_history, 1)
        self.assertTrue(any(
            path[:3] == ("devices", "FIRESTORE_OUTBOX_TEST", "history")
            for path in self.firestore.documents
        ))

    def test_device_packet_state_is_durably_queued_without_per_packet_thread(self):
        self.service.save_device({
            "device_id": "FIRESTORE_DEVICE_OUTBOX_TEST",
            "last_seen": iso_at(),
            "status": "ONLINE",
            "battery": 64,
            "gps_valid": False,
            "gps_status": "No Fix",
        }, sync_firestore=False)

        conn = sqlite3.connect(str(self.db_path))
        try:
            pending = conn.execute(
                "SELECT COUNT(*) FROM firestore_device_outbox"
            ).fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(pending, 1)
        self.assertEqual(self.queued_threads, [])

        self.assertEqual(
            self.service.sync_pending_firestore_device_updates(),
            1
        )
        device = self.firestore.documents[
            ("devices", "FIRESTORE_DEVICE_OUTBOX_TEST")
        ]
        self.assertEqual(device["battery"], 64)
        self.assertFalse(device["gps_valid"])
        self.assertIsNotNone(
            self.service._get_local_device("FIRESTORE_DEVICE_OUTBOX_TEST")
        )

    def test_sqlite_telemetry_write_failure_is_reported(self):
        fix_time = iso_at(timedelta(seconds=-10))
        with (
            patch.object(
                firestore_module.sqlite3,
                "connect",
                side_effect=sqlite3.OperationalError("database unavailable")
            ),
            self.assertLogs("custody_app", level="ERROR") as captured
        ):
            saved = self.service.save_telemetry({
                "device_id": "FIRESTORE_SQLITE_FAILURE_TEST",
                "timestamp": fix_time,
                "gps_valid": True,
                "latitude": 17.0,
                "longitude": 78.0,
                "gps_source": "watch_tcp",
            })

        self.assertFalse(saved)
        self.assertIn(
            "SQLite GPS telemetry persistence failed",
            "\n".join(captured.output)
        )

    def test_sqlite_device_write_failure_does_not_queue_stale_state(self):
        with (
            patch.object(
                firestore_module.sqlite3,
                "connect",
                side_effect=sqlite3.OperationalError("database unavailable")
            ),
            self.assertLogs("custody_app", level="ERROR")
        ):
            saved = self.service.save_device({
                "device_id": "FIRESTORE_DEVICE_WRITE_FAILURE_TEST",
                "last_seen": iso_at(),
                "status": "ONLINE",
                "battery": 64,
            }, sync_firestore=False)

        self.assertFalse(saved)


class GPSRouteLocationTests(unittest.TestCase):
    def test_history_keeps_old_verified_fixes_but_marks_them_stale(self):
        fix_time = iso_at(timedelta(minutes=-20))
        record = {
            "device_id": "STALE_HISTORY_ROUTE_TEST",
            "timestamp": fix_time,
            "received_at": iso_at(timedelta(minutes=-19)),
            "gps_valid": True,
            "latitude": 17.44,
            "longitude": 78.36,
            "gps_source": "watch_tcp",
        }
        with (
            patch.object(
                gps_routes.device_service,
                "get_device_by_id",
                return_value={"device_id": "STALE_HISTORY_ROUTE_TEST"}
            ),
            patch.object(
                gps_routes.firestore_service,
                "get_telemetry_history",
                return_value=[dict(record)]
            )
        ):
            history = gps_routes.get_location_history(
                "STALE_HISTORY_ROUTE_TEST",
                limit=50
            )

        self.assertEqual(len(history), 1)
        self.assertTrue(history[0]["gps_valid"])
        self.assertFalse(history[0]["gps_current"])
        self.assertEqual(history[0]["gps_status"], "Location Stale")
        self.assertEqual(history[0]["received_at"], record["received_at"])

    def test_latest_route_uses_valid_raw_watch_location_without_source_field(self):
        device_id = "RAW_WATCH_ROUTE_TEST"
        fix_time = datetime.now(timezone.utc).replace(microsecond=0)
        latitude = 17.446946
        longitude = 78.374241
        payload = (
            f"UD,{fix_time.strftime('%y%m%d')},{fix_time.strftime('%H%M%S')},A,"
            f"{latitude:.6f},N,{longitude:.6f},E,9.64,0.0,0.0,0,100,52,2277,"
            "0,00000000,1,255,404,49,19362,231609603,-46,0"
        )
        raw_frame = (
            f"[MNYD*{device_id}*{len(payload.encode('latin-1')):04X}*{payload}]"
        )
        device = {
            "device_id": device_id,
            "gps_status": "No Fix",
            "gps_valid": False,
            "gps_fix": True,
            "gps_locked": False,
            "latest": {
                "location": {
                    "command": "UD",
                    "gps_fix": True,
                    "gps_time": fix_time,
                    "lat": latitude,
                    "lon": longitude,
                    "raw": raw_frame
                }
            }
        }
        with (
            patch.object(gps_routes.device_service, "get_device_by_id", return_value=device),
            patch.object(gps_routes.firestore_service, "get_latest_valid_telemetry", return_value=None),
            patch.object(gps_routes.firestore_service, "get_telemetry_history", return_value=[])
        ):
            latest = gps_routes.get_latest_location(device_id)

        self.assertEqual((latest["latitude"], latest["longitude"]), (latitude, longitude))
        self.assertTrue(latest["gps_valid"])
        self.assertEqual(latest["gps_source"], "watch_frame")

    def test_latest_route_returns_last_known_coordinates_without_marking_them_current(self):
        fix_time = iso_at(timedelta(minutes=-2))
        device = {
            "device_id": "LAST_KNOWN_ROUTE_TEST",
            "gps_status": "No Fix",
            "gps_valid": False,
            "speed": 0.0,
            "heading": 0.0,
            "last_fix": {
                "at": fix_time,
                "lat": 17.446886,
                "lon": 78.369267
            }
        }
        last_known = {
            "latitude": 17.446886,
            "longitude": 78.369267,
            "timestamp": fix_time,
            "age_seconds": 120,
            "accuracy_m": 8.5,
            "speed_kmh": 6.6,
            "heading": 326.2,
            "bearing": 326.2,
            "label": "Last known",
            "is_current": False,
            "is_stale": False,
            "verified": False
        }
        with (
            patch.object(gps_routes.device_service, "get_device_by_id", return_value=device),
            patch.object(gps_routes.firestore_service, "get_latest_valid_location", return_value=None),
            patch.object(gps_routes.firestore_service, "get_last_known_location", return_value=last_known),
            patch.object(gps_routes.firestore_service, "get_telemetry_history", return_value=[])
        ):
            latest = gps_routes.get_latest_location("LAST_KNOWN_ROUTE_TEST")

        self.assertEqual(latest["gps_status"], "No Fix")
        self.assertFalse(latest["gps_valid"])
        self.assertEqual((latest["latitude"], latest["longitude"]), (17.446886, 78.369267))
        self.assertEqual((latest["lat"], latest["lon"]), (17.446886, 78.369267))
        self.assertEqual(latest["coordinate_source"], "last_known")
        self.assertEqual(latest["speed_kmh"], 6.6)
        self.assertEqual(latest["heading"], 326.2)
        self.assertEqual(latest["last_known_location"]["age_seconds"], 120)
        self.assertEqual(latest["last_known_location"]["accuracy_m"], 8.5)
        self.assertFalse(latest["location_is_current"])
        self.assertEqual(latest["last_known_location"], last_known)

    def test_last_known_uses_fix_time_and_motion_from_matching_telemetry(self):
        fix_time = iso_at(timedelta(hours=-2))
        telemetry = {
            "device_id": "LAST_KNOWN_FIX_METADATA_TEST",
            "timestamp": fix_time,
            "gps_valid": True,
            "gps_source": "watch_tcp",
            "latitude": 17.446886,
            "longitude": 78.369267,
            "speed_kmh": 6.6,
            "heading": 326.2,
            "accuracy_m": 8.5,
        }
        device = {
            "device_id": telemetry["device_id"],
            "timestamp": iso_at(),
            "last_valid_gps_time": fix_time,
            "last_latitude": telemetry["latitude"],
            "last_longitude": telemetry["longitude"],
            "gps_valid": False,
            "gps_source": "watch_tcp",
            "speed": 0.0,
            "heading": 0.0,
        }
        with patch.object(
            firestore_service, "get_latest_valid_telemetry", return_value=telemetry
        ):
            location = firestore_service.get_last_known_location(device)

        self.assertEqual(location["timestamp"], normalize_iso_timestamp(fix_time))
        self.assertGreaterEqual(location["age_seconds"], 7200)
        self.assertEqual(location["accuracy_m"], 8.5)
        self.assertEqual(location["speed_kmh"], 6.6)
        self.assertEqual(location["heading"], 326.2)
        self.assertEqual(location["coordinate_source"], "last_known")

    def test_live_route_includes_last_known_location_without_promoting_it_to_live(self):
        device_id = "LAST_KNOWN_LIVE_ROUTE_TEST"
        device = {
            "device_id": device_id,
            "gps_status": "No Fix",
            "gps_valid": False
        }
        last_known = {
            "latitude": 17.446886,
            "longitude": 78.369267,
            "timestamp": iso_at(timedelta(minutes=-2)),
            "gps_source": "watch_tcp",
            "label": "Last known",
            "is_current": False,
            "is_stale": False,
            "verified": True
        }
        with (
            patch.object(gps_routes.device_service, "get_all_devices", return_value=[device]),
            patch.object(gps_routes.firestore_service, "get_latest_valid_location", return_value=None),
            patch.object(
                gps_routes.firestore_service,
                "get_last_known_location",
                return_value=last_known
            )
        ):
            live = gps_routes.get_live_locations()

        self.assertEqual(len(live), 1)
        self.assertFalse(live[0]["gps_valid"])
        self.assertFalse(live[0]["location_is_current"])
        self.assertIsNone(live[0]["latitude"])
        self.assertIsNone(live[0]["longitude"])
        self.assertEqual(live[0]["last_known_location"], last_known)

    def test_latest_and_live_routes_use_the_newest_valid_position(self):
        previous_time = iso_at(timedelta(minutes=-1))
        device = {
            "device_id": "ROUTE_LOCATION_TEST",
            "last_latitude": 17.0,
            "last_longitude": 78.0,
            "last_valid_gps_time": previous_time,
            "gps_valid": True,
            "gps_fix": True,
            "gps_locked": True,
            "gps_status": "GPS Locked"
        }
        location = {
            "latitude": 18.0,
            "longitude": 79.0,
            "timestamp": iso_at(),
            "gps_source": "watch_tcp",
            "maps_url": "https://maps.google.com/?q=18.000000,79.000000",
            "is_newer": True,
            "gps_status": "GPS Locked",
            "gps_valid": True
        }
        with (
            patch.object(gps_routes.device_service, "get_all_devices", return_value=[device]),
            patch.object(gps_routes.device_service, "get_device_by_id", return_value=device),
            patch.object(gps_routes.firestore_service, "get_latest_valid_location", return_value=location),
            patch.object(gps_routes.firestore_service, "get_telemetry_history", return_value=[])
        ):
            live = gps_routes.get_live_locations()[0]
            latest = gps_routes.get_latest_location("ROUTE_LOCATION_TEST")

        self.assertEqual((live["latitude"], live["longitude"]), (18.0, 79.0))
        self.assertEqual((latest["latitude"], latest["longitude"]), (18.0, 79.0))
        self.assertEqual(live["maps_url"], location["maps_url"])
        self.assertEqual(latest["gps_source"], "watch_tcp")
        self.assertEqual(live["gps_source"], "watch_tcp")
        self.assertTrue(latest["gps_valid"])
        self.assertNotIn("battery", latest)
        self.assertIn("steps", latest)
        self.assertIn("heart_rate", latest)

    def test_latest_and_live_routes_hide_stale_coordinates_but_keep_battery(self):
        stale_time = iso_at(timedelta(minutes=-20))
        device = {
            "device_id": "STALE_ROUTE_TEST",
            "wearer_name": "Test wearer",
            "battery": 90,
            "last_valid_gps_time": stale_time,
            "gps_valid": False,
            "gps_fix": False,
            "gps_locked": False,
            "gps_status": "Location Stale",
        }
        stale_location = {
            "latitude": 17.444978,
            "longitude": 78.365,
            "timestamp": stale_time,
            "gps_source": "watch_tcp",
            "maps_url": "https://maps.google.com/?q=17.444978,78.365000",
            "gps_status": "Location Stale",
            "gps_valid": False,
            "gps_fix": False,
            "gps_locked": False,
        }
        with (
            patch.object(gps_routes.device_service, "get_all_devices", return_value=[device]),
            patch.object(gps_routes.device_service, "get_device_by_id", return_value=device),
            patch.object(
                gps_routes.firestore_service,
                "get_latest_valid_location",
                return_value=stale_location,
            ),
            patch.object(gps_routes.firestore_service, "get_telemetry_history", return_value=[]),
        ):
            live = gps_routes.get_live_locations()[0]
            latest = gps_routes.get_latest_location("STALE_ROUTE_TEST")

        for response in (live, latest):
            self.assertIsNone(response["latitude"])
            self.assertIsNone(response["longitude"])
            self.assertIsNone(response["lat"])
            self.assertIsNone(response["lon"])
            self.assertIsNone(response["maps_url"])
            self.assertIsNone(response["gps_source"])
            self.assertFalse(response["gps_valid"])
            self.assertFalse(response["gps_fix"])
            self.assertFalse(response["gps_locked"])
            self.assertEqual(response["timestamp"], stale_time)
            self.assertEqual(response["battery"], 90)


class WatchGpsPersistenceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "watch-gps.db"
        self.db_patcher = patch.object(firestore_module, "LOCAL_DB", self.db_path)
        self.db_patcher.start()
        firestore_service._init_local_db()
        with firestore_service._device_cache_lock:
            firestore_service._device_cache.clear()
            firestore_service._devices_cache = None
        self.firebase_patcher = patch.object(
            firestore_module, "is_firebase_active", return_value=False
        )
        self.firebase_patcher.start()
        self.device_id = f"WATCH_FIX_{uuid.uuid4().hex[:8]}"

    def tearDown(self):
        self.firebase_patcher.stop()
        self.db_patcher.stop()
        try:
            self.temp_dir.cleanup()
        except (PermissionError, OSError):
            pass

    @staticmethod
    def _packet(timestamp: datetime, status: str, latitude: float, longitude: float, satellites: int = 0):
        fields = [
            "UD",
            timestamp.strftime("%y%m%d"),
            timestamp.strftime("%H%M%S"),
            status,
            f"{latitude:.6f}",
            "N",
            f"{longitude:.6f}",
            "E",
            "0.0",
            "90.0",
            "0.0",
            str(satellites),
            "0",
            "",
            "",
            "00000000",
        ]
        return ",".join(fields).encode("latin-1")

    @staticmethod
    def _gps_time(timestamp: datetime) -> str:
        return timestamp.replace(microsecond=0).isoformat()

    def test_valid_a_packet_persists_locally_and_syncs_from_durable_outbox(self):
        packet_time = datetime.now(timezone.utc).replace(microsecond=0)
        expected_time = self._gps_time(packet_time)
        fake_firestore = FakeFirestore()
        queued_threads = []

        def queue_thread(target, **options):
            queued = QueuedThread(target, **options)
            queued_threads.append(queued)
            return queued

        with (
            patch.object(firestore_module, "is_firebase_active", return_value=True),
            patch.object(firestore_module, "get_firestore_client", return_value=fake_firestore),
            patch.object(
                firestore_module.threading, "Thread", side_effect=queue_thread
            ),
            patch(
                "app.services.journey_service.journey_service.record_journey_point"
            ),
        ):
            record = parse_ud(
                self._packet(packet_time, "A", 17.445, 78.37, satellites=0),
                self.device_id,
                "MNYD",
                gps_source="watch_tcp",
            )
            duplicate = parse_ud(
                self._packet(packet_time, "A", 17.445, 78.37, satellites=0),
                self.device_id,
                "MNYD",
                gps_source="watch_tcp",
            )
            conn = sqlite3.connect(str(self.db_path))
            try:
                pending_count = conn.execute(
                    """
                    SELECT COUNT(*) FROM firestore_telemetry_outbox
                    WHERE device_id = ?
                    """,
                    (self.device_id,)
                ).fetchone()[0]
            finally:
                conn.close()
            self.assertEqual(pending_count, 1)
            self.assertEqual(
                firestore_service.sync_pending_firestore_telemetry(),
                1
            )

        self.assertIsNotNone(record)
        self.assertIsNotNone(duplicate)
        self.assertTrue(record["gps_valid"])
        self.assertEqual(record["gps_source"], "watch_tcp")
        self.assertEqual(record["timestamp"], expected_time)
        self.assertEqual(record["satellites"], 0)
        remote_history = [
            data for path, data in fake_firestore.documents.items()
            if path[:3] == ("devices", self.device_id, "history")
        ]
        self.assertEqual(len(remote_history), 1)
        history_record = remote_history[0]
        self.assertEqual(history_record["timestamp"], expected_time)
        self.assertTrue(history_record["gps_valid"])
        self.assertEqual(history_record["gps_source"], "watch_tcp")
        remote_device = fake_firestore.documents[("devices", self.device_id)]
        self.assertEqual(remote_device["last_valid_gps_time"], expected_time)
        self.assertEqual(remote_device["gps_source"], "watch_tcp")
        self.assertEqual(len(queued_threads), 0)

        with patch.object(firestore_module, "is_firebase_active", return_value=False):
            latest = gps_routes.get_latest_location(self.device_id)
            history = gps_routes.get_location_history(self.device_id, limit=500)

        self.assertEqual(latest["timestamp"], expected_time)
        self.assertEqual(latest["last_valid_gps_time"], expected_time)
        self.assertTrue(latest["gps_valid"])
        self.assertEqual(latest["gps_source"], "watch_tcp")
        self.assertEqual(history[0]["timestamp"], expected_time)
        self.assertEqual(len(history), 1)
        self.assertTrue(history[0]["gps_valid"])
        self.assertEqual(history[0]["gps_source"], "watch_tcp")
        self.assertEqual((history[0]["latitude"], history[0]["longitude"]), (17.445, 78.37))
        self.assertEqual((latest["latitude"], latest["longitude"]), (17.445, 78.37))

    def test_invalid_fix_inputs_preserve_location_and_history(self):
        fix_time = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(seconds=2)
        expected_time = self._gps_time(fix_time)
        valid = parse_ud(
            self._packet(fix_time, "A", 17.446317, 78.368395),
            self.device_id,
            "MNYD",
            gps_source="watch_tcp",
        )
        self.assertTrue(valid["gps_valid"])

        invalid_packets = [
            (self._packet(fix_time, "A", 91.0, 78.0), "watch_tcp"),
            (self._packet(fix_time - timedelta(minutes=16), "A", 18.0, 79.0), "watch_tcp"),
            (self._packet(fix_time + timedelta(minutes=1), "A", 18.0, 79.0), "watch_tcp"),
            (self._packet(fix_time + timedelta(seconds=1), "A", 18.0, 79.0), None),
            (self._packet(fix_time + timedelta(seconds=1), "A", 18.0, 79.0), "untrusted_source"),
            (
                b"UD,not-a-date,not-a-time,A,18.000000,N,79.000000,E,"
                b"0.0,90.0,0.0,0,0,,,00000000",
                "watch_tcp",
            ),
        ]
        for packet, source in invalid_packets:
            with self.subTest(source=source, packet=packet[:20]):
                record = parse_ud(
                    packet, self.device_id, "MNYD", gps_source=source
                )
                self.assertIsNotNone(record)
                if packet == self._packet(fix_time - timedelta(minutes=16), "A", 18.0, 79.0):
                    self.assertTrue(record["gps_valid"])
                    self.assertFalse(record["gps_current"])
                    self.assertEqual(record["latitude"], 18.0)
                    self.assertEqual(record["longitude"], 79.0)
                else:
                    self.assertFalse(record["gps_valid"])
                    self.assertIsNone(record["latitude"])
                    self.assertIsNone(record["longitude"])

        device = firestore_service.get_device(self.device_id)
        history = firestore_service.get_telemetry_history(self.device_id, limit=10)
        self.assertEqual(device["last_latitude"], 17.446317)
        self.assertEqual(device["last_longitude"], 78.368395)
        self.assertEqual(device["last_valid_gps_time"], expected_time)
        self.assertEqual(device["gps_source"], "watch_tcp")
        self.assertEqual(len(history), 2)

    def test_latest_location_accepts_writer_snapshot_without_gps_source(self):
        fix_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        location = firestore_service.get_latest_valid_location(
            self.device_id,
            {
                "last_latitude": 17.44,
                "last_longitude": 78.36,
                "last_valid_gps_time": fix_time,
                "gps_valid": True,
            }
        )
        self.assertIsNotNone(location)
        self.assertEqual((location["latitude"], location["longitude"]), (17.44, 78.36))
        self.assertEqual(location["gps_source"], "watch_tcp")
        self.assertTrue(location["gps_valid"])

    def test_invalid_v_packet_preserves_previous_valid_location_and_gps_time(self):
        valid_time = datetime.now(timezone.utc).replace(microsecond=0)
        invalid_time = valid_time + timedelta(minutes=1)
        expected_time = self._gps_time(valid_time)
        valid = parse_ud(
            self._packet(valid_time, "A", 17.446317, 78.368395),
            self.device_id,
            "MNYD",
            gps_source="watch_tcp",
        )
        self.assertTrue(valid["gps_valid"])

        invalid = parse_ud(
            self._packet(invalid_time, "V", 0.0, 0.0),
            self.device_id,
            "MNYD",
            gps_source="watch_tcp",
        )

        self.assertIsNotNone(invalid)
        self.assertFalse(invalid["gps_valid"])
        self.assertIsNone(invalid["gps_source"])
        device = firestore_service.get_device(self.device_id)
        self.assertEqual(device["last_latitude"], 17.446317)
        self.assertEqual(device["last_longitude"], 78.368395)
        self.assertEqual(device["last_valid_gps_time"], expected_time)
        self.assertEqual(device["gps_source"], "watch_tcp")
        latest_fix = firestore_service.get_latest_valid_telemetry(self.device_id)
        self.assertEqual(latest_fix["timestamp"], expected_time)
        self.assertEqual(latest_fix["gps_source"], "watch_tcp")
        history = firestore_service.get_telemetry_history(self.device_id, limit=10)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["timestamp"], expected_time)
        latest = gps_routes.get_latest_location(self.device_id)
        self.assertEqual(
            (latest["latitude"], latest["longitude"]), (17.446317, 78.368395)
        )
        self.assertEqual(latest["last_valid_gps_time"], expected_time)
        self.assertEqual(latest["gps_source"], "watch_tcp")

    def test_latest_and_history_api_include_validity_timestamp_and_source(self):
        packet_time = datetime.now(timezone.utc).replace(microsecond=0)
        expected_time = self._gps_time(packet_time)
        firestore_service.save_device({
            "device_id": self.device_id,
            "last_seen": datetime.now(timezone.utc).isoformat(),
            "last_latitude": 17.44,
            "last_longitude": 78.36,
            "last_valid_gps_time": expected_time,
            "gps_valid": True,
            "gps_source": "watch_tcp",
            "gps_status": "GPS Locked",
        })
        firestore_service.save_telemetry({
            "device_id": self.device_id,
            "timestamp": expected_time,
            "gps_valid": True,
            "gps_source": "watch_tcp",
            "latitude": 17.44,
            "longitude": 78.36,
        })

        latest = gps_routes.get_latest_location(self.device_id)
        history = gps_routes.get_location_history(self.device_id, limit=500)

        self.assertTrue(latest["gps_valid"])
        self.assertTrue(latest["gps_fix"])
        self.assertEqual(latest["timestamp"], expected_time)
        self.assertEqual(latest["gps_source"], "watch_tcp")
        self.assertEqual((latest["latitude"], latest["longitude"]), (17.44, 78.36))
        self.assertEqual(len(history), 1)
        self.assertTrue(history[0]["gps_valid"])
        self.assertTrue(history[0]["gps_fix"])
        self.assertEqual(history[0]["timestamp"], expected_time)
        self.assertEqual(history[0]["gps_source"], "watch_tcp")
        self.assertEqual((history[0]["latitude"], history[0]["longitude"]), (17.44, 78.36))


class GPSIngestTimestampTests(unittest.TestCase):
    class Request:
        def __init__(self, payload):
            self.payload = payload

        async def json(self):
            return self.payload

    def test_ingest_requires_packet_gps_timestamp_and_sets_api_source(self):
        fix_time = iso_at(timedelta(seconds=-2))
        payload = {
            "device_id": "GPS_INGEST_TIMESTAMP_TEST",
            "latitude": 17.44,
            "longitude": 78.36,
            "gps_valid": True,
            "timestamp": fix_time
        }
        with (
            patch.object(
                gps_routes.gps_service,
                "process_location_record",
                side_effect=lambda record: record
            ) as process_record,
            patch.object(gps_routes.device_service, "update_position") as update_position
        ):
            result = asyncio.run(
                gps_routes.ingest_gps_telemetry(self.Request(payload))
            )

        record = result["data"]
        self.assertTrue(record["gps_valid"])
        self.assertEqual(record["gps_source"], "gps_ingest_api")
        self.assertEqual(record["timestamp"], normalize_iso_timestamp(fix_time))
        saved_record = process_record.call_args.args[0]
        self.assertEqual(saved_record["timestamp"], record["timestamp"])
        self.assertEqual(saved_record["gps_source"], "gps_ingest_api")
        self.assertEqual(update_position.call_args.kwargs["timestamp"], record["timestamp"])
        self.assertEqual(update_position.call_args.kwargs["gps_source"], "gps_ingest_api")

        missing_time_payload = {**payload, "timestamp": None}
        with (
            patch.object(
                gps_routes.gps_service,
                "process_location_record",
                side_effect=lambda record: record
            ),
            patch.object(gps_routes.device_service, "update_position") as update_position
        ):
            result = asyncio.run(
                gps_routes.ingest_gps_telemetry(
                    self.Request(missing_time_payload)
                )
            )

        self.assertFalse(result["data"]["gps_valid"])
        self.assertIsNone(result["data"]["timestamp"])
        self.assertIsNone(update_position.call_args.kwargs["timestamp"])
        self.assertFalse(update_position.call_args.kwargs["gps_valid"])


if __name__ == "__main__":
    unittest.main()
