"""
Reader support for writer-shaped Firestore documents (no gps_source).

Documents use only the Python writer's field contract: root lat/lon/gps_fix,
latest.*, and logs/{id} location messages — not devices/{id}/history telemetry.
"""

import importlib
import json
import sqlite3
import sys
import tempfile
import time
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.health_data.health_service import HealthService
from app.services.firestore_service import FirestoreService, firestore_service

firestore_module = importlib.import_module("app.services.firestore_service")


def iso_at(offset: timedelta = timedelta()) -> str:
    return (datetime.now(timezone.utc) + offset).isoformat()


class FakeSnapshot:
    def __init__(self, data, document_id):
        self.exists = data is not None
        self.id = document_id
        self._data = data or {}

    def to_dict(self):
        return dict(self._data)


class WriterShapedFirestoreReadTests(unittest.TestCase):
    def setUp(self):
        self.device_id = f"WRITER_{uuid.uuid4().hex[:8]}"
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "writer-shaped.db"
        self.db_patcher = patch.object(firestore_module, "LOCAL_DB", self.db_path)
        self.db_patcher.start()
        self.service = FirestoreService()
        with self.service._device_cache_lock:
            self.service._device_cache.clear()
            self.service._devices_cache = None

        self.fix_time = iso_at(timedelta(seconds=-30))
        self.received_at = iso_at(timedelta(seconds=-25))
        self.device_doc = {
            "last_seen": self.received_at,
            "battery_percent": 67,
            "steps": 2100,
            "latitude": 17.446317,
            "longitude": 78.368395,
            "lat": 17.446317,
            "lon": 78.368395,
            "gps_fix": True,
            "speed_kmh": 3.2,
            "location_time": self.fix_time,
            "last_fix": {
                "lat": 17.446317,
                "lon": 78.368395,
                "at": self.fix_time,
            },
            "latest": {
                "location": {
                    "gps_fix": True,
                    "lat": 17.446317,
                    "lon": 78.368395,
                    "gps_time": self.fix_time,
                    "received_at": self.received_at,
                    "battery_percent": 67,
                    "steps": 2100,
                    "speed_kmh": 3.2,
                    "direction_deg": 90.0,
                    "altitude_m": 510.0,
                    "satellites": 8,
                    "signal_percent": 95,
                    "rolls": 2,
                    "alarms": ["fall"],
                    "cell_mcc": "404",
                    "cell_mnc": "49",
                    "cells": [{"lac": 19392, "cid": 231626243, "rssi": -39}],
                    "wifi": [{"name": "ap", "mac": "00:11:22:33:44:55", "rssi": -54}],
                    "raw": "UD,non-frame-payload",
                },
                "location_on_demand": {
                    "gps_fix": True,
                    "lat": 17.446317,
                    "lon": 78.368395,
                    "gps_time": self.fix_time,
                    "received_at": self.received_at,
                },
                "status": {
                    "battery_percent": 67,
                    "steps": 2100,
                },
                "heart_rate": {
                    "heart_rate_bpm": 82,
                    "hrv": 36,
                },
                "temperature": {
                    "temperature_c": 36.6,
                },
                "blood_pressure": {
                    "systolic": 120,
                    "diastolic": 80,
                },
                "blood_oxygen": {
                    "spo2_percent": 97,
                },
                "sleep": {
                    "awake_min": 40,
                    "light_min": 180,
                    "deep_min": 90,
                    "date": "2026-11-14",
                },
                "blood_glucose": {
                    "glucose_mmol_l": 5.4,
                },
                "sleep_report": {
                    "duration_min": 496,
                    "levels": [0, 1, 2, 4, 7],
                },
                "sleep_allday": {
                    "start_raw": "unknown-format",
                    "levels": [1, 2, 3],
                },
            },
            "mileage_km": 12.5,
        }
        self.log_doc = {
            "device_id": self.device_id,
            "type": "location",
            "received_at": self.received_at,
            "raw": "UD,non-frame-payload",
            "gps_fix": True,
            "lat": 17.450001,
            "lon": 78.370001,
            "gps_time": self.fix_time,
            "battery_percent": 67,
            "steps": 2100,
            "speed_kmh": 3.2,
            "direction_deg": 90.0,
        }

    def tearDown(self):
        self.db_patcher.stop()
        self.temp_dir.cleanup()

    def _mock_firestore(self, device_doc, log_docs):
        device_snapshot = FakeSnapshot(device_doc, self.device_id)
        device_document = MagicMock()
        device_document.get.return_value = device_snapshot
        device_history = MagicMock()
        device_history_query = MagicMock()
        device_history.order_by.return_value = device_history_query
        device_history_query.limit.return_value = device_history_query
        device_history_query.stream.return_value = []
        device_document.collection.return_value = device_history
        device_collection = MagicMock()
        device_collection.document.return_value = device_document

        empty_telemetry = MagicMock()
        telemetry_query = MagicMock()
        empty_telemetry.where.return_value = telemetry_query
        telemetry_query.where.return_value = telemetry_query
        telemetry_query.order_by.return_value = telemetry_query
        telemetry_query.limit.return_value = telemetry_query
        telemetry_query.stream.return_value = []

        log_snapshots = [
            FakeSnapshot(doc, f"log-{index}")
            for index, doc in enumerate(log_docs)
        ]
        logs_collection = MagicMock()
        self.logs_query = MagicMock()
        self.logs_query.where.return_value = self.logs_query
        self.logs_query.order_by.return_value = self.logs_query
        self.logs_query.limit.return_value = self.logs_query
        self.logs_query.stream.return_value = log_snapshots
        logs_collection.where.return_value = self.logs_query

        db = MagicMock()

        def collection(name):
            if name == "devices":
                return device_collection
            if name == "logs":
                return logs_collection
            return empty_telemetry

        db.collection.side_effect = collection
        return db

    def test_writer_shaped_device_and_logs_return_fix_history_and_vitals(self):
        db = self._mock_firestore(self.device_doc, [self.log_doc])

        with (
            patch.object(firestore_module, "is_firebase_active", return_value=True),
            patch.object(firestore_module, "get_firestore_client", return_value=db),
        ):
            device = self.service.get_device(self.device_id)
            location = self.service.get_latest_valid_location(self.device_id, device)
            history = self.service.get_telemetry_history(self.device_id, limit=10)

        self.assertIsNotNone(device)
        self.assertEqual(device["last_latitude"], 17.446317)
        self.assertEqual(device["last_longitude"], 78.368395)
        self.assertEqual(device["battery"], 67)
        self.assertEqual(device["battery_percent"], 67)
        self.assertEqual(device["steps"], 2100)
        self.assertEqual(device["heart_rate"], 82)
        self.assertEqual(device["hrv"], 36)
        self.assertEqual(device["temperature"], 36.6)
        self.assertEqual(device["blood_glucose"], 5.4)
        self.assertEqual(device["glucose_mmol_l"], 5.4)
        self.assertEqual(device["blood_oxygen"], 97)
        self.assertEqual(device["systolic"], 120)
        self.assertEqual(device["diastolic"], 80)
        self.assertEqual(device["blood_pressure"], "120/80")
        self.assertEqual(device["sleep"]["deep_min"], 90)
        self.assertEqual(device["sleep_report"]["levels"], [0, 1, 2, 4, 7])
        self.assertEqual(device["sleep_allday"]["levels"], [1, 2, 3])
        self.assertEqual(device["mileage_km"], 12.5)
        self.assertEqual(device["altitude_m"], 510.0)
        self.assertEqual(device["signal_percent"], 95)
        self.assertEqual(device["gsm_signal"], 95)
        self.assertEqual(device["satellites"], 8)
        self.assertEqual(device["alarms"], ["fall"])
        self.assertEqual(device["cells"][0]["cid"], 231626243)
        self.assertEqual(device["wifi"][0]["rssi"], -54)
        self.assertTrue(device["gps_valid"])
        self.assertEqual(device["gps_source"], "watch_tcp")

        self.assertIsNotNone(location)
        self.assertEqual(
            (location["latitude"], location["longitude"]),
            (17.446317, 78.368395),
        )
        self.assertEqual(location["gps_source"], "watch_tcp")
        self.assertTrue(location["gps_valid"])
        self.assertEqual(location["timestamp"], self.fix_time)

        self.assertEqual(len(history), 1)
        self.assertEqual(
            (history[0]["latitude"], history[0]["longitude"]),
            (17.450001, 78.370001),
        )
        self.assertEqual(history[0]["gps_source"], "watch_tcp")
        self.assertEqual(history[0]["timestamp"], self.fix_time)

        health = HealthService()
        with patch(
            "app.health_data.health_service.device_service.get_device_by_id",
            return_value=device,
        ):
            summary = health.get_health_summary(self.device_id)

        self.assertEqual(summary["heart_rate"], 82)
        self.assertEqual(summary["battery"], 67)
        self.assertEqual(summary["steps"], 2100)
        self.assertEqual(summary["spo2"], 97)
        self.assertEqual(summary["blood_pressure"], "120/80")
        self.assertEqual(summary["blood_glucose"], 5.4)
        self.assertEqual(summary["glucose_mmol_l"], 5.4)
        self.assertEqual(summary["hrv"], 36)
        self.assertEqual(summary["sleep_report"]["levels"], [0, 1, 2, 4, 7])
        self.assertEqual(summary["sleep_allday"]["levels"], [1, 2, 3])
        self.assertNotEqual(summary["heart_rate"], 75)
        self.assertNotEqual(summary["steps"], 1420)
        self.assertNotEqual(summary["battery"], 94)

    def test_log_history_uses_received_at_cursor_for_incremental_sync(self):
        db = self._mock_firestore(self.device_doc, [self.log_doc])

        with (
            patch.object(firestore_module, "is_firebase_active", return_value=True),
            patch.object(firestore_module, "get_firestore_client", return_value=db),
        ):
            initial_history = self.service.get_telemetry_history(
                self.device_id,
                limit=10,
            )

        self.assertEqual(len(initial_history), 1)
        self.logs_query.order_by.assert_called_with(
            "received_at",
            direction=firestore_module.cloud_firestore.Query.DESCENDING,
        )
        self.logs_query.limit.assert_called_with(2000)

        conn = sqlite3.connect(str(self.db_path))
        try:
            watermark, watermark_kind = conn.execute(
                """
                SELECT watermark, watermark_kind
                FROM firestore_history_sync
                WHERE device_id = ?
                """,
                (f"{self.device_id}::logs",),
            ).fetchone()
            conn.execute(
                """
                UPDATE firestore_history_sync
                SET checked_at = 0
                WHERE device_id = ?
                """,
                (f"{self.device_id}::logs",),
            )
            conn.commit()
        finally:
            conn.close()

        self.assertEqual(watermark, self.received_at)
        self.assertEqual(watermark_kind, "logs_received_at_string")

        next_fix_time = iso_at(timedelta(seconds=-20))
        next_received_at = iso_at(timedelta(seconds=-15))
        next_log = {
            **self.log_doc,
            "received_at": next_received_at,
            "gps_time": next_fix_time,
            "lat": 17.451,
            "lon": 78.371,
        }
        self.logs_query.stream.return_value = [
            FakeSnapshot(next_log, "new-log")
        ]

        with (
            patch.object(firestore_module, "is_firebase_active", return_value=True),
            patch.object(firestore_module, "get_firestore_client", return_value=db),
        ):
            history = self.service.get_telemetry_history(self.device_id, limit=10)

        self.logs_query.where.assert_any_call(
            "received_at", ">", self.received_at
        )
        self.logs_query.limit.assert_called_with(500)
        self.assertEqual(len(history), 2)
        self.assertEqual(history[0]["timestamp"], next_fix_time)

    def test_history_sync_backs_off_after_firestore_read_failure(self):
        db = self._mock_firestore(self.device_doc, [])
        self.logs_query.stream.side_effect = RuntimeError("Firestore unavailable")

        with (
            patch.object(firestore_module, "is_firebase_active", return_value=True),
            patch.object(firestore_module, "get_firestore_client", return_value=db),
        ):
            self.service.get_telemetry_history(self.device_id, limit=10)
            self.service.get_telemetry_history(self.device_id, limit=10)

        self.assertEqual(self.logs_query.stream.call_count, 1)
        self.assertGreater(
            self.service._history_firestore_retry_at[f"{self.device_id}::logs"],
            time.monotonic(),
        )

    def test_device_read_prefers_canonical_battery_and_normalizes_legacy_aliases(self):
        mismatched_doc = {
            **self.device_doc,
            "battery": 90,
            "batteryLevel": 90,
            "battery_level": 90,
            "battery_pct": 90,
            "battery_percent": 62,
            "battery_percentage": 90,
            "charge": 90,
        }
        db = self._mock_firestore(mismatched_doc, [])

        with (
            patch.object(firestore_module, "is_firebase_active", return_value=True),
            patch.object(firestore_module, "get_firestore_client", return_value=db),
        ):
            device = self.service.get_device(self.device_id)

        self.assertIsNotNone(device)
        for field in (
            "battery",
            "batteryLevel",
            "battery_level",
            "battery_pct",
            "battery_percent",
            "battery_percentage",
            "charge",
        ):
            self.assertEqual(device[field], 62)

    def test_device_state_outbox_syncs_all_battery_aliases(self):
        self.service.save_device(
            {
                "device_id": self.device_id,
                "last_seen": self.received_at,
                "battery": 62,
            },
            sync_firestore=False,
        )

        conn = sqlite3.connect(str(self.db_path))
        try:
            row = conn.execute(
                "SELECT payload_json FROM firestore_device_outbox WHERE device_id = ?",
                (self.device_id,),
            ).fetchone()
        finally:
            conn.close()

        self.assertIsNotNone(row)
        update_data = json.loads(row[0])["update_data"]
        for field in (
            "battery",
            "batteryLevel",
            "battery_level",
            "battery_pct",
            "battery_percent",
            "battery_percentage",
            "charge",
        ):
            self.assertEqual(update_data[field], 62)

    def test_missing_vitals_stay_empty_and_gps_fix_false_is_rejected(self):
        empty_vitals = {
            "last_seen": self.received_at,
            "lat": 0.0,
            "lon": 0.0,
            "gps_fix": False,
            "location_time": self.fix_time,
            "latest": {
                "location": {
                    "gps_fix": False,
                    "lat": 17.1,
                    "lon": 78.1,
                    "gps_time": self.fix_time,
                    "received_at": self.received_at,
                }
            },
        }
        rejected_log = {
            "device_id": self.device_id,
            "type": "location",
            "received_at": self.received_at,
            "gps_fix": False,
            "lat": 17.2,
            "lon": 78.2,
            "gps_time": self.fix_time,
        }
        db = self._mock_firestore(empty_vitals, [rejected_log])

        with (
            patch.object(firestore_module, "is_firebase_active", return_value=True),
            patch.object(firestore_module, "get_firestore_client", return_value=db),
        ):
            device = self.service.get_device(self.device_id)
            location = self.service.get_latest_valid_location(self.device_id, device)
            history = self.service.get_telemetry_history(self.device_id, limit=10)

        self.assertIsNone(device.get("heart_rate"))
        self.assertIsNone(device.get("battery"))
        self.assertIsNone(device.get("blood_pressure"))
        self.assertIsNone(device.get("sleep"))
        self.assertFalse(device["gps_valid"])
        self.assertIsNone(location)
        self.assertEqual(history, [])


if __name__ == "__main__":
    unittest.main()
