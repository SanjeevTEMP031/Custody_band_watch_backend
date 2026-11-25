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

from app.receiver.packet_parser import parse_ud
from app.services.device_service import DeviceService
from app.services.firestore_service import (
    FirestoreService,
    firestore_service,
    normalize_iso_timestamp,
)
from app.services.gps_service import GPSService


def iso_at(offset: timedelta = timedelta()) -> str:
    return (datetime.now(timezone.utc) + offset).replace(microsecond=0).isoformat()


def make_ud_packet(
    date_str: str,
    time_str: str,
    status: str,
    lat: float,
    lon: float,
    battery: int = 88,
    steps: int = 100,
    course: float = 90.0,
) -> bytes:
    ns = "N" if lat >= 0 else "S"
    ew = "E" if lon >= 0 else "W"
    return (
        f"UD,{date_str},{time_str},{status},{abs(lat):.6f},{ns},"
        f"{abs(lon):.6f},{ew},0.0,{course:.1f},0.0,0,0,{battery},{steps},0,00000000"
    ).encode("ascii")


class GPSPersistenceRegressionTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "test_custody.db"
        self.db_patch = patch("app.services.firestore_service.LOCAL_DB", self.db_path)
        self.fb_patch = patch("app.services.firestore_service.is_firebase_active", return_value=False)
        self.db_patch.start()
        self.fb_patch.start()

        self.device_service = DeviceService()
        self.gps_service = GPSService()
        self.firestore_service = FirestoreService()
        self.device_id = "REGRESSION_DEV_01"

    def tearDown(self):
        self.fb_patch.stop()
        self.db_patch.stop()
        try:
            self.temp_dir.cleanup()
        except (PermissionError, OSError):
            pass

    def _get_device_row(self, device_id: str):
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            return conn.execute("SELECT * FROM devices WHERE device_id = ?", (device_id,)).fetchone()
        finally:
            conn.close()

    def _get_telemetry_rows(self, device_id: str):
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            return conn.execute(
                "SELECT * FROM telemetry WHERE device_id = ? ORDER BY timestamp DESC", (device_id,)
            ).fetchall()
        finally:
            conn.close()

    def test_01_valid_gps_packet_persists_to_both_tables(self):
        """Valid fresh GPS updates devices table and inserts into telemetry."""
        now = datetime.now(timezone.utc).replace(microsecond=0)
        date_str = now.strftime("%y%m%d")
        time_str = now.strftime("%H%M%S")
        pkt = make_ud_packet(date_str, time_str, "A", 17.441954, 78.384253)

        record = parse_ud(pkt, self.device_id, "MNYD", gps_source="watch_tcp")
        self.assertIsNotNone(record)
        self.assertTrue(record["gps_valid"])
        self.assertTrue(record["gps_current"])
        self.assertEqual(record["latitude"], 17.441954)
        self.assertEqual(record["longitude"], 78.384253)

        # Check devices table
        dev = self._get_device_row(self.device_id)
        self.assertIsNotNone(dev)
        self.assertEqual(dev["last_latitude"], 17.441954)
        self.assertEqual(dev["last_longitude"], 78.384253)
        self.assertEqual(dev["last_valid_gps_time"], record["timestamp"])
        self.assertEqual(dev["gps_valid"], 1)
        self.assertEqual(dev["gps_status"], "GPS Locked")

        # Check telemetry table
        telemetry = self._get_telemetry_rows(self.device_id)
        self.assertEqual(len(telemetry), 1)
        self.assertEqual(telemetry[0]["latitude"], 17.441954)
        self.assertEqual(telemetry[0]["longitude"], 78.384253)

    def test_02_invalid_packet_never_overwrites_valid_coordinates(self):
        """Status 'V' with 0,0 coordinates preserves previous valid coordinates in devices table."""
        # 1. Establish valid fix
        t1 = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(minutes=5)
        pkt1 = make_ud_packet(t1.strftime("%y%m%d"), t1.strftime("%H%M%S"), "A", 17.441954, 78.384253)
        parse_ud(pkt1, self.device_id, "MNYD", gps_source="watch_tcp")

        dev_before = self._get_device_row(self.device_id)
        valid_gps_time = dev_before["last_valid_gps_time"]

        # 2. Receive invalid LBS packet (V, 0.0, 0.0)
        t2 = datetime.now(timezone.utc).replace(microsecond=0)
        pkt2 = make_ud_packet(t2.strftime("%y%m%d"), t2.strftime("%H%M%S"), "V", 0.0, 0.0, battery=85)
        record2 = parse_ud(pkt2, self.device_id, "MNYD", gps_source="watch_tcp")

        self.assertFalse(record2["gps_valid"])
        self.assertIsNone(record2["latitude"])
        self.assertIsNone(record2["longitude"])

        # 3. Verify devices table preserved coordinates and last_valid_gps_time
        dev_after = self._get_device_row(self.device_id)
        self.assertEqual(dev_after["last_latitude"], 17.441954)
        self.assertEqual(dev_after["last_longitude"], 78.384253)
        self.assertEqual(dev_after["last_valid_gps_time"], valid_gps_time)
        self.assertEqual(dev_after["gps_valid"], 0)
        self.assertEqual(dev_after["battery"], 85)

        # 4. Telemetry count remains 1 (invalid fixes not appended to telemetry)
        telemetry = self._get_telemetry_rows(self.device_id)
        self.assertEqual(len(telemetry), 1)

    def test_03_stale_gps_packet_advances_location_but_marks_stale(self):
        """A valid fix older than 15 mins advances last_valid_gps_time if newer, but is not current."""
        # 1. Older fix 2 hours ago
        t1 = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(hours=2)
        pkt1 = make_ud_packet(t1.strftime("%y%m%d"), t1.strftime("%H%M%S"), "A", 17.400000, 78.300000)
        parse_ud(pkt1, self.device_id, "MNYD", gps_source="watch_tcp")

        # 2. Stale fix 30 mins ago (newer than t1, but > 15 min old)
        t2 = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(minutes=30)
        pkt2 = make_ud_packet(t2.strftime("%y%m%d"), t2.strftime("%H%M%S"), "A", 17.442000, 78.385000)
        record2 = parse_ud(pkt2, self.device_id, "MNYD", gps_source="watch_tcp")

        self.assertTrue(record2["gps_valid"])
        self.assertFalse(record2["gps_current"])
        self.assertEqual(record2["gps_status"], "Location Stale")

        dev = self._get_device_row(self.device_id)
        self.assertEqual(dev["last_latitude"], 17.442000)
        self.assertEqual(dev["last_longitude"], 78.385000)
        self.assertEqual(dev["last_valid_gps_time"], record2["timestamp"])
        self.assertEqual(dev["gps_valid"], 0)  # Not current lock
        self.assertEqual(dev["gps_status"], "Location Stale")

        # Both records in telemetry history
        telemetry = self._get_telemetry_rows(self.device_id)
        self.assertEqual(len(telemetry), 2)

    def test_04_duplicate_gps_packet_is_idempotent(self):
        """Sending the exact same packet twice does not duplicate telemetry or alter state."""
        now = datetime.now(timezone.utc).replace(microsecond=0)
        pkt = make_ud_packet(now.strftime("%y%m%d"), now.strftime("%H%M%S"), "A", 17.441954, 78.384253)

        parse_ud(pkt, self.device_id, "MNYD", gps_source="watch_tcp")
        parse_ud(pkt, self.device_id, "MNYD", gps_source="watch_tcp")

        telemetry = self._get_telemetry_rows(self.device_id)
        self.assertEqual(len(telemetry), 1)

    def test_05_out_of_order_packet_does_not_overwrite_newer_device_position(self):
        """An older GPS reading arriving after a newer reading does not overwrite devices table."""
        t_newer = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(minutes=2)
        t_older = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(minutes=10)

        # Receive newer fix first
        pkt_newer = make_ud_packet(t_newer.strftime("%y%m%d"), t_newer.strftime("%H%M%S"), "A", 17.500000, 78.500000)
        parse_ud(pkt_newer, self.device_id, "MNYD", gps_source="watch_tcp")

        # Receive older delayed fix second
        pkt_older = make_ud_packet(t_older.strftime("%y%m%d"), t_older.strftime("%H%M%S"), "A", 17.100000, 78.100000)
        parse_ud(pkt_older, self.device_id, "MNYD", gps_source="watch_tcp")

        dev = self._get_device_row(self.device_id)
        # Position must remain the NEWER fix
        self.assertEqual(dev["last_latitude"], 17.500000)
        self.assertEqual(dev["last_longitude"], 78.500000)
        self.assertEqual(dev["last_valid_gps_time"], normalize_iso_timestamp(t_newer.isoformat()))

        # But telemetry table contains both points in timestamp order
        telemetry = self._get_telemetry_rows(self.device_id)
        self.assertEqual(len(telemetry), 2)
        self.assertEqual(telemetry[0]["latitude"], 17.500000)  # Newer
        self.assertEqual(telemetry[1]["latitude"], 17.100000)  # Older

    def test_06_update_position_returns_boolean_success(self):
        """update_position() returns True on successful SQLite write, not None or fake True."""
        result = self.device_service.update_position(
            device_id=self.device_id,
            lat=17.441954,
            lon=78.384253,
            gps_valid=True,
            gps_source="watch_tcp",
            timestamp=iso_at(),
        )
        self.assertIs(result, True)


if __name__ == "__main__":
    unittest.main()
