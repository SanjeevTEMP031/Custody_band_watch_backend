import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from app.custody.custody_routes import _format_custody_record
from app.gps.gps_routes import get_live_locations


class CustodyLocationTests(unittest.TestCase):
    def test_returns_device_coordinates_without_replacing_them(self):
        record = _format_custody_record({
            "device_id": "watch-1",
            "last_latitude": 17.123456,
            "last_longitude": 78.654321,
            "gps_valid": True,
            "gps_status": "GPS Locked",
            "last_valid_gps_time": "2026-11-10T10:00:00+00:00",
        })

        self.assertEqual(record["latitude"], 17.123456)
        self.assertEqual(record["longitude"], 78.654321)
        self.assertEqual(record["maps_url"], "https://maps.google.com/?q=17.123456,78.654321")
        self.assertTrue(record["gps_valid"])
        self.assertEqual(record["gps_status"], "GPS Locked")
        self.assertEqual(record["last_valid_gps_time"], "2026-11-10T10:00:00+00:00")

    def test_does_not_invent_coordinates_when_device_has_no_fix(self):
        record = _format_custody_record({"device_id": "watch-2"})

        self.assertIsNone(record["latitude"])
        self.assertIsNone(record["longitude"])
        self.assertIsNone(record["maps_url"])
        self.assertFalse(record["gps_valid"])
        self.assertEqual(record["gps_status"], "No Fix")

    def test_preserves_valid_zero_latitude_and_longitude(self):
        record = _format_custody_record({
            "device_id": "watch-3",
            "last_latitude": 0.0,
            "last_longitude": 12.5,
        })

        self.assertEqual(record["latitude"], 0.0)
        self.assertEqual(record["longitude"], 12.5)

    def test_rejects_invalid_coordinates(self):
        record = _format_custody_record({
            "device_id": "watch-4",
            "last_latitude": 91.0,
            "last_longitude": 78.0,
        })

        self.assertIsNone(record["latitude"])
        self.assertIsNone(record["longitude"])
        self.assertIsNone(record["maps_url"])

    @patch("app.gps.gps_routes.device_service.get_all_devices")
    def test_live_endpoint_returns_latest_device_coordinates(self, get_devices):
        device = {
            "device_id": "watch-5",
            "last_latitude": 17.1,
            "last_longitude": 78.1,
            "gps_valid": True,
            "gps_status": "GPS Locked",
            "last_valid_gps_time": datetime.now(timezone.utc).isoformat(),
            "gps_source": "watch_tcp",
        }
        get_devices.return_value = [device]

        first_location = get_live_locations()[0]
        device["last_latitude"] = 17.2
        device["last_longitude"] = 78.2
        second_location = get_live_locations()[0]

        self.assertEqual((first_location["latitude"], first_location["longitude"]), (17.1, 78.1))
        self.assertEqual((second_location["latitude"], second_location["longitude"]), (17.2, 78.2))

    @patch("app.gps.gps_routes.device_service.get_all_devices")
    def test_live_endpoint_preserves_zero_latitude(self, get_devices):
        get_devices.return_value = [{
            "device_id": "watch-6",
            "last_latitude": 0.0,
            "last_longitude": 12.5,
            "gps_valid": True,
            "last_valid_gps_time": datetime.now(timezone.utc).isoformat(),
            "gps_source": "watch_tcp",
        }]

        location = get_live_locations()[0]

        self.assertEqual(location["latitude"], 0.0)
        self.assertEqual(location["longitude"], 12.5)


if __name__ == "__main__":
    unittest.main()
