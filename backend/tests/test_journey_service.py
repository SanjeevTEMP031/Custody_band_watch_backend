"""
Unit Tests for Travel Distance Tracking, Route Highlighting & Journey Lifecycle
Verifies:
1. Start Journey captures initial valid GPS fix as START marker
2. Live Route Tracking records valid GPS fixes and ignores duplicate/invalid points
3. Haversine distance calculation accumulates distance accurately along route
4. End Journey captures final GPS fix as END marker and computes duration
5. Separate journeys do not combine distance
6. API routes (/api/v1/journeys/start, /end, /active, /device) operate cleanly
"""

import tempfile
import unittest
from pathlib import Path
from datetime import datetime, timedelta, timezone
from unittest.mock import patch, MagicMock
from app.services.journey_service import journey_service, format_duration
from app.services.gps_service import gps_service
from app.services.firestore_service import firestore_service
from app.journeys.journey_routes import (
    start_journey,
    end_journey,
    get_active_journey,
    StartJourneyRequest,
    EndJourneyRequest
)


class TestJourneyTracking(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.temp_db_path = Path(cls.temp_dir.name) / "test_custody.db"
        cls.db_patch1 = patch("app.services.firestore_service.LOCAL_DB", cls.temp_db_path)
        cls.db_patch2 = patch("app.services.journey_service.LOCAL_DB", cls.temp_db_path)
        cls.db_patch1.start()
        cls.db_patch2.start()
        firestore_service._init_local_db()

    @classmethod
    def tearDownClass(cls):
        cls.db_patch1.stop()
        cls.db_patch2.stop()
        cls.temp_dir.cleanup()

    def setUp(self):
        self.device_id = "863758061830955"

        # Mock Firestore client
        self.mock_db = MagicMock()
        self.saved_docs = {}

        def mock_set(data, merge=True):
            self.saved_docs[self.device_id] = dict(data)

        mock_doc = MagicMock()
        mock_doc.set.side_effect = mock_set
        mock_doc.get.return_value.exists = False
        mock_doc.get.return_value.to_dict.return_value = {}

        self.mock_db.collection.return_value.document.return_value = mock_doc
        self.mock_db.collection.return_value.document.return_value.collection.return_value.document.return_value = mock_doc
        self.mock_db.collection.return_value.document.return_value.collection.return_value.where.return_value.limit.return_value.stream.return_value = []
        self.mock_db.collection.return_value.document.return_value.collection.return_value.order_by.return_value.limit.return_value.stream.return_value = []

        self.fs_patch1 = patch("app.services.firestore_service.get_firestore_client", return_value=self.mock_db)
        self.fs_patch2 = patch("app.core.firebase.get_firestore_client", return_value=self.mock_db)
        self.fs_patch1.start()
        self.fs_patch2.start()

        journey_service._active_journeys.clear()

        # Seed initial device fix in local storage
        self.valid_gps_timestamp = datetime.now(timezone.utc).isoformat()
        firestore_service.save_device({
            "device_id": self.device_id,
            "last_latitude": 17.446317,
            "last_longitude": 78.368395,
            "gps_valid": True,
            "gps_source": "watch_tcp",
            "status": "ONLINE",
            "last_seen": self.valid_gps_timestamp,
            "last_valid_gps_time": self.valid_gps_timestamp
        })

    def tearDown(self):
        journey_service.end_journey(self.device_id)
        journey_service._active_journeys.clear()
        self.fs_patch1.stop()
        self.fs_patch2.stop()

    def test_1_start_journey_captures_initial_valid_fix(self):
        """START JOURNEY captures first valid GPS fix and sets active status."""
        journey = journey_service.start_journey(self.device_id)
        self.assertIsNotNone(journey)
        self.assertEqual(journey["device_id"], self.device_id)
        self.assertEqual(journey["status"], "ACTIVE")
        self.assertIsNotNone(journey["start_location"])
        self.assertAlmostEqual(journey["start_location"]["latitude"], 17.446317, places=4)
        self.assertAlmostEqual(journey["start_location"]["longitude"], 78.368395, places=4)
        self.assertEqual(journey["total_distance_km"], 0.0)
        self.assertEqual(len(journey["route_coordinates"]), 1)

    def test_start_journey_ignores_untrusted_or_stale_device_fix(self):
        stale_timestamp = (
            datetime.now(timezone.utc) - timedelta(minutes=16)
        ).isoformat()
        snapshots = [
            {
                "gps_valid": True,
                "last_latitude": 17.446317,
                "last_longitude": 78.368395,
                "last_valid_gps_time": self.valid_gps_timestamp,
            },
            {
                "gps_valid": True,
                "gps_source": "watch_tcp",
                "last_latitude": 17.446317,
                "last_longitude": 78.368395,
                "last_valid_gps_time": stale_timestamp,
            },
        ]

        for snapshot in snapshots:
            with self.subTest(snapshot=snapshot):
                journey_service._active_journeys.clear()
                with patch.object(
                    firestore_service, "get_device", return_value=snapshot
                ):
                    journey = journey_service.start_journey(self.device_id)
                    self.assertIsNone(journey["start_location"])
                    journey_service.end_journey(self.device_id)

        journey_service._active_journeys.clear()

    def test_2_live_route_tracking_accumulates_distance_haversine(self):
        """LIVE ROUTE TRACKING calculates travelled distance using Haversine formula."""
        journey = journey_service.start_journey(self.device_id)

        # Move ~500 meters East: 17.446317, 78.368395 -> 17.446317, 78.373100
        point2_lat, point2_lon = 17.446317, 78.373100
        expected_dist_m = gps_service.calculate_distance_meters(17.446317, 78.368395, point2_lat, point2_lon)
        expected_km = expected_dist_m / 1000.0

        updated = journey_service.record_journey_point(
            self.device_id,
            latitude=point2_lat,
            longitude=point2_lon,
            timestamp="2026-11-06T10:05:00Z",
            speed_kmh=15.0
        )

        self.assertIsNotNone(updated)
        self.assertEqual(len(updated["route_coordinates"]), 2)
        self.assertAlmostEqual(updated["total_distance_km"], expected_km, places=2)
        self.assertGreater(updated["total_distance_km"], 0.4)

    def test_3_ignore_duplicate_and_invalid_points(self):
        """Does not record duplicate points or invalid (0.0, 0.0) coordinates."""
        journey = journey_service.start_journey(self.device_id)
        initial_points = len(journey["route_coordinates"])

        # Duplicate point (exact same coordinates)
        journey_service.record_journey_point(
            self.device_id,
            latitude=17.446317,
            longitude=78.368395,
            timestamp="2026-11-06T10:01:00Z"
        )
        active = journey_service.get_active_journey(self.device_id)
        self.assertEqual(len(active["route_coordinates"]), initial_points)

        # Noise under 2.5 meters
        journey_service.record_journey_point(
            self.device_id,
            latitude=17.446317001,
            longitude=78.368395001,
            timestamp="2026-11-06T10:02:00Z"
        )
        active = journey_service.get_active_journey(self.device_id)
        self.assertEqual(len(active["route_coordinates"]), initial_points)

        # Invalid (0, 0) coordinates
        journey_service.record_journey_point(
            self.device_id,
            latitude=0.0,
            longitude=0.0,
            timestamp="2026-11-06T10:03:00Z"
        )
        active = journey_service.get_active_journey(self.device_id)
        self.assertEqual(len(active["route_coordinates"]), initial_points)

    def test_4_end_journey_captures_end_marker_and_duration(self):
        """END JOURNEY captures final valid coordinates as END marker and computes duration."""
        journey_service.start_journey(self.device_id)

        # Move to destination
        journey_service.record_journey_point(
            self.device_id,
            latitude=17.449000,
            longitude=78.372000,
            timestamp="2026-11-06T10:15:00Z",
            speed_kmh=12.0
        )

        completed = journey_service.end_journey(self.device_id)
        self.assertIsNotNone(completed)
        self.assertEqual(completed["status"], "COMPLETED")
        self.assertIsNotNone(completed["end_time"])
        self.assertIsNotNone(completed["end_location"])
        self.assertAlmostEqual(completed["end_location"]["latitude"], 17.449000, places=4)
        self.assertAlmostEqual(completed["end_location"]["longitude"], 78.372000, places=4)
        self.assertGreaterEqual(completed["duration_seconds"], 0)
        self.assertIn(":", completed["duration_formatted"])

        # Active cache should be cleared
        self.assertIsNone(journey_service._active_journeys.get(self.device_id))

    def test_5_separate_journeys_do_not_combine_distance(self):
        """Verifies distance is isolated per journey and never carries over between journeys."""
        # Journey 1
        j1 = journey_service.start_journey(self.device_id)
        journey_service.record_journey_point(self.device_id, 17.448000, 78.370000, "2026-11-06T10:05:00Z")
        j1_completed = journey_service.end_journey(self.device_id)
        d1 = j1_completed["total_distance_km"]

        # Journey 2 starts from new location
        firestore_service.save_device({
            "device_id": self.device_id,
            "last_latitude": 17.450000,
            "last_longitude": 78.380000,
            "gps_valid": True,
            "gps_source": "watch_tcp",
            "last_valid_gps_time": datetime.now(timezone.utc).isoformat()
        })
        j2 = journey_service.start_journey(self.device_id)
        self.assertEqual(j2["total_distance_km"], 0.0)  # Starts at 0.0

        # Move small distance in journey 2
        journey_service.record_journey_point(self.device_id, 17.450500, 78.380500, "2026-11-06T11:05:00Z")
        j2_completed = journey_service.end_journey(self.device_id)
        d2 = j2_completed["total_distance_km"]

        self.assertNotEqual(d2, d1)
        self.assertLess(d2, 0.2)  # Should only measure journey 2 segment

    def test_6_api_endpoints_start_end_and_active(self):
        """Tests REST API endpoints for Journey tracking."""
        # Start via API route function
        resp = start_journey(StartJourneyRequest(device_id=self.device_id))
        self.assertTrue(resp["success"])
        self.assertEqual(resp["journey"]["status"], "ACTIVE")

        # Active query route function
        active_resp = get_active_journey(self.device_id)
        self.assertTrue(active_resp["active"])
        self.assertIsNotNone(active_resp["journey"])

        # End via API route function
        end_resp = end_journey(EndJourneyRequest(device_id=self.device_id))
        self.assertTrue(end_resp["success"])
        self.assertEqual(end_resp["journey"]["status"], "COMPLETED")

        # Active query now returns False
        active_resp_after = get_active_journey(self.device_id)
        self.assertFalse(active_resp_after["active"])

    def test_7_auto_create_journey_on_first_valid_fix(self):
        """Verifies automatic journey creation on first verified GPS fix when none exists."""
        journey_service.end_journey(self.device_id)
        journey_service._active_journeys.clear()

        now_str = datetime.now(timezone.utc).isoformat()
        lat, lon = 17.447000, 78.369000
        res = journey_service.record_journey_point(
            self.device_id,
            latitude=lat,
            longitude=lon,
            timestamp=now_str,
            speed_kmh=22.5
        )

        self.assertIsNotNone(res)
        self.assertEqual(res["status"], "ACTIVE")
        self.assertEqual(res["points_count"], 1)
        self.assertIsNotNone(res["start_location"])
        self.assertAlmostEqual(res["start_location"]["latitude"], lat, places=5)
        self.assertAlmostEqual(res["start_location"]["longitude"], lon, places=5)

        # Check get_active_journey returns it
        active = journey_service.get_active_journey(self.device_id)
        self.assertIsNotNone(active)
        self.assertEqual(active["journey_id"], res["journey_id"])

    def test_8_continuous_travel_remains_single_journey(self):
        """Verifies continuous travel across multiple hours remains ONE single persistent journey."""
        journey_service.end_journey(self.device_id)
        journey_service._active_journeys.clear()

        base_time = datetime.now(timezone.utc) - timedelta(hours=3)
        curr_lat, curr_lon = 17.440000, 78.360000

        # Simulate 180 minutes of continuous travel (point every 10 minutes)
        initial = journey_service.record_journey_point(
            self.device_id,
            latitude=curr_lat,
            longitude=curr_lon,
            timestamp=base_time.isoformat(),
            speed_kmh=30.0
        )
        self.assertIsNotNone(initial)
        initial_id = initial["journey_id"]

        for i in range(1, 18):
            step_time = base_time + timedelta(minutes=i * 10)
            curr_lat += 0.005
            curr_lon += 0.005
            res = journey_service.record_journey_point(
                self.device_id,
                latitude=curr_lat,
                longitude=curr_lon,
                timestamp=step_time.isoformat(),
                speed_kmh=35.0
            )
            self.assertIsNotNone(res)
            # Journey ID must remain identical
            self.assertEqual(res["journey_id"], initial_id)
            self.assertEqual(res["status"], "ACTIVE")

        final_active = journey_service.get_active_journey(self.device_id)
        self.assertEqual(final_active["journey_id"], initial_id)
        self.assertEqual(final_active["points_count"], 18)
        self.assertGreater(final_active["total_distance_km"], 5.0)

    def test_9_inactivity_timeout_auto_completes_journey(self):
        """Verifies inactivity > 30 mins auto-completes old journey and starts new one on next fix."""
        journey_service.end_journey(self.device_id)
        journey_service._active_journeys.clear()

        t0 = datetime.now(timezone.utc) - timedelta(hours=2)
        j1 = journey_service.record_journey_point(
            self.device_id,
            latitude=17.441000,
            longitude=78.361000,
            timestamp=t0.isoformat()
        )
        self.assertIsNotNone(j1)
        j1_id = j1["journey_id"]

        # Next point 35 minutes later (exceeds 30 min inactivity threshold)
        t1 = t0 + timedelta(minutes=35)
        j2 = journey_service.record_journey_point(
            self.device_id,
            latitude=17.450000,
            longitude=78.370000,
            timestamp=t1.isoformat()
        )
        self.assertIsNotNone(j2)
        self.assertNotEqual(j2["journey_id"], j1_id)
        self.assertEqual(j2["status"], "ACTIVE")

    def test_10_historical_point_does_not_auto_create_active_journey(self):
        """Verifies historical/replayed points older than 24 hours do not create a new active journey."""
        journey_service.end_journey(self.device_id)
        journey_service._active_journeys.clear()

        old_time = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
        res = journey_service.record_journey_point(
            self.device_id,
            latitude=17.440000,
            longitude=78.360000,
            timestamp=old_time
        )
        self.assertIsNone(res)
        self.assertIsNone(journey_service.get_active_journey(self.device_id))

    def test_11_start_journey_route_accepts_and_captures_coordinates(self):
        """Verifies start_journey endpoint captures start_latitude and start_longitude when provided."""
        journey_service.end_journey(self.device_id)
        journey_service._active_journeys.clear()

        req = StartJourneyRequest(
            device_id=self.device_id,
            start_latitude=17.444050,
            start_longitude=78.385460,
        )
        res = start_journey(req)
        self.assertTrue(res["success"])
        journey = res["journey"]
        self.assertEqual(journey["start_latitude"], 17.44405)
        self.assertEqual(journey["start_longitude"], 78.38546)
        self.assertIsNotNone(journey["start_location"])
        self.assertEqual(journey["start_location"]["latitude"], 17.44405)
        self.assertEqual(journey["start_location"]["longitude"], 78.38546)
        self.assertEqual(len(journey["route_coordinates"]), 1)


if __name__ == "__main__":
    unittest.main()
