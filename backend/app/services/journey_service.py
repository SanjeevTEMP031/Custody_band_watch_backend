"""
Journey Tracking Service
Manages custody band journeys, GPS route tracking,
distance calculation, automatic journey lifecycle, and Firestore/SQLite synchronization.
"""

import math
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

from ..config import settings
from ..utils.datetime_utils import current_iso_timestamp
from ..utils.logger import app_logger, gps_logger
from .firestore_service import (
    firestore_service,
    normalize_verified_gps_fix,
    normalize_iso_timestamp,
    parse_iso_timestamp,
    LOCAL_DB,
)
from .gps_service import gps_service


def format_duration(seconds: float) -> str:
    """Format seconds as HH:MM:SS."""
    total = int(max(0, seconds))
    hours = total // 3600
    minutes = (total % 3600) // 60
    secs = total % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


class JourneyService:
    def __init__(self):
        self._lock = threading.Lock()
        self._active_journeys: Dict[str, Dict[str, Any]] = {}
        # Inactivity timeout: 30 minutes (1800s) default
        self.inactivity_timeout_seconds: float = float(
            getattr(settings, "JOURNEY_INACTIVITY_TIMEOUT_SECONDS", 1800)
        )

    @staticmethod
    def _valid_coordinates(latitude: Any, longitude: Any) -> bool:
        """Check whether coordinates are valid, finite, and non-zero."""
        try:
            lat = float(latitude)
            lon = float(longitude)
            return (
                math.isfinite(lat)
                and math.isfinite(lon)
                and -90 <= lat <= 90
                and -180 <= lon <= 180
                and (lat != 0.0 or lon != 0.0)
            )
        except (TypeError, ValueError):
            return False

    def _get_initial_device_location(
        self, device: Dict[str, Any], device_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Read the last known verified GPS location for a journey endpoint."""
        lat = device.get("last_latitude")
        lon = device.get("last_longitude")

        if lat is None:
            lat = device.get("latitude")
        if lon is None:
            lon = device.get("longitude")

        gps_status = str(
            device.get("gps_status") or device.get("gps_fix_status") or ""
        ).strip().upper()
        if gps_status == "V":
            return None

        fix = normalize_verified_gps_fix(
            device.get("gps_valid"),
            lat,
            lon,
            device.get("last_valid_gps_time"),
            device.get("gps_source"),
        )
        if fix is None:
            return None

        latitude, longitude, timestamp, _ = fix
        return {
            "latitude": round(float(latitude), 6),
            "longitude": round(float(longitude), 6),
            "timestamp": timestamp,
            "speed_kmh": round(
                float(device.get("speed_kmh") or device.get("speed") or 0.0), 1
            ),
        }

    def _tag_telemetry_journey(
        self, device_id: str, timestamp: str, journey_id: str
    ) -> None:
        """Associate SQLite telemetry record with the active journey ID."""
        try:
            conn = sqlite3.connect(str(LOCAL_DB))
            conn.execute(
                """
                UPDATE telemetry
                SET journey_id = ?
                WHERE device_id = ?
                  AND (timestamp = ? OR julianday(timestamp) = julianday(?))
                """,
                (journey_id, device_id, timestamp, timestamp),
            )
            conn.commit()
            conn.close()
        except Exception as exc:
            app_logger.debug(
                f"[JOURNEY] Failed to tag telemetry with journey_id: {exc}"
            )

    @staticmethod
    def _update_journey_duration(journey: Dict[str, Any], current_timestamp: str) -> None:
        """Recalculate elapsed journey duration in seconds and formatted string."""
        try:
            start_iso = journey.get("start_time")
            if not start_iso:
                return
            st = parse_iso_timestamp(start_iso)
            ct = parse_iso_timestamp(current_timestamp)
            if st and ct:
                dur = max(0, int((ct - st).total_seconds()))
                journey["duration_seconds"] = dur
                journey["duration_formatted"] = format_duration(dur)
        except Exception:
            pass

    def get_active_journey(self, device_id: str) -> Optional[Dict[str, Any]]:
        """Get an active journey from memory, SQLite, or Firestore."""
        journey = self._active_journeys.get(device_id)
        if journey and journey.get("status") == "ACTIVE":
            return journey

        persisted = firestore_service.get_active_journey(device_id)
        if persisted and persisted.get("status") == "ACTIVE":
            self._active_journeys[device_id] = persisted
            return persisted

        return None

    def start_journey(
        self,
        device_id: str,
        start_time: Optional[str] = None,
        initial_location: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Start a new journey and capture its initial valid GPS fix.
        Thread-safe; if an active journey exists, returns it without creating a duplicate.
        """
        with self._lock:
            existing = self.get_active_journey(device_id)
            if existing and existing.get("status") == "ACTIVE":
                # Check if this existing journey is stale (> inactivity_timeout_seconds)
                last_activity = existing.get("updated_at") or existing.get("start_time")
                last_dt = parse_iso_timestamp(last_activity)
                now_dt = datetime.now(timezone.utc)
                if last_dt and (now_dt - last_dt).total_seconds() > self.inactivity_timeout_seconds:
                    app_logger.info(
                        f"[JOURNEY] Existing active journey {existing.get('journey_id')} for {device_id} is stale "
                        f"({(now_dt - last_dt).total_seconds():.0f}s inactive). Auto-completing."
                    )
                    self._complete_journey_locked(device_id, existing)
                    existing = None
                else:
                    # If existing journey lacks start_location or start_latitude, attach coordinates now
                    if not existing.get("start_location") or existing.get("start_latitude") is None:
                        loc = None
                        if initial_location and self._valid_coordinates(
                            initial_location.get("latitude"), initial_location.get("longitude")
                        ):
                            loc = {
                                "latitude": round(float(initial_location["latitude"]), 6),
                                "longitude": round(float(initial_location["longitude"]), 6),
                                "timestamp": initial_location.get("timestamp") or current_iso_timestamp(),
                                "speed_kmh": round(float(initial_location.get("speed_kmh") or 0.0), 1),
                            }
                        else:
                            device = firestore_service.get_device(device_id) or {}
                            loc = self._get_initial_device_location(device, device_id)

                        if loc:
                            existing["start_location"] = loc
                            existing["start_latitude"] = loc["latitude"]
                            existing["start_longitude"] = loc["longitude"]
                            if not existing.get("route_coordinates"):
                                existing["route_coordinates"] = [loc]
                                existing["points_count"] = 1
                            firestore_service.save_journey(existing)
                    return existing

            now_iso = current_iso_timestamp()
            st = start_time or now_iso
            journey_id = f"journey_{device_id}_{uuid.uuid4().hex[:12]}"

            start_location = None
            if initial_location and self._valid_coordinates(
                initial_location.get("latitude"), initial_location.get("longitude")
            ):
                start_location = {
                    "latitude": round(float(initial_location["latitude"]), 6),
                    "longitude": round(float(initial_location["longitude"]), 6),
                    "timestamp": initial_location.get("timestamp") or st,
                    "speed_kmh": round(float(initial_location.get("speed_kmh") or 0.0), 1),
                }
            else:
                device = firestore_service.get_device(device_id) or {}
                start_location = self._get_initial_device_location(device, device_id)

            route_coordinates: List[Dict[str, Any]] = []
            if start_location:
                route_coordinates.append(start_location)

            start_lat = start_location.get("latitude") if start_location else None
            start_lon = start_location.get("longitude") if start_location else None

            journey: Dict[str, Any] = {
                "journey_id": journey_id,
                "id": journey_id,
                "device_id": device_id,
                "status": "ACTIVE",
                "start_time": st,
                "end_time": None,
                "start_location": start_location,
                "start_latitude": start_lat,
                "start_longitude": start_lon,
                "end_location": None,
                "end_latitude": None,
                "end_longitude": None,
                "route_coordinates": route_coordinates,
                "total_distance_km": 0.0,
                "duration_seconds": 0,
                "duration_formatted": "00:00:00",
                "points_count": len(route_coordinates),
                "created_at": now_iso,
                "updated_at": st,
            }

            self._active_journeys[device_id] = journey
            firestore_service.save_journey(journey)

            app_logger.info(
                f"[JOURNEY START] Device {device_id}, "
                f"Journey {journey_id}, "
                f"Initial GPS fix: {bool(start_location)}"
            )
            return journey

    def record_journey_point(
        self,
        device_id: str,
        latitude: float,
        longitude: float,
        timestamp: str,
        speed_kmh: float = 0.0,
    ) -> Optional[Dict[str, Any]]:
        """
        Record a valid GPS point and calculate travelled distance.
        Automatically starts a new persistent journey on the first valid incoming fix
        if no active journey exists.
        Thread-safe; maintains continuous travel across 15m, 45m, 1h, 2h, 4h, 8h+ trips.
        """
        if not self._valid_coordinates(latitude, longitude):
            gps_logger.warning(
                f"[JOURNEY REJECTED] Invalid coordinates for {device_id}: "
                f"lat={latitude}, lon={longitude}"
            )
            return None

        pt_dt = parse_iso_timestamp(timestamp)
        if not pt_dt:
            try:
                pt_dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            except Exception:
                gps_logger.warning(
                    f"[JOURNEY REJECTED] Unparseable timestamp for {device_id}: {timestamp}"
                )
                return None

        if pt_dt.tzinfo is None:
            pt_dt = pt_dt.replace(tzinfo=timezone.utc)

        now_dt = datetime.now(timezone.utc)
        # Reject future timestamps beyond 10 minutes
        if (pt_dt - now_dt).total_seconds() > 600:
            gps_logger.warning(
                f"[JOURNEY REJECTED] Future timestamp for {device_id}: {timestamp}"
            )
            return None

        normalized_timestamp = normalize_iso_timestamp(timestamp) or pt_dt.isoformat()

        with self._lock:
            journey = self.get_active_journey(device_id)

            if journey and journey.get("status") == "ACTIVE":
                # Check inactivity gap between this point and the last point of the journey
                route = journey.get("route_coordinates", [])
                last_time_str = route[-1].get("timestamp") if route else (journey.get("updated_at") or journey.get("start_time"))
                last_dt = parse_iso_timestamp(last_time_str)
                if last_dt and pt_dt:
                    if last_dt.tzinfo is None:
                        last_dt = last_dt.replace(tzinfo=timezone.utc)
                    gap_seconds = (pt_dt - last_dt).total_seconds()
                    # Inactivity policy: a positive gap > inactivity_timeout_seconds ends previous journey
                    if gap_seconds > self.inactivity_timeout_seconds:
                        app_logger.info(
                            f"[JOURNEY AUTO-COMPLETE] Inactivity gap of {gap_seconds:.0f}s for {device_id}. "
                            f"Ending journey {journey.get('journey_id')}"
                        )
                        self._complete_journey_locked(device_id, journey)
                        journey = None

            # Auto-create journey if none is active
            if not journey or journey.get("status") != "ACTIVE":
                # Guard against historical / replayed GPS points starting a live active journey
                # If packet is older than 24 hours from current real time, do not auto-create
                packet_age_seconds = (now_dt - pt_dt).total_seconds()
                if packet_age_seconds > 86400:
                    gps_logger.info(
                        f"[JOURNEY REJECTED] Historical GPS point for {device_id} "
                        f"(age {packet_age_seconds:.0f}s); not starting new active journey"
                    )
                    return None

                now_iso = current_iso_timestamp()
                journey_id = f"journey_{device_id}_{int(pt_dt.timestamp())}_{uuid.uuid4().hex[:6]}"
                first_point = {
                    "latitude": round(float(latitude), 6),
                    "longitude": round(float(longitude), 6),
                    "timestamp": normalized_timestamp,
                    "speed_kmh": round(float(speed_kmh or 0.0), 1),
                }
                journey = {
                    "journey_id": journey_id,
                    "id": journey_id,
                    "device_id": device_id,
                    "status": "ACTIVE",
                    "start_time": normalized_timestamp,
                    "end_time": None,
                    "start_location": first_point,
                    "start_latitude": first_point["latitude"],
                    "start_longitude": first_point["longitude"],
                    "end_location": None,
                    "end_latitude": None,
                    "end_longitude": None,
                    "route_coordinates": [first_point],
                    "total_distance_km": 0.0,
                    "duration_seconds": 0,
                    "duration_formatted": "00:00:00",
                    "points_count": 1,
                    "created_at": now_iso,
                    "updated_at": normalized_timestamp,
                }
                self._active_journeys[device_id] = journey
                firestore_service.save_journey(journey)
                self._tag_telemetry_journey(device_id, normalized_timestamp, journey_id)

                app_logger.info(
                    f"[JOURNEY AUTO-START] Device {device_id} started journey {journey_id} "
                    f"at ({first_point['latitude']}, {first_point['longitude']})"
                )
                return journey

            # Existing active journey: append point and compute metrics
            new_point = {
                "latitude": round(float(latitude), 6),
                "longitude": round(float(longitude), 6),
                "timestamp": normalized_timestamp,
                "speed_kmh": round(float(speed_kmh or 0.0), 1),
            }

            route = journey.setdefault("route_coordinates", [])
            if not journey.get("start_location") or journey.get("start_latitude") is None:
                journey["start_location"] = new_point
                journey["start_latitude"] = new_point["latitude"]
                journey["start_longitude"] = new_point["longitude"]
                journey["route_coordinates"] = [new_point]
                journey["points_count"] = 1
                journey["updated_at"] = normalized_timestamp
                self._update_journey_duration(journey, normalized_timestamp)
                firestore_service.save_journey(journey)
                self._tag_telemetry_journey(device_id, normalized_timestamp, journey["journey_id"])
                return journey

            if route:
                previous_point = route[-1]
                prev_lat = float(previous_point["latitude"])
                prev_lon = float(previous_point["longitude"])

                # Check exact coordinate duplicate
                if (
                    prev_lat == new_point["latitude"]
                    and prev_lon == new_point["longitude"]
                ):
                    journey["updated_at"] = normalized_timestamp
                    self._update_journey_duration(journey, normalized_timestamp)
                    firestore_service.save_journey(journey)
                    self._tag_telemetry_journey(device_id, normalized_timestamp, journey["journey_id"])
                    return journey

                distance_meters = gps_service.calculate_distance_meters(
                    prev_lat,
                    prev_lon,
                    new_point["latitude"],
                    new_point["longitude"],
                )

                # Filter GPS jitter under 2.5 metres
                if distance_meters < 2.5:
                    journey["updated_at"] = normalized_timestamp
                    self._update_journey_duration(journey, normalized_timestamp)
                    firestore_service.save_journey(journey)
                    self._tag_telemetry_journey(device_id, normalized_timestamp, journey["journey_id"])
                    return journey

                # Speed jump filtering (> 180 km/h)
                try:
                    prev_pt_dt = parse_iso_timestamp(previous_point["timestamp"])
                    if prev_pt_dt:
                        if prev_pt_dt.tzinfo is None:
                            prev_pt_dt = prev_pt_dt.replace(tzinfo=timezone.utc)
                        time_delta_sec = (pt_dt - prev_pt_dt).total_seconds()
                        if time_delta_sec > 0:
                            calc_speed_kmh = (distance_meters / time_delta_sec) * 3.6
                            if calc_speed_kmh > 180.0:
                                gps_logger.warning(
                                    f"[JOURNEY REJECTED JUMP] Abnormal speed {calc_speed_kmh:.1f} km/h "
                                    f"for {device_id} ({distance_meters:.1f}m in {time_delta_sec:.1f}s)"
                                )
                                return journey
                except Exception:
                    pass

                incremental_km = distance_meters / 1000.0
                journey["total_distance_km"] = round(
                    float(journey.get("total_distance_km", 0.0)) + incremental_km,
                    3,
                )

            route.append(new_point)
            journey["route_coordinates"] = route
            journey["points_count"] = len(route)
            journey["updated_at"] = normalized_timestamp
            self._update_journey_duration(journey, normalized_timestamp)

            firestore_service.save_journey(journey)
            self._tag_telemetry_journey(device_id, normalized_timestamp, journey["journey_id"])

            gps_logger.info(
                f"[JOURNEY POINT ACCEPTED] Device {device_id} Journey={journey['journey_id']} "
                f"Points={journey['points_count']} Distance={journey['total_distance_km']} km"
            )
            return journey

    def _complete_journey_locked(
        self, device_id: str, journey: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Internal helper to mark a journey completed and persist it."""
        now_iso = current_iso_timestamp()
        route = journey.get("route_coordinates", [])
        end_location = route[-1] if route else None

        if not end_location:
            device = firestore_service.get_device(device_id) or {}
            candidate = self._get_initial_device_location(device, device_id)
            if candidate:
                end_location = candidate

        journey["status"] = "COMPLETED"
        journey["end_time"] = now_iso
        journey["end_location"] = end_location
        if end_location:
            journey["end_latitude"] = end_location.get("latitude")
            journey["end_longitude"] = end_location.get("longitude")
        if journey.get("start_location"):
            journey["start_latitude"] = journey["start_location"].get("latitude")
            journey["start_longitude"] = journey["start_location"].get("longitude")
        journey["updated_at"] = now_iso
        journey["points_count"] = len(route)

        self._update_journey_duration(journey, now_iso)

        active = self._active_journeys.get(device_id)
        if active and active.get("journey_id") == journey.get("journey_id"):
            self._active_journeys.pop(device_id, None)

        firestore_service.save_journey(journey)
        app_logger.info(
            f"[JOURNEY COMPLETED] Device {device_id}: "
            f"Journey={journey.get('journey_id')}, "
            f"Distance={journey['total_distance_km']} km, "
            f"Duration={journey['duration_formatted']}, "
            f"Points={len(route)}"
        )
        return journey

    def end_journey(
        self,
        device_id: str,
        journey_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Complete an active journey and save its final details. Thread-safe."""
        with self._lock:
            if journey_id:
                journey = firestore_service.get_journey(journey_id)
                if journey and journey.get("device_id") != device_id:
                    app_logger.warning(
                        f"[JOURNEY END] Journey {journey_id} does not belong to device {device_id}"
                    )
                    return None
            else:
                journey = self.get_active_journey(device_id)

            if not journey or journey.get("status") != "ACTIVE":
                app_logger.warning(
                    f"[JOURNEY END] No active journey found for {device_id}"
                )
                return None

            return self._complete_journey_locked(device_id, journey)

    def get_device_journeys(
        self, device_id: str, limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Get journey history for a device."""
        return firestore_service.get_device_journeys(device_id, limit=limit)

    def get_journey(self, journey_id: str) -> Optional[Dict[str, Any]]:
        """Get journey details by ID."""
        return firestore_service.get_journey(journey_id)


journey_service = JourneyService()
