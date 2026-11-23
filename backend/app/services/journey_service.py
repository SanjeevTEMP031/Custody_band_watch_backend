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
        self._lock = threading.RLock()
        self._active_journeys: Dict[str, Dict[str, Any]] = {}
        # Inactivity timeout: 10 minutes (600s) default
        self.inactivity_timeout_seconds: float = float(
            getattr(settings, "JOURNEY_INACTIVITY_TIMEOUT_SECONDS", 600)
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

    def _is_journey_timed_out(
        self, journey: Dict[str, Any], current_dt: Optional[datetime] = None
    ) -> bool:
        """Check if an active journey has exceeded the inactivity timeout."""
        if not journey or journey.get("status") != "ACTIVE":
            return False

        last_time_str = None
        route = journey.get("route_coordinates", [])
        if route:
            last_time_str = route[-1].get("timestamp")
        if not last_time_str:
            last_time_str = journey.get("updated_at") or journey.get("start_time")

        last_dt = parse_iso_timestamp(last_time_str) if last_time_str else None
        if not last_dt:
            return False

        if last_dt.tzinfo is None:
            last_dt = last_dt.replace(tzinfo=timezone.utc)

        ref_dt = current_dt or datetime.now(timezone.utc)
        if ref_dt.tzinfo is None:
            ref_dt = ref_dt.replace(tzinfo=timezone.utc)

        return (ref_dt - last_dt).total_seconds() > self.inactivity_timeout_seconds

    def get_active_journey(self, device_id: str) -> Optional[Dict[str, Any]]:
        """Get an active journey from memory, SQLite, or Firestore, auto-completing if timed out."""
        with self._lock:
            journey = self._active_journeys.get(device_id)
            if not journey or journey.get("status") != "ACTIVE":
                persisted = firestore_service.get_active_journey(device_id)
                if persisted and persisted.get("status") == "ACTIVE":
                    journey = persisted
                    self._active_journeys[device_id] = persisted
                else:
                    return None

            if journey.get("status") != "ACTIVE" or self._is_journey_timed_out(journey):
                app_logger.info(
                    f"[JOURNEY AUTO-COMPLETE] Active journey {journey.get('journey_id')} for {device_id} "
                    f"inactive > {self.inactivity_timeout_seconds}s or marked non-active. Marking COMPLETED."
                )
                self._complete_journey_locked(device_id, journey)
                return None

            return journey

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
                if self._is_journey_timed_out(journey, current_dt=pt_dt):
                    app_logger.info(
                        f"[JOURNEY AUTO-COMPLETE] Inactivity gap detected for {device_id}. "
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
        self,
        device_id: str,
        journey: Dict[str, Any],
        end_time_override: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Internal helper to mark a journey completed and persist it."""
        route = journey.get("route_coordinates", [])
        end_location = route[-1] if route else None

        if not end_location:
            device = firestore_service.get_device(device_id) or {}
            candidate = self._get_initial_device_location(device, device_id)
            if candidate:
                end_location = candidate

        last_fix_time = end_location.get("timestamp") if end_location else None
        completed_time = end_time_override or last_fix_time or current_iso_timestamp()

        journey["status"] = "COMPLETED"
        journey["end_time"] = completed_time
        journey["end_location"] = end_location
        if end_location:
            journey["end_latitude"] = end_location.get("latitude")
            journey["end_longitude"] = end_location.get("longitude")
        if journey.get("start_location"):
            journey["start_latitude"] = journey["start_location"].get("latitude")
            journey["start_longitude"] = journey["start_location"].get("longitude")
        journey["updated_at"] = completed_time
        journey["points_count"] = len(route)

        self._update_journey_duration(journey, completed_time)

        active = self._active_journeys.get(device_id)
        if active and active.get("journey_id") == journey.get("journey_id"):
            self._active_journeys.pop(device_id, None)

        firestore_service.save_journey(journey)
        app_logger.info(
            f"[JOURNEY COMPLETED] Device {device_id}: "
            f"Journey={journey.get('journey_id')}, "
            f"Distance={journey.get('total_distance_km', 0.0)} km, "
            f"Duration={journey.get('duration_formatted', '00:00:00')}, "
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

            now_iso = current_iso_timestamp()
            return self._complete_journey_locked(device_id, journey, end_time_override=now_iso)

    def process_unassigned_telemetry(self, device_id: Optional[str] = None) -> int:
        """
        Processes unassigned telemetry points (journey_id is NULL) into persistent journeys.
        Automatically segments continuous travel using inactivity_timeout_seconds,
        accumulates Haversine distance and duration, saves journeys into SQLite and Firestore,
        and tags each telemetry row with its assigned journey_id.
        """
        with self._lock:
            try:
                conn = sqlite3.connect(str(LOCAL_DB))
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()

                query = """
                    SELECT id, device_id, timestamp, latitude, longitude, speed_kmh
                    FROM telemetry
                    WHERE (journey_id IS NULL OR TRIM(journey_id) = '')
                      AND gps_valid = 1
                      AND latitude BETWEEN -90 AND 90
                      AND longitude BETWEEN -180 AND 180
                      AND NOT (latitude = 0 AND longitude = 0)
                """
                params = []
                if device_id:
                    query += " AND device_id = ?"
                    params.append(str(device_id))
                query += " ORDER BY device_id, julianday(timestamp) ASC, id ASC"

                rows = cur.execute(query, params).fetchall()
                conn.close()
            except Exception as e:
                app_logger.error(f"[JOURNEY] Error querying unassigned telemetry: {e}")
                return 0

            if not rows:
                return 0

            # Group rows by device_id
            devices_map: Dict[str, List[sqlite3.Row]] = {}
            for r in rows:
                devices_map.setdefault(r["device_id"], []).append(r)

            created_count = 0
            now_dt = datetime.now(timezone.utc)
            now_iso = current_iso_timestamp()

            for dev_id, dev_rows in devices_map.items():
                groups: List[List[sqlite3.Row]] = []
                current_group: List[sqlite3.Row] = []
                prev_time: Optional[datetime] = None

                for r in dev_rows:
                    dt = parse_iso_timestamp(r["timestamp"])
                    if not dt:
                        continue
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)

                    if prev_time and (dt - prev_time).total_seconds() > self.inactivity_timeout_seconds:
                        if current_group:
                            groups.append(current_group)
                        current_group = [r]
                    else:
                        current_group.append(r)
                    prev_time = dt

                if current_group:
                    groups.append(current_group)

                for group in groups:
                    if not group:
                        continue

                    # Filter coordinates and calculate distance
                    route_coords: List[Dict[str, Any]] = []
                    total_dist_km = 0.0
                    tagged_ids: List[int] = []

                    for pt in group:
                        tagged_ids.append(pt["id"])
                        lat = round(float(pt["latitude"]), 6)
                        lon = round(float(pt["longitude"]), 6)
                        ts = normalize_iso_timestamp(pt["timestamp"]) or pt["timestamp"]
                        spd = round(float(pt["speed_kmh"] or 0.0), 1)

                        pt_obj = {
                            "latitude": lat,
                            "longitude": lon,
                            "timestamp": ts,
                            "speed_kmh": spd,
                        }

                        if not route_coords:
                            route_coords.append(pt_obj)
                            continue

                        prev_pt = route_coords[-1]
                        dist_m = gps_service.calculate_distance_meters(
                            prev_pt["latitude"], prev_pt["longitude"], lat, lon
                        )

                        # Filter exact duplicate
                        if prev_pt["latitude"] == lat and prev_pt["longitude"] == lon:
                            continue

                        # Filter micro-jitter (< 2.5m)
                        if dist_m < 2.5:
                            continue

                        # Filter speed jump (> 180 km/h)
                        try:
                            t_prev = parse_iso_timestamp(prev_pt["timestamp"])
                            t_curr = parse_iso_timestamp(ts)
                            if t_prev and t_curr:
                                if t_prev.tzinfo is None:
                                    t_prev = t_prev.replace(tzinfo=timezone.utc)
                                if t_curr.tzinfo is None:
                                    t_curr = t_curr.replace(tzinfo=timezone.utc)
                                dt_s = (t_curr - t_prev).total_seconds()
                                if dt_s > 0 and (dist_m / dt_s) * 3.6 > 180.0:
                                    continue
                        except Exception:
                            pass

                        route_coords.append(pt_obj)
                        total_dist_km += dist_m / 1000.0

                    if not route_coords:
                        continue

                    start_pt = route_coords[0]
                    end_pt = route_coords[-1]
                    st_time = start_pt["timestamp"]
                    et_time = end_pt["timestamp"]

                    st_dt = parse_iso_timestamp(st_time)
                    et_dt = parse_iso_timestamp(et_time)
                    if st_dt and st_dt.tzinfo is None:
                        st_dt = st_dt.replace(tzinfo=timezone.utc)
                    if et_dt and et_dt.tzinfo is None:
                        et_dt = et_dt.replace(tzinfo=timezone.utc)

                    dur_seconds = max(0, int((et_dt - st_dt).total_seconds())) if st_dt and et_dt else 0
                    dur_fmt = format_duration(dur_seconds)

                    # Determine journey status
                    is_active = et_dt and (now_dt - et_dt).total_seconds() <= self.inactivity_timeout_seconds
                    status = "ACTIVE" if is_active else "COMPLETED"

                    # Generate clean journey ID based on start time
                    st_tag = st_dt.strftime("%Y%m%d_%H%M%S") if st_dt else uuid.uuid4().hex[:12]
                    journey_id = f"journey_{dev_id}_{st_tag}"

                    journey = {
                        "journey_id": journey_id,
                        "id": journey_id,
                        "device_id": dev_id,
                        "status": status,
                        "start_time": st_time,
                        "end_time": et_time if not is_active else None,
                        "start_location": start_pt,
                        "start_latitude": start_pt["latitude"],
                        "start_longitude": start_pt["longitude"],
                        "end_location": end_pt if not is_active else None,
                        "end_latitude": end_pt["latitude"] if not is_active else None,
                        "end_longitude": end_pt["longitude"] if not is_active else None,
                        "route_coordinates": route_coords,
                        "total_distance_km": round(total_dist_km, 3),
                        "duration_seconds": dur_seconds,
                        "duration_formatted": dur_fmt,
                        "points_count": len(route_coords),
                        "created_at": st_time,
                        "updated_at": et_time or now_iso,
                    }

                    if is_active:
                        self._active_journeys[dev_id] = journey

                    firestore_service.save_journey(journey)

                    # Tag telemetry rows in SQLite
                    try:
                        conn = sqlite3.connect(str(LOCAL_DB))
                        for i in range(0, len(tagged_ids), 500):
                            chunk = tagged_ids[i:i + 500]
                            placeholders = ",".join("?" for _ in chunk)
                            conn.execute(
                                f"UPDATE telemetry SET journey_id = ? WHERE id IN ({placeholders})",
                                [journey_id] + chunk,
                            )
                        conn.commit()
                        conn.close()
                    except Exception as exc:
                        app_logger.error(f"[JOURNEY] Error tagging telemetry chunk: {exc}")

                    app_logger.info(
                        f"[JOURNEY CREATED] Created {status} journey {journey_id} for {dev_id}: "
                        f"dist={journey['total_distance_km']} km, pts={journey['points_count']}, "
                        f"start={st_time}, end={et_time}"
                    )
                    created_count += 1

            return created_count

    def get_device_journeys(
        self, device_id: str, limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Get journey history for a device, processing any pending untagged telemetry first."""
        self.process_unassigned_telemetry(device_id)
        return firestore_service.get_device_journeys(device_id, limit=limit)

    def get_journey(self, journey_id: str) -> Optional[Dict[str, Any]]:
        """Get journey details by ID."""
        return firestore_service.get_journey(journey_id)


journey_service = JourneyService()
