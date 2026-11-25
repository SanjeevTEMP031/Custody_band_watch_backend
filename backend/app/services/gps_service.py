"""
GPS Service
Coordinate parsing, Google Maps link generation,
distance calculation & geofence checking.
"""

import math
import concurrent.futures
from typing import Dict, Any

from ..utils.logger import gps_logger
from .firestore_service import (
    firestore_service,
    normalize_verified_gps_fix,
)


# Dedicated thread pool for non-blocking geofence evaluations.
# Single worker preserves submission order within this process.
_geofence_executor = concurrent.futures.ThreadPoolExecutor(
    max_workers=1,
    thread_name_prefix="geofence_eval_worker",
)


def _dispatch_geofence_evaluation(
    device_id: str,
    latitude: float,
    longitude: float,
    timestamp: str,
    accuracy: float,
):
    """Evaluate an accepted, current GPS location in the background."""
    try:
        from ..geofencing.evaluator import evaluate_geofence

        evaluate_geofence(
            device_id=device_id,
            latitude=latitude,
            longitude=longitude,
            timestamp=timestamp,
            accuracy=accuracy,
        )
    except Exception as exc:
        gps_logger.warning(
            "Geofence evaluation background worker failed "
            "device=%s exception=%s",
            device_id,
            exc,
        )


def _dispatch_handle_gps_unknown(device_id: str, reason: str):
    """Notify the evaluator when GPS verification fails."""
    try:
        from ..geofencing.evaluator import handle_device_gps_unknown

        handle_device_gps_unknown(
            device_id=device_id,
            reason=reason,
        )
    except Exception as exc:
        gps_logger.warning(
            "GPS unknown background handler failed "
            "device=%s exception=%s",
            device_id,
            exc,
        )


def _dispatch_route_monitoring(
    device_id: str,
    latitude: float,
    longitude: float,
    timestamp: str,
    accuracy: float,
    speed_kmh: float,
):
    """Evaluate an accepted, current GPS location against approved routes and schedules."""
    try:
        from ..journeys.route_monitor import route_monitor

        route_monitor.evaluate_gps_position(
            device_id=device_id,
            latitude=latitude,
            longitude=longitude,
            timestamp=timestamp,
            accuracy_meters=accuracy,
            speed_kmh=speed_kmh,
        )
    except Exception as exc:
        gps_logger.warning(
            "Route monitoring background worker failed "
            "device=%s exception=%s",
            device_id,
            exc,
        )


def _dispatch_handle_route_gps_loss(device_id: str, reason: str):
    """Notify the route monitor when GPS fix is lost."""
    try:
        from ..journeys.route_monitor import route_monitor

        route_monitor.handle_device_gps_loss(
            device_id=device_id,
            reason=reason,
        )
    except Exception as exc:
        gps_logger.warning(
            "Route GPS loss background handler failed "
            "device=%s exception=%s",
            device_id,
            exc,
        )



class GPSService:

    @staticmethod
    def build_maps_url(lat: float, lon: float) -> str:
        """Generate a Google Maps coordinate URL."""
        return (
            f"https://maps.google.com/"
            f"?q={lat:.6f},{lon:.6f}"
        )

    @staticmethod
    def calculate_distance_meters(
        lat1: float,
        lon1: float,
        lat2: float,
        lon2: float,
    ) -> float:
        """Calculate distance using the Haversine formula."""
        R = 6371000

        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)

        delta_phi = math.radians(lat2 - lat1)
        delta_lambda = math.radians(lon2 - lon1)

        a = (
            math.sin(delta_phi / 2.0) ** 2
            + math.cos(phi1)
            * math.cos(phi2)
            * math.sin(delta_lambda / 2.0) ** 2
        )

        c = 2 * math.atan2(
            math.sqrt(a),
            math.sqrt(max(0.0, 1.0 - a)),
        )

        return R * c

    @staticmethod
    def calculate_bearing(
        lat1: float,
        lon1: float,
        lat2: float,
        lon2: float,
    ) -> float:
        """Calculate compass heading from point 1 to point 2."""
        if lat1 == lat2 and lon1 == lon2:
            return 0.0

        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        delta_lambda = math.radians(lon2 - lon1)

        x = (
            math.sin(delta_lambda)
            * math.cos(phi2)
        )

        y = (
            math.cos(phi1) * math.sin(phi2)
            - math.sin(phi1)
            * math.cos(phi2)
            * math.cos(delta_lambda)
        )

        initial_bearing = math.atan2(x, y)

        compass_bearing = (
            math.degrees(initial_bearing) + 360.0
        ) % 360.0

        return round(compass_bearing, 1)

    @staticmethod
    def is_inside_geofence(
        lat: float,
        lon: float,
        center_lat: float,
        center_lon: float,
        radius_meters: float,
    ) -> bool:
        """Check whether a coordinate is inside a circular geofence."""
        dist = GPSService.calculate_distance_meters(
            lat,
            lon,
            center_lat,
            center_lon,
        )

        return dist <= radius_meters

    def process_location_record(
        self,
        record: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Enrich and persist GPS records.

        Current verified fixes:
          - Update journey tracking
          - Submit geofence evaluation
          - Save telemetry

        Stale verified fixes:
          - Do not trigger live geofence evaluation

        Invalid fixes:
          - Clear invalid coordinates
          - Notify the geofence GPS-unknown handler
        """

        lat = record.get("latitude")
        lon = record.get("longitude")

        # Verify GPS without freshness restriction.
        verified_fix = normalize_verified_gps_fix(
            record.get("gps_valid"),
            lat,
            lon,
            record.get("timestamp"),
            record.get("gps_source"),
            max_age_seconds=None,
        )

        # Independently check GPS freshness.
        current_fix = normalize_verified_gps_fix(
            record.get("gps_valid"),
            lat,
            lon,
            record.get("timestamp"),
            record.get("gps_source"),
        )

        is_valid = verified_fix is not None
        is_current = current_fix is not None

        record["gps_valid"] = is_valid
        record["gps_fix"] = is_valid
        record["gps_locked"] = is_valid
        record["gps_current"] = is_current

        record["gps_status"] = (
            "GPS Locked"
            if is_current
            else (
                "Location Stale"
                if is_valid
                else "No Fix"
            )
        )

        # Resolve travel direction.
        bearing = (
            record.get("heading")
            if record.get("heading") is not None
            else (
                record.get("bearing")
                if record.get("bearing") is not None
                else record.get("course")
            )
        )

        if (
            bearing is None
            and is_valid
            and lat is not None
            and lon is not None
        ):
            last_dev = firestore_service.get_local_device(
                record["device_id"]
            )

            if (
                last_dev
                and last_dev.get("last_latitude") is not None
                and last_dev.get("last_longitude") is not None
            ):
                p_lat = last_dev["last_latitude"]
                p_lon = last_dev["last_longitude"]

                if p_lat != lat or p_lon != lon:
                    bearing = self.calculate_bearing(
                        p_lat,
                        p_lon,
                        lat,
                        lon,
                    )
                else:
                    bearing = (
                        last_dev.get("heading")
                        or last_dev.get("bearing")
                        or 0.0
                    )
            else:
                bearing = 0.0

        if bearing is None:
            bearing = 0.0

        bearing = float(bearing)

        record["heading"] = bearing
        record["bearing"] = bearing
        record["course"] = bearing
        record["direction"] = bearing
        record["angle"] = bearing
        record["rotation"] = bearing

        # Process verified GPS location.
        if verified_fix is not None:

            lat, lon, timestamp, source = verified_fix

            record["timestamp"] = timestamp
            record["gps_source"] = source
            record["latitude"] = lat
            record["longitude"] = lon

            record["maps_url"] = self.build_maps_url(
                float(lat),
                float(lon),
            )

            gps_logger.info(
                "GPS report accepted "
                "device=%s source=%s current=%s",
                record["device_id"],
                source,
                is_current,
            )

            # Only current GPS fixes can update journeys
            # and trigger live geofence evaluation.
            if is_current:

                # Journey tracking.
                try:
                    from .journey_service import journey_service

                    journey_service.record_journey_point(
                        device_id=record["device_id"],
                        latitude=float(lat),
                        longitude=float(lon),
                        timestamp=record["timestamp"],
                        speed_kmh=float(
                            record.get("speed_kmh")
                            or record.get("speed")
                            or 0.0
                        ),
                    )

                except Exception as exc:
                    gps_logger.warning(
                        "Journey point write failed "
                        "device=%s exception_type=%s",
                        record["device_id"],
                        type(exc).__name__,
                    )

                # Geofence evaluation.
                # Runs in background, not on receiver thread.
                try:
                    _geofence_executor.submit(
                        _dispatch_geofence_evaluation,
                        record["device_id"],
                        float(lat),
                        float(lon),
                        record["timestamp"],
                        float(
                            record.get("accuracy_m")
                            or record.get("accuracy")
                            or 0.0
                        ),
                    )

                except RuntimeError as exc:
                    gps_logger.warning(
                        "Unable to queue geofence evaluation "
                        "device=%s exception=%s",
                        record["device_id"],
                        exc,
                    )

                # Route corridor and schedule monitoring.
                # Runs in background, not on receiver thread.
                try:
                    _geofence_executor.submit(
                        _dispatch_route_monitoring,
                        record["device_id"],
                        float(lat),
                        float(lon),
                        record["timestamp"],
                        float(
                            record.get("accuracy_m")
                            or record.get("accuracy")
                            or 0.0
                        ),
                        float(
                            record.get("speed_kmh")
                            or record.get("speed")
                            or 0.0
                        ),
                    )

                except RuntimeError as exc:
                    gps_logger.warning(
                        "Unable to queue route monitoring "
                        "device=%s exception=%s",
                        record["device_id"],
                        exc,
                    )

        else:
            # No verified GPS fix.
            record["latitude"] = None
            record["longitude"] = None
            record["gps_source"] = None
            record["maps_url"] = None

            gps_logger.info(
                "GPS report rejected "
                "device=%s reason=unverified_fix",
                record["device_id"],
            )

            try:
                _geofence_executor.submit(
                    _dispatch_handle_gps_unknown,
                    record["device_id"],
                    "unverified_fix",
                )
                _geofence_executor.submit(
                    _dispatch_handle_route_gps_loss,
                    record["device_id"],
                    "unverified_fix",
                )

            except RuntimeError as exc:
                gps_logger.warning(
                    "Unable to queue GPS unknown handler "
                    "device=%s exception=%s",
                    record["device_id"],
                    exc,
                )

        # Preserve existing telemetry persistence behavior.
        if is_valid:
            firestore_service.save_telemetry(
                record,
                wait_for_firestore=False,
            )

        return record


gps_service = GPSService()
