"""
Route Corridor Monitoring & Schedule Verification Engine
Evaluates live verified GPS points against officer-approved journey routes,
calculates geodesic perpendicular distance in meters using Shapely projection,
detects route deviations and rejoins with hysteresis confirmation,
and monitors scheduled departure, arrival deadlines, and destination arrival.
"""

import json
import math
import threading
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

from shapely.geometry import LineString, Point

from ..config import settings
from ..utils.logger import app_logger, gps_logger
from ..utils.datetime_utils import current_iso_timestamp, parse_to_utc_datetime
from ..services.alert_service import alert_service, ws_manager
from ..services.firestore_service import GPS_FIX_MAX_AGE_SECONDS
from ..services.alert_config import ALERT_CONFIG
from .models import (
    get_journey_by_id,
    get_active_journey_for_device,
    get_selected_route_for_journey,
    update_journey_record,
    record_journey_gps_point,
    record_journey_event,
)

_monitor_lock = threading.RLock()

# Configurable route deviation thresholds
DEFAULT_CORRIDOR_METERS = 75.0
DEFAULT_CONFIRMATION_READINGS = 2
DEFAULT_ORIGIN_RADIUS_METERS = 150.0
DEFAULT_DESTINATION_RADIUS_METERS = 150.0


def parse_utc_dt(ts_str: Optional[str]) -> Optional[datetime]:
    """Parses an ISO-8601 string to timezone-aware UTC datetime."""
    return parse_to_utc_datetime(ts_str)


def calculate_haversine_distance_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculates great-circle distance between two GPS coordinates in meters."""
    R = 6371000.0  # Earth radius in meters
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = math.sin(delta_phi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2.0) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(max(0.0, 1.0 - a)))
    return R * c


def calculate_perpendicular_distance_to_route(
    lat: float,
    lon: float,
    route_coordinates: List[List[float]]
) -> float:
    """
    Calculates the exact shortest perpendicular distance in meters
    from point (lat, lon) to a GeoJSON LineString [[lon, lat], ...].
    Uses local equirectangular meter projection around the route centroid.
    """
    if not route_coordinates or len(route_coordinates) < 2:
        return 0.0

    # GeoJSON coordinates format: [longitude, latitude]
    center_lat = route_coordinates[0][1]
    center_lon = route_coordinates[0][0]

    lat_rad = math.radians(center_lat)
    meters_per_deg_lon = 111319.5 * math.cos(lat_rad)
    meters_per_deg_lat = 111132.9

    projected_line_pts = []
    for pt in route_coordinates:
        px = (pt[0] - center_lon) * meters_per_deg_lon
        py = (pt[1] - center_lat) * meters_per_deg_lat
        projected_line_pts.append((px, py))

    line = LineString(projected_line_pts)
    point_x = (lon - center_lon) * meters_per_deg_lon
    point_y = (lat - center_lat) * meters_per_deg_lat
    point = Point(point_x, point_y)

    dist_meters = line.distance(point)
    return round(float(dist_meters), 2)


class RouteMonitor:
    def __init__(self):
        self._states: Dict[str, Dict[str, Any]] = {}

    def _get_or_create_state(self, journey_id: str) -> Dict[str, Any]:
        """Retrieves or initializes runtime state for journey tracking."""
        if journey_id not in self._states:
            self._states[journey_id] = {
                "deviation_active": False,
                "outside_count": 0,
                "inside_count": 0,
                "last_gps_time": None,
                "last_lat": None,
                "last_lon": None,
                "early_departure_alerted": False,
                "delayed_start_alerted": False,
                "overdue_alerted": False,
                "arrival_candidates": 0,
            }
        return self._states[journey_id]

    def ingest_synced_point(
        self,
        journey: Dict[str, Any],
        latitude: float,
        longitude: float,
        timestamp: str,
        speed_kmh: float = 0.0,
    ) -> None:
        """
        Route-monitors a verified GPS point that reached this backend through the
        Firestore history sync instead of the live receiver.
        - Open journey that is the device's current one: full live evaluation
          (corridor deviation, rejoin, schedule and arrival alerts).
        - Closed journey (completed before the sync delivered the point): the point is
          recorded with its route distance so the travelled/deviation path can be drawn.
          If the fix is still current (<= GPS_FIX_MAX_AGE_SECONDS) the same two-reading
          corridor confirmation runs, so a deviation that happened during the journey is
          still alerted; older history never raises alarms.
        """
        journey_id = str(journey.get("journey_id") or "").strip()
        device_id = str(journey.get("device_id") or "").strip()
        if not journey_id or not device_id:
            return
        current = get_active_journey_for_device(device_id)
        if current and current.get("journey_id") == journey_id:
            self.evaluate_gps_position(
                device_id=device_id,
                latitude=latitude,
                longitude=longitude,
                timestamp=timestamp,
                speed_kmh=speed_kmh,
            )
            return

        selected_route = get_selected_route_for_journey(journey_id)
        route_coords = (selected_route or {}).get("geometry", {}).get("coordinates", []) if selected_route else []
        corridor = float(journey.get("route_corridor_meters") or DEFAULT_CORRIDOR_METERS)
        distance = (
            calculate_perpendicular_distance_to_route(latitude, longitude, route_coords)
            if route_coords else 0.0
        )
        record_journey_gps_point({
            "journey_id": journey_id,
            "device_id": device_id,
            "latitude": latitude,
            "longitude": longitude,
            "gps_timestamp": timestamp,
            "accuracy_meters": 0.0,
            "speed_kmh": speed_kmh,
            "distance_from_route_meters": distance,
            "on_route": (distance <= corridor) if route_coords else True,
        })

        point_dt = parse_utc_dt(timestamp)
        if not route_coords or point_dt is None:
            return
        if (datetime.now(timezone.utc) - point_dt).total_seconds() > GPS_FIX_MAX_AGE_SECONDS:
            return
        with _monitor_lock:
            state = self._get_or_create_state(journey_id)
            last_dt = parse_utc_dt(state.get("last_gps_time"))
            if last_dt and point_dt <= last_dt:
                return
            state["last_gps_time"] = timestamp
            self._apply_corridor_reading(
                state, journey_id, device_id, latitude, longitude, timestamp,
                distance <= corridor, distance, corridor, selected_route,
            )

    def _apply_corridor_reading(
        self,
        state: Dict[str, Any],
        journey_id: str,
        device_id: str,
        latitude: float,
        longitude: float,
        timestamp: str,
        on_route: bool,
        dist_from_route: float,
        corridor_threshold: float,
        selected_route: Dict[str, Any],
    ) -> None:
        """Two-consecutive-reading corridor confirmation shared by live and synced points."""
        if not on_route:
            state["outside_count"] += 1
            state["inside_count"] = 0
            if state["outside_count"] >= DEFAULT_CONFIRMATION_READINGS and not state["deviation_active"]:
                state["deviation_active"] = True
                self._confirm_deviation(
                    journey_id, device_id, latitude, longitude, timestamp,
                    dist_from_route, corridor_threshold, selected_route,
                )
        else:
            state["inside_count"] += 1
            state["outside_count"] = 0
            if state["deviation_active"] and state["inside_count"] >= DEFAULT_CONFIRMATION_READINGS:
                state["deviation_active"] = False
                self._confirm_rejoin(
                    journey_id, device_id, latitude, longitude, timestamp,
                    dist_from_route, selected_route,
                )

    @staticmethod
    def _confirm_deviation(
        journey_id: str,
        device_id: str,
        latitude: float,
        longitude: float,
        timestamp: str,
        dist_from_route: float,
        corridor_threshold: float,
        selected_route: Dict[str, Any],
    ) -> None:
        fix_dt = parse_utc_dt(timestamp)
        location_is_stale = fix_dt is None or (
            datetime.now(timezone.utc) - fix_dt
        ).total_seconds() > ALERT_CONFIG["gps_unavailable_seconds"]
        record_journey_event({
            "journey_id": journey_id,
            "device_id": device_id,
            "event_type": "ROUTE_DEVIATION_ALERT",
            "latitude": latitude,
            "longitude": longitude,
            "event_time": timestamp,
            "details": {
                "message": "Device deviated from the approved route",
                "distance_from_route_meters": dist_from_route,
                "corridor_meters": corridor_threshold,
                "selected_route_id": selected_route.get("id"),
                "route_name": selected_route.get("route_name"),
            }
        })
        alert_service.trigger_alert(
            device_id=device_id,
            alert_type="ROUTE_DEVIATION_ALERT",
            title="Route Deviation Alert",
            severity="high",
            message=f"Device {device_id} deviated from approved route '{selected_route.get('route_name')}'. Distance: {dist_from_route:.1f}m (Corridor: {corridor_threshold:.0f}m).",
            latitude=latitude,
            longitude=longitude,
            gps_valid=True,
            gps_timestamp=timestamp,
            source="ROUTE_MONITOR",
            raw_event_code="ROUTE_DEVIATION_ALERT",
            requires_alarm=True
        )
        ws_manager.broadcast({
            "type": "ROUTE_DEVIATION_ALERT",
            "alert_type": "ROUTE_DEVIATION_ALERT",
            "journey_id": journey_id,
            "device_id": device_id,
            "severity": "HIGH",
            "title": "Route Deviation Alert",
            "message": "Device deviated from the approved route",
            "distance_from_route_meters": dist_from_route,
            "corridor_meters": corridor_threshold,
            "latitude": latitude,
            "longitude": longitude,
            # The deviation position is the verified fix itself; staleness uses the same
            # threshold as the persisted alert's location snapshot.
            "gps_valid": True,
            "location_timestamp": timestamp,
            "location_is_stale": location_is_stale,
            "event_time": timestamp,
            "status": "ACTIVE",
            "requires_alarm": True,
            "timestamp": timestamp,
        })
        app_logger.warning(
            f"[ROUTE DEVIATION CONFIRMED] Device {device_id} Journey {journey_id} "
            f"distance={dist_from_route:.1f}m > corridor={corridor_threshold}m"
        )

    @staticmethod
    def _confirm_rejoin(
        journey_id: str,
        device_id: str,
        latitude: float,
        longitude: float,
        timestamp: str,
        dist_from_route: float,
        selected_route: Dict[str, Any],
    ) -> None:
        record_journey_event({
            "journey_id": journey_id,
            "device_id": device_id,
            "event_type": "ROUTE_REJOINED",
            "latitude": latitude,
            "longitude": longitude,
            "event_time": timestamp,
            "details": {
                "message": "Device returned to the approved route corridor",
                "distance_from_route_meters": dist_from_route,
                "selected_route_id": selected_route.get("id"),
            }
        })
        # Auto-resolve active deviation alert so audible sirens stop
        alert_service.resolve_active_alerts_by_type(
            device_id=device_id,
            alert_types=["ROUTE_DEVIATION_ALERT"],
            resolved_by="SYSTEM_ROUTE_REJOINED",
            notes="Device returned to approved route corridor"
        )
        ws_manager.broadcast({
            "type": "ROUTE_REJOINED",
            "journey_id": journey_id,
            "device_id": device_id,
            "severity": "INFO",
            "title": "Route Rejoined",
            "message": f"Device {device_id} returned to approved route corridor.",
            "distance_from_route_meters": dist_from_route,
            "latitude": latitude,
            "longitude": longitude,
            "requires_alarm": False,
            "timestamp": timestamp,
        })
        app_logger.info(
            f"[ROUTE REJOINED CONFIRMED] Device {device_id} Journey {journey_id} returned to corridor"
        )

    def reset_journey_state(self, journey_id: str) -> None:
        """Clears memory state for completed or cancelled journeys."""
        with _monitor_lock:
            self._states.pop(journey_id, None)

    def evaluate_gps_position(
        self,
        device_id: str,
        latitude: float,
        longitude: float,
        timestamp: str,
        accuracy_meters: float = 0.0,
        speed_kmh: float = 0.0,
    ) -> Optional[Dict[str, Any]]:
        """
        Main entry point called when a verified GPS fix arrives.
        Checks scheduled departures, approved route corridor deviation,
        overdue deadlines, and destination arrival.
        """
        device_id = str(device_id).strip()

        with _monitor_lock:
            # 1. Fetch active or scheduled journey for this device
            journey = get_active_journey_for_device(device_id)
            if not journey:
                return None

            journey_id = journey["journey_id"]
            state = self._get_or_create_state(journey_id)
            status = journey.get("status", "SCHEDULED").upper()
            now_iso = current_iso_timestamp()

            pt_dt = parse_utc_dt(timestamp)
            if not pt_dt:
                return None

            now_dt = datetime.now(timezone.utc)
            if (pt_dt - now_dt).total_seconds() > 15.0:
                gps_logger.warning(
                    f"[ROUTE MONITOR] Future GPS timestamp rejected device={device_id} ts={timestamp}"
                )
                return None

            # 2. Out-of-order and duplicate timestamp check
            last_dt = parse_utc_dt(state.get("last_gps_time"))
            if last_dt and pt_dt <= last_dt:
                gps_logger.debug(
                    f"[ROUTE MONITOR REPLAY] Device {device_id} point time {timestamp} <= {state['last_gps_time']}; skipping"
                )
                return None

            state["last_gps_time"] = timestamp
            state["last_lat"] = latitude
            state["last_lon"] = longitude

            # 3. Retrieve approved route geometry
            selected_route = get_selected_route_for_journey(journey_id)
            route_coords: List[List[float]] = []
            if selected_route and selected_route.get("geometry"):
                route_coords = selected_route["geometry"].get("coordinates", [])

            corridor_threshold = float(journey.get("route_corridor_meters") or DEFAULT_CORRIDOR_METERS)
            dist_from_route = 0.0
            if route_coords:
                dist_from_route = calculate_perpendicular_distance_to_route(latitude, longitude, route_coords)

            on_route = (dist_from_route <= corridor_threshold) if route_coords else True

            # Record point in journey_gps_points
            record_journey_gps_point({
                "journey_id": journey_id,
                "device_id": device_id,
                "latitude": latitude,
                "longitude": longitude,
                "gps_timestamp": timestamp,
                "accuracy_meters": accuracy_meters,
                "speed_kmh": speed_kmh,
                "distance_from_route_meters": dist_from_route,
                "on_route": on_route,
            })

            # =========================================================================
            # SCHEDULED JOURNEY EVALUATION
            # =========================================================================
            if status == "SCHEDULED":
                scheduled_dep_str = journey.get("scheduled_departure")
                scheduled_dep_dt = parse_utc_dt(scheduled_dep_str)

                start_lat = journey.get("start_latitude")
                start_lon = journey.get("start_longitude")

                # If origin coordinates not explicitly defined, infer from first coordinate of route
                if (start_lat is None or start_lon is None) and route_coords:
                    start_lon = route_coords[0][0]
                    start_lat = route_coords[0][1]

                if start_lat is not None and start_lon is not None:
                    dist_from_origin = calculate_haversine_distance_meters(
                        latitude, longitude, float(start_lat), float(start_lon)
                    )

                    # Scenario A: Device leaves origin before scheduled departure
                    if dist_from_origin > DEFAULT_ORIGIN_RADIUS_METERS:
                        if scheduled_dep_dt and pt_dt < scheduled_dep_dt:
                            if not state["early_departure_alerted"]:
                                state["early_departure_alerted"] = True
                                event_payload = {
                                    "journey_id": journey_id,
                                    "device_id": device_id,
                                    "event_type": "UNAUTHORIZED_EARLY_DEPARTURE",
                                    "latitude": latitude,
                                    "longitude": longitude,
                                    "event_time": timestamp,
                                    "details": {
                                        "message": f"Device {device_id} departed origin before scheduled departure ({scheduled_dep_str})",
                                        "distance_from_origin_meters": round(dist_from_origin, 1),
                                        "scheduled_departure": scheduled_dep_str,
                                        "departure_time": timestamp,
                                    }
                                }
                                record_journey_event(event_payload)
                                alert_service.trigger_alert(
                                    device_id=device_id,
                                    alert_type="UNAUTHORIZED_EARLY_DEPARTURE",
                                    title="Unauthorized Early Departure",
                                    severity="high",
                                    message=f"Device {device_id} moved outside permitted origin perimeter before scheduled time ({scheduled_dep_str}).",
                                    latitude=latitude,
                                    longitude=longitude,
                                    gps_valid=True,
                                    gps_timestamp=timestamp,
                                    source="ROUTE_MONITOR",
                                    raw_event_code="UNAUTHORIZED_EARLY_DEPARTURE",
                                    requires_alarm=True
                                )
                                ws_manager.broadcast({
                                    "type": "UNAUTHORIZED_EARLY_DEPARTURE",
                                    "journey_id": journey_id,
                                    "device_id": device_id,
                                    "severity": "HIGH",
                                    "title": "Unauthorized Early Departure",
                                    "message": f"Device {device_id} departed before authorized time.",
                                    "distance_from_origin_meters": round(dist_from_origin, 1),
                                    "requires_alarm": True,
                                    "timestamp": timestamp,
                                })

                        # If departed after scheduled time (or without scheduled time), transition to ACTIVE
                        elif scheduled_dep_dt and pt_dt >= scheduled_dep_dt:
                            update_journey_record(journey_id, {
                                "status": "ACTIVE",
                                "actual_departure": timestamp,
                                "start_time": timestamp,
                            })
                            ws_manager.broadcast({
                                "type": "JOURNEY_STARTED",
                                "event_type": "JOURNEY_STARTED",
                                "event_id": f"{journey_id}:JOURNEY_STARTED",
                                "journey_id": journey_id,
                                "device_id": device_id,
                                "severity": "INFO",
                                "title": "Journey Started",
                                "message": f"Device {device_id} departed the origin; journey {journey_id} is now active.",
                                "latitude": latitude,
                                "longitude": longitude,
                                "requires_alarm": False,
                                "timestamp": timestamp,
                            })
                            app_logger.info(f"[JOURNEY STARTED] Device {device_id} departed origin; status set to ACTIVE")

                # Scenario B: Scheduled departure time passes without departure
                if scheduled_dep_dt and pt_dt > scheduled_dep_dt:
                    # 5 minutes delay grace threshold
                    if (pt_dt - scheduled_dep_dt).total_seconds() > 300 and not state["delayed_start_alerted"]:
                        state["delayed_start_alerted"] = True
                        event_payload = {
                            "journey_id": journey_id,
                            "device_id": device_id,
                            "event_type": "JOURNEY_START_DELAYED",
                            "latitude": latitude,
                            "longitude": longitude,
                            "event_time": timestamp,
                            "details": {
                                "message": f"Journey {journey_id} has not started; scheduled departure ({scheduled_dep_str}) exceeded by > 5m",
                                "scheduled_departure": scheduled_dep_str,
                            }
                        }
                        record_journey_event(event_payload)
                        alert_service.trigger_alert(
                            device_id=device_id,
                            alert_type="JOURNEY_START_DELAYED",
                            title="Journey Start Delayed",
                            severity="medium",
                            message=f"Journey {journey_id} for custody device {device_id} was scheduled for {scheduled_dep_str} but has not departed.",
                            latitude=latitude,
                            longitude=longitude,
                            gps_valid=True,
                            gps_timestamp=timestamp,
                            source="ROUTE_MONITOR",
                            raw_event_code="JOURNEY_START_DELAYED"
                        )
                        ws_manager.broadcast({
                            "type": "JOURNEY_START_DELAYED",
                            "journey_id": journey_id,
                            "device_id": device_id,
                            "severity": "MEDIUM",
                            "title": "Journey Start Delayed",
                            "message": f"Journey {journey_id} has not started on time.",
                            "timestamp": timestamp,
                        })

            # =========================================================================
            # ACTIVE JOURNEY EVALUATION
            # =========================================================================
            elif status == "ACTIVE":
                # Scenario C: Check Journey Overdue
                scheduled_arr_str = journey.get("scheduled_arrival")
                scheduled_arr_dt = parse_utc_dt(scheduled_arr_str)
                if scheduled_arr_dt and pt_dt > scheduled_arr_dt and not state["overdue_alerted"]:
                    state["overdue_alerted"] = True
                    event_payload = {
                        "journey_id": journey_id,
                        "device_id": device_id,
                        "event_type": "JOURNEY_OVERDUE",
                        "latitude": latitude,
                        "longitude": longitude,
                        "event_time": timestamp,
                        "details": {
                            "message": f"Journey {journey_id} has exceeded scheduled arrival time ({scheduled_arr_str})",
                            "scheduled_arrival": scheduled_arr_str,
                        }
                    }
                    record_journey_event(event_payload)
                    alert_service.trigger_alert(
                        device_id=device_id,
                        alert_type="JOURNEY_OVERDUE",
                        title="Journey Overdue",
                        severity="high",
                        message=f"Custody device {device_id} has not arrived by expected deadline ({scheduled_arr_str}).",
                        latitude=latitude,
                        longitude=longitude,
                        gps_valid=True,
                        gps_timestamp=timestamp,
                        source="ROUTE_MONITOR",
                        raw_event_code="JOURNEY_OVERDUE",
                        requires_alarm=True
                    )
                    ws_manager.broadcast({
                        "type": "JOURNEY_OVERDUE",
                        "journey_id": journey_id,
                        "device_id": device_id,
                        "severity": "HIGH",
                        "title": "Journey Overdue",
                        "message": f"Journey {journey_id} is overdue.",
                        "requires_alarm": True,
                        "timestamp": timestamp,
                    })

                # Scenario D: Destination Arrival Check
                end_lat = journey.get("end_latitude")
                end_lon = journey.get("end_longitude")
                if (end_lat is None or end_lon is None) and route_coords:
                    end_lon = route_coords[-1][0]
                    end_lat = route_coords[-1][1]

                if end_lat is not None and end_lon is not None:
                    dist_to_destination = calculate_haversine_distance_meters(
                        latitude, longitude, float(end_lat), float(end_lon)
                    )
                    if dist_to_destination <= DEFAULT_DESTINATION_RADIUS_METERS:
                        state["arrival_candidates"] += 1
                        if state["arrival_candidates"] >= DEFAULT_CONFIRMATION_READINGS:
                            # Confirmed arrival at destination
                            update_journey_record(journey_id, {
                                "status": "COMPLETED",
                                "actual_arrival": timestamp,
                                "end_time": timestamp,
                            })
                            event_payload = {
                                "journey_id": journey_id,
                                "device_id": device_id,
                                "event_type": "DESTINATION_ARRIVAL",
                                "latitude": latitude,
                                "longitude": longitude,
                                "event_time": timestamp,
                                "details": {
                                    "message": f"Device {device_id} confirmed arrival at destination ({journey.get('destination_name') or 'Destination'})",
                                    "actual_arrival": timestamp,
                                }
                            }
                            record_journey_event(event_payload)
                            ws_manager.broadcast({
                                "type": "DESTINATION_ARRIVAL",
                                "event_type": "DESTINATION_ARRIVAL",
                                "event_id": f"{journey_id}:JOURNEY_COMPLETED",
                                "journey_id": journey_id,
                                "device_id": device_id,
                                "severity": "INFO",
                                "title": "Destination Arrival",
                                "message": f"Custody transport arrived at destination: {journey.get('destination_name') or 'Destination'}",
                                "timestamp": timestamp,
                                "status": "COMPLETED"
                            })
                            app_logger.info(f"[DESTINATION ARRIVAL] Journey {journey_id} completed successfully")
                            return {
                                "journey_id": journey_id,
                                "status": "COMPLETED",
                                "event": "DESTINATION_ARRIVAL"
                            }
                    else:
                        state["arrival_candidates"] = 0

                # =====================================================================
                # Selected Route Corridor Deviation Check
                # =====================================================================
                if route_coords:
                    self._apply_corridor_reading(
                        state, journey_id, device_id, latitude, longitude, timestamp,
                        on_route, dist_from_route, corridor_threshold, selected_route,
                    )

            # Broadcast live position update for monitoring dashboard
            live_payload = {
                "type": "JOURNEY_LIVE_UPDATE",
                "device_id": device_id,
                "journey_id": journey_id,
                "latitude": latitude,
                "longitude": longitude,
                "timestamp": timestamp,
                "gps_valid": True,
                "location_is_current": True,
                "speed_kmh": speed_kmh,
                "on_selected_route": on_route,
                "distance_from_route_meters": dist_from_route,
                "deviation_active": state["deviation_active"]
            }
            ws_manager.broadcast(live_payload)

            return live_payload

    def handle_device_gps_loss(self, device_id: str, reason: str = "SIGNAL_LOSS") -> None:
        """
        Called when GPS fix is lost for a device.
        Does NOT generate false route deviation.
        Broadcasts and logs GPS signal loss event.
        """
        device_id = str(device_id).strip()
        with _monitor_lock:
            journey = get_active_journey_for_device(device_id)
            if not journey or journey.get("status") not in ("ACTIVE", "SCHEDULED"):
                return

            journey_id = journey["journey_id"]
            now_iso = current_iso_timestamp()

            event_payload = {
                "journey_id": journey_id,
                "device_id": device_id,
                "event_type": "GPS_SIGNAL_LOSS",
                "latitude": None,
                "longitude": None,
                "event_time": now_iso,
                "details": {
                    "reason": reason,
                    "message": f"GPS signal lost for device {device_id} during journey {journey_id} ({reason})"
                }
            }
            record_journey_event(event_payload)

            ws_manager.broadcast({
                "type": "GPS_SIGNAL_LOSS",
                "journey_id": journey_id,
                "device_id": device_id,
                "severity": "MEDIUM",
                "title": "GPS Signal Loss",
                "message": f"GPS fix lost for device {device_id}.",
                "timestamp": now_iso,
            })
            app_logger.info(f"[ROUTE MONITOR] GPS loss recorded for {device_id}: {reason}")


route_monitor = RouteMonitor()
