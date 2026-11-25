"""
Device Management Service
Maintains device statuses, battery life, step counts, pulse, and registration.
Strictly uses actual telemetry readings without hardcoded fallback values.
"""

from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from ..utils.datetime_utils import current_iso_timestamp
from ..utils.logger import app_logger
from .firestore_service import (
    GPS_FIX_MAX_AGE_SECONDS,
    firestore_service,
    is_newer_timestamp,
    normalize_verified_gps_fix,
    normalize_iso_timestamp,
    parse_iso_timestamp,
    valid_coordinate_pair
)


class DeviceService:
    def register_device(self, device_data: Dict[str, Any]) -> Dict[str, Any]:
        """Registers a custody band profile if not already present, preserving existing fields."""
        device_id = device_data["device_id"]
        existing = firestore_service.get_device(device_id)

        if existing:
            # Preserve existing device registration information and status
            merged = {**existing}
            for k, v in device_data.items():
                if v is not None and (k not in merged or merged[k] is None):
                    merged[k] = v
            firestore_service.save_device(merged)
            return firestore_service.get_device(device_id) or merged

        device_data.setdefault("status", "ACTIVE")
        device_data.setdefault("last_seen", current_iso_timestamp())

        firestore_service.save_device(device_data)
        return firestore_service.get_device(device_id) or device_data

    def update_heartbeat(
        self,
        device_id: str,
        battery: Optional[int],
        steps: Optional[int],
        is_charging: bool = False,
        received_at: Optional[str] = None
    ):
        """Updates battery and steps from KA (Keepalive) / LK packets."""
        existing = firestore_service.get_local_device(device_id) or {}
        now_iso = normalize_iso_timestamp(received_at) or current_iso_timestamp()

        # Preserve existing valid coordinates and GPS status
        last_lat = existing.get("last_latitude")
        last_lon = existing.get("last_longitude")
        last_gps_time = existing.get("last_valid_gps_time")
        gps_valid = bool(existing.get("gps_valid", False))

        # Check for location staleness
        gps_status = existing.get("gps_status", "No Fix")
        if gps_valid and last_gps_time:
            dt_fix = parse_iso_timestamp(last_gps_time)
            now_dt = parse_iso_timestamp(now_iso)
            if (
                dt_fix is not None
                and now_dt is not None
                and (now_dt - dt_fix).total_seconds() > GPS_FIX_MAX_AGE_SECONDS
            ):
                gps_status = "Location Stale"
                gps_valid = False

        # Actual battery - preserve existing if current is None
        dev_battery = battery if battery is not None else existing.get("battery")
        dev_steps = steps if steps is not None else existing.get("steps")

        update_data: Dict[str, Any] = {
            "device_id": device_id,
            "last_seen": now_iso,
            "status": "ONLINE",
            "is_charging": is_charging,
            "charging": is_charging,
            "charge_status": "charging" if is_charging else "discharging",
            "gps_valid": gps_valid,
            "gps_fix": gps_valid,
            "gps_locked": gps_valid,
            "gps_status": gps_status,
        }

        if last_lat is not None and last_lon is not None:
            update_data["last_latitude"] = last_lat
            update_data["last_longitude"] = last_lon
            update_data["maps_url"] = f"https://maps.google.com/?q={last_lat:.6f},{last_lon:.6f}"
        if last_gps_time:
            update_data["last_valid_gps_time"] = last_gps_time

        if dev_battery is not None:
            update_data["battery"] = dev_battery
            update_data["battery_level"] = dev_battery
            update_data["battery_percent"] = dev_battery
            update_data["battery_status"] = "normal" if dev_battery > 20 else "low"

        if dev_steps is not None:
            update_data["steps"] = dev_steps

        if not firestore_service.save_device(update_data, sync_firestore=False):
            app_logger.error(
                "[DEVICE STATUS] %s heartbeat persistence failed",
                device_id
            )
        batt_str = f"{dev_battery}%" if dev_battery is not None else "N/A"
        app_logger.info(f"[DEVICE STATUS] {device_id} Heartbeat: Battery={batt_str}, Steps={dev_steps}, Charging={is_charging}")

    def update_heart_rate(
        self, device_id: str, bpm: int, received_at: Optional[str] = None
    ):
        """Updates heart rate pulse record."""
        update_data = {
            "device_id": device_id,
            "last_seen": normalize_iso_timestamp(received_at) or current_iso_timestamp(),
            "heart_rate": bpm,
            "status": "ONLINE"
        }
        if not firestore_service.save_device(update_data, sync_firestore=False):
            app_logger.error(
                "[HEALTH] %s heart-rate persistence failed",
                device_id
            )
        app_logger.info(f"[HEALTH] {device_id} Heart Rate: {bpm} BPM")

    def update_position(
        self,
        device_id: str,
        lat: Optional[float] = None,
        lon: Optional[float] = None,
        battery: Optional[int] = None,
        steps: Optional[int] = None,
        speed: float = 0.0,
        bearing: float = 0.0,
        is_charging: Optional[bool] = None,
        gps_valid: bool = False,
        satellites: int = 0,
        gsm_signal: int = 0,
        timestamp: Optional[str] = None,
        gps_source: Optional[str] = None,
        received_at: Optional[str] = None
    ):
        """
        Updates device live position and telemetry.
        CRITICAL: If gps_valid is False (status 'V'), preserves previous valid coordinates
        while updating GPS status to 'No Fix' or 'Location Stale'.
        """
        existing = firestore_service.get_local_device(device_id) or {}
        packet_received_at = normalize_iso_timestamp(received_at) or current_iso_timestamp()
        gps_time = normalize_iso_timestamp(timestamp)
        last_lat = existing.get("last_latitude")
        last_lon = existing.get("last_longitude")
        last_gps_time = normalize_iso_timestamp(existing.get("last_valid_gps_time"))
        verified_fix = normalize_verified_gps_fix(
            gps_valid,
            lat,
            lon,
            gps_time,
            gps_source,
            max_age_seconds=None
        )
        current_fix = normalize_verified_gps_fix(
            gps_valid,
            lat,
            lon,
            gps_time,
            gps_source
        )
        is_current = current_fix is not None
        position = verified_fix[:2] if verified_fix else None
        if verified_fix is not None:
            gps_time = verified_fix[2]
        new_valid_fix = (
            verified_fix is not None
            and is_newer_timestamp(gps_time, last_gps_time)
        )

        if new_valid_fix and position is not None:
            last_lat, last_lon = position
            last_gps_time = gps_time
            gps_status = "GPS Locked" if is_current else "Location Stale"
            is_fix = is_current
        else:
            last_fix = parse_iso_timestamp(last_gps_time)
            is_stale = (
                last_fix is not None
                and (datetime.now(timezone.utc) - last_fix).total_seconds() > GPS_FIX_MAX_AGE_SECONDS
            )
            incoming_fix_time = parse_iso_timestamp(gps_time)
            incoming_is_stale = (
                incoming_fix_time is not None
                and (datetime.now(timezone.utc) - incoming_fix_time).total_seconds() > GPS_FIX_MAX_AGE_SECONDS
            )
            previous_fix_is_newer = (
                last_fix is not None
                and parse_iso_timestamp(gps_time) is not None
                and parse_iso_timestamp(gps_time) < last_fix
            )
            duplicate_fix = (
                verified_fix is not None
                and parse_iso_timestamp(gps_time) == last_fix
                and position == valid_coordinate_pair(last_lat, last_lon)
            )

            if duplicate_fix:
                is_fix = bool(existing.get("gps_valid")) and not is_stale
                gps_status = (
                    "Location Stale" if is_stale
                    else (existing.get("gps_status") or ("GPS Locked" if is_fix else "No Fix"))
                )
            elif previous_fix_is_newer:
                # Delayed packets must not change the state of a newer accepted fix.
                prior_status = existing.get("gps_status")
                is_fix = bool(existing.get("gps_valid")) and not is_stale
                gps_status = (
                    "Location Stale" if is_stale
                    else ("GPS Locked" if is_fix else prior_status or "No Fix")
                )
            else:
                is_fix = False
                gps_status = (
                    "Location Stale" if is_stale or incoming_is_stale else "No Fix"
                )

        # Resolve battery: actual reading if given, else preserve existing; NEVER default to 90
        dev_battery = battery if battery is not None else existing.get("battery")
        dev_steps = steps if steps is not None else existing.get("steps")
        dev_chg = is_charging if is_charging is not None else bool(existing.get("is_charging") or existing.get("charging"))

        # Travel angle
        heading_val = float(bearing or 0.0)
        speed_val = float(speed or 0.0)

        update_data: Dict[str, Any] = {
            "device_id": device_id,
            "last_seen": packet_received_at,
            "status": "ONLINE",
            "gps_valid": is_fix,
            "gps_fix": is_fix,
            "gps_locked": is_fix,
            "gps_status": gps_status,
            "speed": speed_val,
            "speed_kmh": speed_val,
            "heading": heading_val,
            "bearing": heading_val,
            "course": heading_val,
            "direction": heading_val,
            "direction_deg": heading_val,
            "angle": heading_val,
            "rotation": heading_val,
            "satellites": satellites,
            "gsm_signal": gsm_signal,
            "is_charging": dev_chg,
            "charging": dev_chg,
            "charge_status": "charging" if dev_chg else "discharging",
        }

        # Save coordinates (new fix or preserved previous fix)
        if last_lat is not None and last_lon is not None:
            update_data["last_latitude"] = last_lat
            update_data["last_longitude"] = last_lon
            update_data["maps_url"] = f"https://maps.google.com/?q={last_lat:.6f},{last_lon:.6f}"
        if last_gps_time:
            update_data["last_valid_gps_time"] = last_gps_time
        if new_valid_fix and verified_fix is not None:
            update_data["gps_source"] = verified_fix[3]
        elif existing.get("gps_source"):
            update_data["gps_source"] = existing.get("gps_source")

        if dev_battery is not None:
            update_data["battery"] = dev_battery
            update_data["battery_level"] = dev_battery
            update_data["battery_percent"] = dev_battery
            update_data["battery_status"] = "normal" if dev_battery > 20 else "low"

        if dev_steps is not None:
            update_data["steps"] = dev_steps

        persisted = firestore_service.save_device(update_data, sync_firestore=False)
        if not persisted:
            app_logger.error(
                "[DEVICE STATUS] %s GPS update persistence failed",
                device_id
            )
        return persisted

    def get_all_devices(self) -> List[Dict[str, Any]]:
        return [
            self._add_connection_status(device)
            for device in firestore_service.get_devices()
        ]

    def get_device_by_id(self, device_id: str) -> Optional[Dict[str, Any]]:
        device = firestore_service.get_device(device_id)
        return self._add_connection_status(device) if device else None

    def get_device(self, device_id: str) -> Optional[Dict[str, Any]]:
        return self.get_device_by_id(device_id)

    @staticmethod
    def _add_connection_status(device: Dict[str, Any]) -> Dict[str, Any]:
        from ..receiver.tcp_server import is_device_connected

        result = dict(device)
        device_id = str(result.get("device_id") or result.get("id") or "")
        connected = is_device_connected(device_id) if device_id else False
        result["tcp_connected"] = connected
        result["connection_status"] = "CONNECTED" if connected else "DISCONNECTED"
        result["last_packet_received"] = result.get("last_seen")
        if connected:
            result["online"] = True
            result["status"] = "ONLINE"
        return result


device_service = DeviceService()
