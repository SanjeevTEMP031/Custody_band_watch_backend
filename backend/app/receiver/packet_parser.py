"""
Packet Parser for Huaten C05 GPS Custody Watch
Decodes command payloads: PING, KA, UD (GPS), CRUD (Polled GPS), heart, and AL (Alarms).
"""

import os
import re
import traceback
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Tuple
from ..utils.logger import tcp_logger, gps_logger
from ..services.gps_service import gps_service
from ..services.device_service import device_service
from ..services.firestore_service import normalize_verified_gps_fix, valid_coordinate_pair
from ..services.alert_service import alert_service

REPLIES = {
    "PING": "PING,1",
    "KA": "KA",
    "LK": "LK",
    "HEART": "heart",
    "AL": "AL",
}
_DIAGNOSTICS_ENABLED = os.getenv("GPS_RECEIVER_DIAGNOSTICS", "").strip().lower() in {
    "1", "true", "yes", "on"
}


def _log_gps_diagnostic(message: str, *args):
    if _DIAGNOSTICS_ENABLED:
        gps_logger.info(message, *args)


def build_frame(maker: str, dev_id: str, content: str) -> bytes:
    """Builds a compliant [MAKER*ID*HEX_LEN*CONTENT] frame."""
    body = content.encode("latin-1")
    return b"[%s*%s*%04X*%s]" % (maker.encode("latin-1"), dev_id.encode("latin-1"), len(body), body)


def _parse_watch_gps_timestamp(date_value: str, time_value: str) -> Optional[str]:
    if not re.fullmatch(r"\d{6}", date_value) or not re.fullmatch(r"\d{6}", time_value):
        return None
    try:
        timestamp = datetime.strptime(
            f"{date_value}{time_value}", "%y%m%d%H%M%S"
        ).replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return timestamp.isoformat()


def _received_at_iso(received_at: Optional[float]) -> str:
    if received_at is None:
        return datetime.now(timezone.utc).isoformat()
    return datetime.fromtimestamp(received_at, tz=timezone.utc).isoformat()


def parse_ud(
    content: bytes,
    device_id: str,
    maker: str,
    gps_source: Optional[str] = None,
    received_at: Optional[float] = None
) -> Optional[Dict[str, Any]]:
    """
    Parses Huaten UD / CRUD / AL packets:
    fields[0] : Command (UD / CRUD / AL)
    fields[1] : Date YYMMDD (e.g. 261102)
    fields[2] : UTC Time HHMMSS (e.g. 092137)
    fields[3] : Status (A = Valid GPS fix, V = Void / Indoor LBS)
    fields[4] : Latitude (e.g. 17.446317)
    fields[5] : Lat Hem (N/S)
    fields[6] : Longitude (e.g. 78.368395)
    fields[7] : Lon Hem (E/W)
    fields[8] : Speed km/h
    fields[9] : Course (Heading)
    fields[10]: Altitude
    fields[11]: Satellites
    fields[12]: GSM Signal
    fields[13]: Battery %
    fields[14]: Step Count
    fields[15]: Terminal Status (Hex)
    """
    try:
        msg = content.decode("latin-1", errors="ignore")
        fields = msg.split(",")
        if len(fields) < 15:
            _log_gps_diagnostic(
                "GPS packet rejected device=%s command=%s reason=insufficient_fields field_count=%d",
                device_id,
                fields[0].strip() if fields else "UNKNOWN",
                len(fields)
            )
            return None

        cmd = fields[0].strip()
        date_str = fields[1].strip()
        time_str = fields[2].strip()
        status = fields[3].strip().upper()
        gps_timestamp = _parse_watch_gps_timestamp(date_str, time_str)

        # Coordinate parsing and strict validation
        try:
            lat = float(fields[4].strip()) if fields[4].strip() else 0.0
        except (ValueError, TypeError):
            lat = 0.0
        if len(fields) > 5 and fields[5].strip().upper() == "S":
            lat = -abs(lat)

        try:
            lon = float(fields[6].strip()) if fields[6].strip() else 0.0
        except (ValueError, TypeError):
            lon = 0.0
        if len(fields) > 7 and fields[7].strip().upper() == "W":
            lon = -abs(lon)

        is_valid_coords = valid_coordinate_pair(lat, lon) is not None
        verified_fix = normalize_verified_gps_fix(
            status == "A",
            lat,
            lon,
            gps_timestamp,
            gps_source,
            max_age_seconds=None
        )
        is_valid_gps = verified_fix is not None
        current_fix = normalize_verified_gps_fix(
            status == "A",
            lat,
            lon,
            gps_timestamp,
            gps_source
        )
        if verified_fix is not None:
            lat, lon, gps_timestamp, gps_source = verified_fix
        is_current_gps = current_fix is not None
        server_received_at = _received_at_iso(received_at)
        _log_gps_diagnostic(
            "GPS packet device=%s command=%s status=%s coordinates_valid=%s timestamp_valid=%s",
            device_id,
            cmd,
            status,
            is_valid_coords,
            gps_timestamp is not None
        )
        gps_logger.info(
            "GPS packet device=%s command=%s status=%s coordinates_valid=%s "
            "verified=%s current=%s",
            device_id,
            cmd,
            status,
            is_valid_coords,
            is_valid_gps,
            is_current_gps
        )

        try:
            speed = float(fields[8].strip()) if len(fields) > 8 and fields[8].strip() else 0.0
        except (ValueError, TypeError):
            speed = 0.0

        try:
            course = float(fields[9].strip()) if len(fields) > 9 and fields[9].strip() else 0.0
        except (ValueError, TypeError):
            course = 0.0

        satellites = int(fields[11].strip()) if len(fields) > 11 and fields[11].strip().isdigit() else 0
        gsm_signal = int(fields[12].strip()) if len(fields) > 12 and fields[12].strip().isdigit() else 0

        # Actual device battery - NEVER default to 90 or 100
        battery = int(fields[13].strip()) if len(fields) > 13 and fields[13].strip().isdigit() else None
        steps = int(fields[14].strip()) if len(fields) > 14 and fields[14].strip().isdigit() else None

        # Field 15: Terminal status (Hex 8 digits)
        is_charging = False
        term_hex = None
        status_int = 0
        if len(fields) > 15:
            try:
                term_hex = fields[15].strip()
                status_int = int(term_hex, 16) if term_hex else 0
                is_charging = bool(status_int & 0x10 or status_int & 0x08)
            except Exception:
                pass

        gps_status = (
            "GPS Locked" if is_current_gps
            else ("Location Stale" if is_valid_gps else "No Fix")
        )

        record: Dict[str, Any] = {
            "device_id": device_id,
            "maker": maker,
            "command": cmd,
            "timestamp": gps_timestamp,
            "received_at": server_received_at,
            "watch_date": date_str,
            "watch_time_utc": time_str,
            "gps_valid": is_valid_gps,
            "gps_fix": is_valid_gps,
            "gps_locked": is_valid_gps,
            "gps_current": is_current_gps,
            "gps_status": gps_status,
            "gps_source": gps_source if is_valid_gps else None,
            "latitude": round(lat, 6) if is_valid_gps else None,
            "longitude": round(lon, 6) if is_valid_gps else None,
            "speed_kmh": speed,
            "speed": speed,
            "heading": course,
            "bearing": course,
            "course": course,
            "direction": course,
            "direction_deg": course,
            "satellites": satellites,
            "gsm_signal": gsm_signal,
            "is_charging": is_charging,
            "charging": is_charging,
            "charge_status": "charging" if is_charging else "discharging",
        }

        if battery is not None:
            record["battery"] = battery
            record["battery_level"] = battery
            record["battery_percent"] = battery
            record["battery_status"] = "normal" if battery > 20 else "low"

        if steps is not None:
            record["steps"] = steps

        # Check nearby Wi-Fi APs
        if len(fields) > 24 and fields[24].strip().isdigit():
            record["nearby_wifi_count"] = int(fields[24].strip())

        # Process through GPS service and Device service
        processed_record = gps_service.process_location_record(record)
        gps_processing_succeeded = processed_record is record
        device_service.update_position(
            device_id=device_id,
            lat=record.get("latitude"),
            lon=record.get("longitude"),
            battery=battery,
            steps=steps,
            speed=speed,
            bearing=course,
            is_charging=is_charging,
            gps_valid=bool(record.get("gps_valid")),
            satellites=satellites,
            gsm_signal=gsm_signal,
            timestamp=record.get("timestamp"),
            gps_source=record.get("gps_source"),
            received_at=server_received_at
        )
        device_position_update_succeeded = True
        _log_gps_diagnostic(
            "GPS processing device=%s command=%s status=%s coordinates_valid=%s "
            "gps_processing_succeeded=%s device_position_update_succeeded=%s",
            device_id,
            cmd,
            status,
            is_valid_coords,
            gps_processing_succeeded,
            device_position_update_succeeded
        )

        if battery is not None:
            alert_service.check_battery_alert(device_id, battery)

        # Communication received: auto-clear offline and communication lost alerts
        alert_service.resolve_active_alerts_by_type(
            device_id=device_id,
            alert_types=["WATCH_OFFLINE", "DEVICE_COMMUNICATION_LOST", "NETWORK_DISCONNECTED"],
            resolved_by="SYSTEM_TELEMETRY_RECEIVED",
            notes="Active communication restored"
        )

        if is_valid_gps:
            alert_service.resolve_active_alerts_by_type(
                device_id=device_id,
                alert_types=["GPS_UNAVAILABLE"],
                resolved_by="SYSTEM_GPS_RESTORED",
                notes="Valid GPS fix received"
            )

        # Check alarm conditions (Never assume AL always means SOS)
        if cmd == "AL":
            detected_alert_type = None
            detected_message = None

            if status_int & 0x01:
                detected_alert_type = "SOS_ALERT"
                detected_message = "Emergency SOS button triggered by custody subject"
            elif status_int & 0x40:
                detected_alert_type = "STRAP_CUT_ALERT"
                detected_message = "Verified strap-cut sensor event detected on watch band"
            elif status_int & 0x2000:
                detected_alert_type = "TAMPER_ALERT"
                detected_message = "Verified hardware tampering event detected"
            elif status_int & 0x80:
                detected_alert_type = "BRACELET_REMOVED"
                detected_message = "Verified removal event: Watch removed from subject's wrist"
            elif status_int & 0x1000:
                detected_alert_type = "BRACELET_DISCONNECTED"
                detected_message = "Bracelet link disconnected event"
            elif status_int & 0x20:
                detected_alert_type = "STRAP_OPENED"
                detected_message = "Verified strap-open sensor event"
            elif status_int & 0x100:
                detected_alert_type = "FALL_ALERT"
                detected_message = "Verified fall-detection event reported by firmware"
            elif status_int & 0x200:
                detected_alert_type = "DEVICE_POWERED_OFF"
                detected_message = "Explicit device power-off event reported"
            elif status_int & 0x400:
                detected_alert_type = "WATCH_INACTIVE"
                detected_message = "Subject prolonged inactivity sensor event"

            # Check if any later field has an explicit alarm sub-code or text
            if not detected_alert_type:
                for f_idx in range(16, min(len(fields), 22)):
                    f_val = fields[f_idx].strip().upper()
                    if f_val in ("SOS", "SOS_ALERT"):
                        detected_alert_type = "SOS_ALERT"
                        detected_message = "SOS Alarm triggered by wearer"
                        break
                    elif f_val in ("CUT", "STRAP_CUT", "STRAP_CUT_ALERT"):
                        detected_alert_type = "STRAP_CUT_ALERT"
                        detected_message = "Strap cut sensor event detected"
                        break
                    elif f_val in ("REMOVE", "REMOVED", "BRACELET_REMOVED"):
                        detected_alert_type = "BRACELET_REMOVED"
                        detected_message = "Bracelet removed event detected"
                        break
                    elif f_val in ("TAMPER", "TAMPER_ALERT"):
                        detected_alert_type = "TAMPER_ALERT"
                        detected_message = "Tamper event reported"
                        break
                    elif f_val in ("OPEN", "STRAP_OPENED"):
                        detected_alert_type = "STRAP_OPENED"
                        detected_message = "Strap open event reported"
                        break
                    elif f_val in ("FALL", "FALL_ALERT"):
                        detected_alert_type = "FALL_ALERT"
                        detected_message = "Fall event detected"
                        break

            # If still unspecified: Do NOT assume it is SOS! Create TAMPER_ALERT
            if not detected_alert_type:
                detected_alert_type = "TAMPER_ALERT"
                detected_message = f"Watch reported alarm frame (status hex: {term_hex or 'UNKNOWN'})"

            alert_service.trigger_alert(
                device_id=device_id,
                alert_type=detected_alert_type,
                message=detected_message,
                latitude=record.get("latitude"),
                longitude=record.get("longitude"),
                gps_valid=bool(record.get("gps_valid")),
                gps_timestamp=record.get("timestamp"),
                event_time=server_received_at,
                source="C05_DEVICE",
                raw_event_code=term_hex or cmd
            )

        return record

    except Exception as exc:
        tcp_logger.error(
            "Error parsing GPS packet device=%s exception_type=%s stack=%s",
            device_id,
            type(exc).__name__,
            "".join(traceback.format_tb(exc.__traceback__))
        )
        return None


def parse_packet(
    maker: str,
    dev_id: str,
    content: bytes,
    gps_source: Optional[str] = None,
    received_at: Optional[float] = None
) -> Tuple[Optional[str], Optional[bytes]]:
    """
    Decodes packet content, dispatches business logic, and returns (cmd_type, reply_frame_bytes).
    """
    msg = content.decode("latin-1", errors="ignore")
    cmd = msg.split(",")[0].strip()
    cmd_key = cmd.upper()
    received_at_value = _received_at_iso(received_at)
    reply_bytes = None

    tcp_logger.info("[%s*%s] Recv command=%s", maker, dev_id, cmd or "UNKNOWN")

    # Build acknowledgment reply if required by protocol
    if cmd_key in REPLIES:
        reply_body = REPLIES[cmd_key]
        reply_bytes = build_frame(maker, dev_id, reply_body)

    # 1. Location / Telemetry
    if cmd_key in ("UD", "CRUD", "AL"):
        parse_ud(
            content,
            dev_id,
            maker,
            gps_source=gps_source,
            received_at=received_at
        )

    # 2. Keepalive (KA)
    elif cmd_key == "KA":
        parts = msg.split(",")
        bat = None
        steps = None
        is_charging = False
        # Standard Huaten KA: KA,steps,rolls,battery[,charging]
        if len(parts) >= 4 and parts[3].strip().isdigit():
            bat = int(parts[3].strip())
            if len(parts) > 1 and parts[1].strip().isdigit():
                steps = int(parts[1].strip())
            if len(parts) > 4 and parts[4].strip() in ("1", "true"):
                is_charging = True
        elif len(parts) >= 5 and parts[4].strip().isdigit():
            bat = int(parts[4].strip())
            if len(parts) > 2 and parts[2].strip().isdigit():
                steps = int(parts[2].strip())
            if len(parts) > 5 and parts[5].strip() in ("1", "true"):
                is_charging = True
        else:
            for p in parts[1:]:
                p_c = p.strip()
                if p_c.isdigit():
                    v = int(p_c)
                    if 1 <= v <= 100 and bat is None:
                        bat = v
                    elif v > 100 and steps is None:
                        steps = v

        device_service.update_heartbeat(
            dev_id, bat, steps, is_charging=is_charging, received_at=received_at_value
        )
        if bat is not None:
            alert_service.check_battery_alert(dev_id, bat)

        alert_service.resolve_active_alerts_by_type(
            device_id=dev_id,
            alert_types=["WATCH_OFFLINE", "DEVICE_COMMUNICATION_LOST", "NETWORK_DISCONNECTED"],
            resolved_by="SYSTEM_KEEPALIVE_RECEIVED",
            notes="Watch keepalive received"
        )

    # 3. Heart Rate
    elif cmd_key == "HEART":
        parts = msg.split(",")
        bpm = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
        device_service.update_heart_rate(dev_id, bpm, received_at=received_at_value)

    # 4. Link & Status Heartbeat (LK)
    elif cmd_key == "LK":
        parts = msg.split(",")
        bat = None
        steps = None
        is_charging = False
        # Huaten LK frame formats: LK,steps,rolls,battery or LK,battery
        if len(parts) >= 4 and parts[3].strip().isdigit():
            bat = int(parts[3].strip())
            if len(parts) > 1 and parts[1].strip().isdigit():
                steps = int(parts[1].strip())
            if len(parts) > 4 and parts[4].strip() in ("1", "true"):
                is_charging = True
        else:
            for p in parts[1:]:
                p_clean = p.strip()
                if p_clean.isdigit():
                    val = int(p_clean)
                    if 1 <= val <= 100 and bat is None:
                        bat = val
                    elif val > 100 and steps is None:
                        steps = val
            if len(parts) > 4 and parts[4].strip() in ("1", "true"):
                is_charging = True

        device_service.update_heartbeat(
            dev_id, bat, steps, is_charging=is_charging, received_at=received_at_value
        )
        if bat is not None:
            alert_service.check_battery_alert(dev_id, bat)

        alert_service.resolve_active_alerts_by_type(
            device_id=dev_id,
            alert_types=["WATCH_OFFLINE", "DEVICE_COMMUNICATION_LOST", "NETWORK_DISCONNECTED"],
            resolved_by="SYSTEM_LINK_RECEIVED",
            notes="Watch link status received"
        )

    # 5. Bracelet Connection & Tamper Packets
    elif cmd_key in ("BRACELET", "BT", "BLE"):
        parts = msg.split(",")
        state = parts[1].strip().upper() if len(parts) > 1 else ""
        if state in ("1", "CONNECTED", "CONNECT", "ON"):
            alert_service.trigger_alert(
                device_id=dev_id,
                alert_type="BRACELET_CONNECTED",
                severity="INFO",
                message="Bracelet link connection established / restored",
                event_time=received_at_value,
                source="C05_DEVICE"
            )
            alert_service.resolve_active_alerts_by_type(
                device_id=dev_id,
                alert_types=["BRACELET_DISCONNECTED"],
                resolved_by="SYSTEM_BRACELET_CONNECTED",
                notes="Bracelet link restored"
            )
        else:
            # Do not assume physically removed or cut
            alert_service.trigger_alert(
                device_id=dev_id,
                alert_type="BRACELET_DISCONNECTED",
                severity="HIGH",
                message="Bracelet link disconnected event",
                event_time=received_at_value,
                source="C05_DEVICE"
            )

    elif cmd_key in ("STRAP", "BELT"):
        parts = msg.split(",")
        action = parts[1].strip().upper() if len(parts) > 1 else ""
        if "CUT" in action:
            alert_service.trigger_alert(
                device_id=dev_id,
                alert_type="STRAP_CUT_ALERT",
                severity="CRITICAL",
                message="Verified strap-cut sensor event reported",
                event_time=received_at_value,
                source="C05_DEVICE"
            )
        elif "OPEN" in action:
            alert_service.trigger_alert(
                device_id=dev_id,
                alert_type="STRAP_OPENED",
                severity="HIGH",
                message="Verified strap-open sensor event",
                event_time=received_at_value,
                source="C05_DEVICE"
            )
        elif "LOCK" in action or "CLOSE" in action:
            alert_service.trigger_alert(
                device_id=dev_id,
                alert_type="WATCH_LOCKED",
                severity="INFO",
                message="Verified watch locking event",
                event_time=received_at_value,
                source="C05_DEVICE"
            )
            alert_service.resolve_active_alerts_by_type(
                device_id=dev_id,
                alert_types=["STRAP_OPENED"],
                resolved_by="SYSTEM_WATCH_LOCKED",
                notes="Watch locked"
            )

    elif cmd_key in ("POWEROFF", "SHUTDOWN"):
        # Explicit device power-off event (Do not assume an offline watch was powered off)
        alert_service.trigger_alert(
            device_id=dev_id,
            alert_type="DEVICE_POWERED_OFF",
            severity="HIGH",
            message="Explicit device power-off event reported by firmware",
            event_time=received_at_value,
            source="C05_DEVICE"
        )

    elif cmd_key == "FALL":
        alert_service.trigger_alert(
            device_id=dev_id,
            alert_type="FALL_ALERT",
            severity="HIGH",
            message="Verified fall-detection event reported by firmware",
            event_time=received_at_value,
            source="C05_DEVICE"
        )

    # These manufacturer control commands do not represent a GPS fix.
    elif cmd_key in ("PING", "CR", "TIME", "LGZONE"):
        device_service.update_heartbeat(
            dev_id, None, None, received_at=received_at_value
        )
        alert_service.resolve_active_alerts_by_type(
            device_id=dev_id,
            alert_types=["WATCH_OFFLINE", "DEVICE_COMMUNICATION_LOST", "NETWORK_DISCONNECTED"],
            resolved_by="SYSTEM_PING_RECEIVED",
            notes="Watch ping received"
        )

    return cmd_key, reply_bytes
