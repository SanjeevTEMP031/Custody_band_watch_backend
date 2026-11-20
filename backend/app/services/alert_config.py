"""
Centralized Alert Configuration for Huaten C05 GPS Custody Watch
Defines default thresholds, alert severities, and metadata.
"""

import os
from typing import Dict, Any

ALERT_CONFIG: Dict[str, Any] = {
    "watch_offline_seconds": int(os.getenv("ALERT_WATCH_OFFLINE_SECONDS", "120")),
    "low_battery_percent": int(os.getenv("ALERT_LOW_BATTERY_PERCENT", "20")),
    "gps_unavailable_seconds": int(os.getenv("ALERT_GPS_UNAVAILABLE_SECONDS", "120")),
    "repeated_disconnect_count": int(os.getenv("ALERT_REPEATED_DISCONNECT_COUNT", "3")),
    "repeated_disconnect_window_seconds": int(os.getenv("ALERT_REPEATED_DISCONNECT_WINDOW_SECONDS", "600")),
    "vehicle_separation_meters": float(os.getenv("ALERT_VEHICLE_SEPARATION_METERS", "100")),
    "unscheduled_stop_minutes": float(os.getenv("ALERT_UNSCHEDULED_STOP_MINUTES", "5")),
}

# 21 Required Alert Types with authoritative severity classifications
ALERT_SEVERITIES: Dict[str, str] = {
    # CRITICAL
    "SOS_ALERT": "CRITICAL",
    "STRAP_CUT_ALERT": "CRITICAL",
    "TAMPER_ALERT": "CRITICAL",
    "BRACELET_REMOVED": "CRITICAL",
    # HIGH
    "BRACELET_DISCONNECTED": "HIGH",
    "STRAP_OPENED": "HIGH",
    "SKIN_CONTACT_LOST": "HIGH",
    "DEVICE_POWERED_OFF": "HIGH",
    "WATCH_OFFLINE": "HIGH",
    "DEVICE_COMMUNICATION_LOST": "HIGH",
    "NETWORK_DISCONNECTED": "HIGH",
    "REPEATED_DISCONNECTION": "HIGH",
    "GPS_UNAVAILABLE": "HIGH",
    "VEHICLE_SEPARATION_ALERT": "HIGH",
    "ROUTE_DEVIATION_ALERT": "HIGH",
    "FALL_ALERT": "HIGH",
    # MEDIUM
    "UNSCHEDULED_STOP_ALERT": "MEDIUM",
    "LOW_BATTERY_ALERT": "MEDIUM",
    "WATCH_INACTIVE": "MEDIUM",
    # INFO
    "BRACELET_CONNECTED": "INFO",
    "WATCH_LOCKED": "INFO",
}

ALERT_TITLES: Dict[str, str] = {
    "SOS_ALERT": "Emergency SOS Triggered",
    "STRAP_CUT_ALERT": "Strap Cut Detected",
    "TAMPER_ALERT": "Hardware Tampering Detected",
    "BRACELET_REMOVED": "Bracelet Removed from Subject",
    "BRACELET_DISCONNECTED": "Bracelet Link Disconnected",
    "STRAP_OPENED": "Strap Opened",
    "SKIN_CONTACT_LOST": "Skin Contact Lost",
    "DEVICE_POWERED_OFF": "Device Powered Off",
    "WATCH_OFFLINE": "Watch Offline",
    "DEVICE_COMMUNICATION_LOST": "Device Communication Lost",
    "NETWORK_DISCONNECTED": "Network Disconnected",
    "REPEATED_DISCONNECTION": "Repeated Disconnections Detected",
    "GPS_UNAVAILABLE": "GPS Fix Unavailable",
    "VEHICLE_SEPARATION_ALERT": "Vehicle Separation Breach",
    "ROUTE_DEVIATION_ALERT": "Approved Route Deviation",
    "UNSCHEDULED_STOP_ALERT": "Unscheduled Vehicle Stop",
    "LOW_BATTERY_ALERT": "Low Battery Warning",
    "WATCH_INACTIVE": "Prolonged Inactivity Detected",
    "FALL_ALERT": "Fall Event Detected",
    "BRACELET_CONNECTED": "Bracelet Connected",
    "WATCH_LOCKED": "Watch Secured & Locked",
}

# Legacy code mapping for backwards compatibility
LEGACY_ALERT_TYPE_MAP: Dict[str, str] = {
    "SOS": "SOS_ALERT",
    "STRAP_CUT": "STRAP_CUT_ALERT",
    "TAMPER": "TAMPER_ALERT",
    "LOW_BATTERY": "LOW_BATTERY_ALERT",
    "DEVICE_OFFLINE": "WATCH_OFFLINE",
    "GEOFENCE_BREACH": "ROUTE_DEVIATION_ALERT",
}
