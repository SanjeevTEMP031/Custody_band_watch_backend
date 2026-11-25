"""
Comprehensive Unit & Integration Test Suite for the C05 Alert & Tampering Monitoring System
Tests:
- All 21 Alert Types and severities
- Non-assumption rules (AL != SOS, disconnected != cut/removed, offline != powered off)
- Sensor state reporting (UNKNOWN / UNSUPPORTED when unverified)
- Location snapshot capturing (Priority 1 Fresh GPS, Priority 2 Historical, Priority 3 Unavailable)
- Idempotency & Deduplication
- State transitions (ACTIVE -> ACKNOWLEDGED -> RESOLVED)
- Auto-resolution on condition clear
- REST API endpoints and filtering
- WebSocket live streaming
"""

import importlib
import sys
import time
from pathlib import Path

# Ensure backend root is on sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import pytest
from datetime import datetime, timezone, timedelta
from fastapi.testclient import TestClient

alert_module = importlib.import_module("app.services.alert_service")
firestore_module = importlib.import_module("app.services.firestore_service")
command_module = importlib.import_module("app.commands.command_service")
geofence_module = importlib.import_module("app.geofences.geofence_service")
health_module = importlib.import_module("app.health_data.health_service")
stats_module = importlib.import_module("app.statistics.statistics_service")
account_module = importlib.import_module("app.accounts.account_service")
auth_module = importlib.import_module("app.authentication.auth_service")
fb_core = importlib.import_module("app.core.firebase")

from app.main import app
from app.services.alert_service import alert_service
from app.services.alert_config import ALERT_CONFIG, ALERT_SEVERITIES, ALERT_TITLES
from app.services.device_service import device_service
from app.services.firestore_service import firestore_service
from app.receiver.packet_parser import parse_ud, parse_packet


@pytest.fixture(autouse=True)
def isolate_test_environment(monkeypatch, tmp_path):
    test_db_path = tmp_path / "test_custody_watch.db"
    
    # Patch LOCAL_DB across all relevant services
    for mod in (alert_module, firestore_module, command_module, geofence_module, health_module, stats_module, account_module, auth_module):
        if hasattr(mod, "LOCAL_DB"):
            monkeypatch.setattr(mod, "LOCAL_DB", test_db_path)

    # Patch is_firebase_active to False so tests never touch Firestore or outbox sync
    monkeypatch.setattr(fb_core, "is_firebase_active", lambda: False)
    monkeypatch.setattr(alert_module, "is_firebase_active", lambda: False)
    monkeypatch.setattr(firestore_module, "is_firebase_active", lambda: False)

    # Re-initialize the test database schema
    firestore_service._init_local_db()
    alert_service._ensure_tables()

    # Clear in-memory caches
    with firestore_service._device_cache_lock:
        firestore_service._device_cache.clear()
        firestore_service._devices_cache = None

    yield


@pytest.fixture
def client():
    return TestClient(app)


class TestAlertTamperingSystem:
    test_imei = "863758061830999"

    def setup_method(self):
        # Register test device
        device_service.register_device({
            "device_id": self.test_imei,
            "maker": "MNYD",
            "wearer_name": "Subject Test",
            "case_number": "CASE-TEST-001"
        })

    def test_01_all_21_alert_types_and_severities(self):
        """Verify all 21 alert types are defined with the exact required severity."""
        required_alerts = {
            "SOS_ALERT": "CRITICAL",
            "STRAP_CUT_ALERT": "CRITICAL",
            "TAMPER_ALERT": "CRITICAL",
            "BRACELET_REMOVED": "CRITICAL",
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
            "UNSCHEDULED_STOP_ALERT": "MEDIUM",
            "LOW_BATTERY_ALERT": "MEDIUM",
            "WATCH_INACTIVE": "MEDIUM",
            "FALL_ALERT": "HIGH",
            "BRACELET_CONNECTED": "INFO",
            "WATCH_LOCKED": "INFO",
        }

        for alert_type, expected_sev in required_alerts.items():
            assert alert_type in ALERT_SEVERITIES, f"Missing alert type: {alert_type}"
            assert ALERT_SEVERITIES[alert_type] == expected_sev, (
                f"Mismatch for {alert_type}: expected {expected_sev}, got {ALERT_SEVERITIES[alert_type]}"
            )
            assert alert_type in ALERT_TITLES, f"Missing title for {alert_type}"

    def test_02_do_not_assume_al_always_means_sos(self):
        """Rule: Do not assume that AL always means SOS."""
        dev = f"TEST_AL_{int(time.time())}"

        # 1. AL frame with bit 6 set (0x40 = STRAP CUT) -> must trigger STRAP_CUT_ALERT, not SOS
        strap_cut_status = "00000040"
        packet = f"AL,261118,063000,A,17.446317,N,78.368395,E,0.0,0.0,500.0,8,90,85,1200,{strap_cut_status},0".encode("latin-1")
        parse_ud(packet, dev, "MNYD", gps_source="watch_tcp")

        alerts = alert_service.list_alerts(device_id=dev, limit=5)
        assert len(alerts) >= 1
        assert alerts[0]["type"] == "STRAP_CUT_ALERT"
        assert alerts[0]["severity"].upper() == "CRITICAL"

        # 2. AL frame with no specific bits set (00000000) -> must trigger TAMPER_ALERT, never blindly SOS
        dev2 = f"TEST_AL_UNSPECIFIED_{int(time.time())}"
        packet2 = f"AL,261118,063100,A,17.446317,N,78.368395,E,0.0,0.0,500.0,8,90,85,1200,00000000,0".encode("latin-1")
        parse_ud(packet2, dev2, "MNYD", gps_source="watch_tcp")

        alerts2 = alert_service.list_alerts(device_id=dev2, limit=5)
        assert len(alerts2) >= 1
        assert alerts2[0]["type"] != "SOS_ALERT", "AL frame with zero bits must not default to SOS"
        assert alerts2[0]["type"] == "TAMPER_ALERT"

    def test_03_do_not_assume_disconnected_bracelet_is_physically_cut_or_removed(self):
        """Rule: Do not assume a disconnected bracelet is physically removed or cut."""
        dev = f"TEST_DISC_{int(time.time())}"

        # Bracelet disconnected packet (e.g. BT,0)
        parse_packet("MNYD", dev, b"BT,0")

        alerts = alert_service.list_alerts(device_id=dev, limit=5)
        assert len(alerts) >= 1
        assert alerts[0]["type"] == "BRACELET_DISCONNECTED"
        assert alerts[0]["type"] not in ("STRAP_CUT_ALERT", "BRACELET_REMOVED")
        assert alerts[0]["severity"].upper() == "HIGH"

        # Auto-clearing when connected packet arrives
        parse_packet("MNYD", dev, b"BT,1")
        active = alert_service.list_alerts(device_id=dev, status="ACTIVE", alert_type="BRACELET_DISCONNECTED")
        assert len(active) == 0, "BRACELET_DISCONNECTED should be auto-resolved upon reconnection"

    def test_04_do_not_assume_offline_watch_was_powered_off(self):
        """Rule: Do not assume an offline watch was powered off."""
        dev = f"TEST_OFFLINE_{int(time.time())}"

        # Explicit POWEROFF packet
        parse_packet("MNYD", dev, b"POWEROFF")
        alerts = alert_service.list_alerts(device_id=dev, limit=5)
        assert len(alerts) >= 1
        assert alerts[0]["type"] == "DEVICE_POWERED_OFF"

        # Background offline alert
        alert_service.trigger_alert(
            device_id=dev,
            alert_type="WATCH_OFFLINE",
            severity="HIGH",
            message="Watch has not communicated within threshold"
        )
        alerts_all = alert_service.list_alerts(device_id=dev, limit=10)
        types = [a["type"] for a in alerts_all]
        assert "WATCH_OFFLINE" in types
        assert "DEVICE_POWERED_OFF" in types
        # They must be separate distinct alert records
        assert len(types) >= 2

    def test_05_unknown_sensor_states_are_unsupported_or_unknown(self):
        """Rule: Expose sensor state as UNKNOWN or UNSUPPORTED when hardware support is unverified."""
        dev = f"TEST_SENSORS_{int(time.time())}"
        status = alert_service.get_device_alert_status(dev)

        assert status["strap_status"] in ("UNKNOWN", "SECURED")
        assert status["bracelet_status"] in ("UNKNOWN", "CONNECTED")
        assert status["skin_contact_status"] in ("UNSUPPORTED", "UNKNOWN")

    def test_06_location_snapshot_priorities(self):
        """Rule: Location snapshot follows Priority 1 (Fresh) -> Priority 2 (Historical) -> Priority 3 (Unavailable)."""
        dev = f"TEST_LOC_{int(time.time())}"

        # 1. Fresh GPS in alert event
        alert_fresh = alert_service.trigger_alert(
            device_id=dev,
            alert_type="SOS_ALERT",
            latitude=17.446317,
            longitude=78.368395,
            gps_valid=True,
            gps_timestamp=datetime.now(timezone.utc).isoformat()
        )
        assert alert_fresh["location_source"] in ("GPS_FRESH", "LIVE")
        assert alert_fresh["latitude"] == 17.446317
        assert alert_fresh["longitude"] == 78.368395
        assert bool(alert_fresh["location_is_stale"]) is False

        # 2. No GPS in current alert event, but historical telemetry exists
        # Ingest a valid fix first
        packet = b"UD,261118,060000,A,17.449000,N,78.369000,E,0.0,0.0,500.0,8,90,80,100,00000000,0"
        parse_ud(packet, dev, "MNYD", gps_source="watch_tcp")

        # Now trigger an alert with status 'V' (no fix)
        alert_hist = alert_service.trigger_alert(
            device_id=dev,
            alert_type="STRAP_OPENED",
            latitude=None,
            longitude=None,
            gps_valid=False
        )
        assert alert_hist["location_source"] == "HISTORICAL"
        assert alert_hist["latitude"] == 17.449
        assert alert_hist["longitude"] == 78.369
        assert bool(alert_hist["location_is_stale"]) is True
        assert alert_hist["location_age_seconds"] is not None

        # 3. New device with zero fixes anywhere -> must be UNAVAILABLE, never 0,0
        dev_empty = f"TEST_NO_GPS_{int(time.time())}"
        alert_none = alert_service.trigger_alert(
            device_id=dev_empty,
            alert_type="TAMPER_ALERT",
            latitude=None,
            longitude=None,
            gps_valid=False
        )
        assert alert_none["location_source"] == "UNAVAILABLE"
        assert alert_none["latitude"] is None
        assert alert_none["longitude"] is None
        assert bool(alert_none["location_is_stale"]) is True

    def test_07_alert_idempotency_and_deduplication(self):
        """Verify that triggering the same active alert multiple times does not produce duplicates."""
        dev = f"TEST_DEDUP_{int(time.time())}"

        # Trigger once
        a1 = alert_service.trigger_alert(dev, "STRAP_CUT_ALERT", message="Strap cut #1")
        # Trigger second time while still ACTIVE
        a2 = alert_service.trigger_alert(dev, "STRAP_CUT_ALERT", message="Strap cut #2")

        assert a1["id"] == a2["id"]
        assert a1["alert_id"] == a2["alert_id"]

        # Only 1 record in database
        alerts = alert_service.list_alerts(device_id=dev, alert_type="STRAP_CUT_ALERT")
        assert len(alerts) == 1

    def test_08_alert_lifecycle_ack_does_not_resolve_ongoing_hardware_alert(self):
        """Acknowledgement must NOT automatically resolve an ongoing hardware alert."""
        dev = f"TEST_LIFE_{int(time.time())}"
        a = alert_service.trigger_alert(dev, "STRAP_CUT_ALERT")
        alert_id = a["id"]

        # Acknowledge
        acked = alert_service.acknowledge_alert(alert_id, acknowledged_by="officer_smith", notes="Inspecting subject")
        assert acked["status"] == "ACKNOWLEDGED"
        assert acked["acknowledged_by"] == "officer_smith"
        assert acked["acknowledged_at"] is not None
        assert acked["resolved_at"] is None

        # Alert is STILL considered active/unresolved in status queries
        active_list = alert_service.list_alerts(device_id=dev, status="ACTIVE")
        assert len(active_list) >= 1

        # Now explicitly resolve
        resolved = alert_service.resolve_alert(alert_id, resolved_by="officer_smith", resolution_notes="Strap verified intact")
        assert resolved["status"] == "RESOLVED"
        assert resolved["resolved_at"] is not None
        assert resolved["resolved_by"] == "officer_smith"

        # Now active list is empty
        active_now = alert_service.list_alerts(device_id=dev, status="ACTIVE")
        assert len(active_now) == 0

    def test_09_battery_alert_threshold_and_recharge_auto_resolution(self):
        """Low battery triggers at <= 20% and resolves when recharged."""
        dev = f"TEST_BATT_{int(time.time())}"

        # Battery 18% <= 20% -> triggers
        alert_service.check_battery_alert(dev, 18)
        active = alert_service.list_alerts(device_id=dev, alert_type="LOW_BATTERY_ALERT", status="ACTIVE")
        assert len(active) == 1

        # Recharged to 45% -> auto-resolves
        alert_service.check_battery_alert(dev, 45)
        active_after = alert_service.list_alerts(device_id=dev, alert_type="LOW_BATTERY_ALERT", status="ACTIVE")
        assert len(active_after) == 0

    def test_10_rest_api_endpoints(self, client):
        """Verify all 8 mandatory REST endpoints are active and functional."""
        dev = f"TEST_API_{int(time.time())}"
        a = alert_service.trigger_alert(dev, "SOS_ALERT")
        alert_id = a["id"]
        alt_str = a["alert_id"]

        # 1. GET /api/v1/alerts
        res = client.get("/api/v1/alerts")
        assert res.status_code == 200
        assert isinstance(res.json(), list)

        # 2. GET /api/v1/alerts/active
        res = client.get("/api/v1/alerts/active")
        assert res.status_code == 200
        assert isinstance(res.json(), list)

        # 3. GET /api/v1/alerts/{alert_id} (both integer and string)
        res_int = client.get(f"/api/v1/alerts/{alert_id}")
        assert res_int.status_code == 200
        assert res_int.json()["alert_type"] == "SOS_ALERT"

        res_str = client.get(f"/api/v1/alerts/{alt_str}")
        assert res_str.status_code == 200
        assert res_str.json()["alert_type"] == "SOS_ALERT"

        # 4. GET /api/v1/alerts/device/{device_id}
        res_dev = client.get(f"/api/v1/alerts/device/{dev}")
        assert res_dev.status_code == 200
        assert len(res_dev.json()) >= 1

        # 5. POST /api/v1/alerts/{alert_id}/acknowledge
        res_ack = client.post(f"/api/v1/alerts/{alert_id}/acknowledge", json={"notes": "On my way"})
        assert res_ack.status_code == 200
        assert res_ack.json()["status"] == "ACKNOWLEDGED"

        # 6. POST /api/v1/alerts/{alert_id}/resolve
        res_res = client.post(f"/api/v1/alerts/{alert_id}/resolve", json={"resolution_notes": "All safe"})
        assert res_res.status_code == 200
        assert res_res.json()["status"] == "RESOLVED"

        # 7. GET /api/v1/devices/{device_id}/alert-status
        res_status = client.get(f"/api/v1/devices/{dev}/alert-status")
        assert res_status.status_code == 200
        data = res_status.json()
        assert "connectivity_status" in data
        assert "strap_status" in data
        assert "bracelet_status" in data
        assert "active_alert_count" in data
        assert "last_known_location" in data
        assert "current_live_location" in data

    def test_11_websocket_live_alerts_endpoint(self, client):
        """Verify WS /api/v1/alerts/live connects and transmits messages."""
        with client.websocket_connect("/api/v1/alerts/live") as websocket:
            data = websocket.receive_json()
            assert data["type"] == "INIT"
            assert "active_alerts" in data
            websocket.send_text("ping")
            pong = websocket.receive_text()
            assert pong == "pong"
