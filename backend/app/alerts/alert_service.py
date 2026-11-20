"""
Alert Incident Management Service
Authoritative wrapper delegating to centralized alert_service.
Ensures single authoritative lifecycle owner and complete backwards compatibility.
"""

from typing import List, Dict, Any, Optional
from ..services.alert_service import alert_service


class IncidentAlertService:
    """Backwards-compatible adapter delegating to the centralized alert_service."""

    def list_alerts(
        self,
        status: Optional[str] = None,
        alert_type: Optional[str] = None,
        device_id: Optional[str] = None,
        severity: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 50,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        return alert_service.list_alerts(
            status=status,
            severity=severity,
            alert_type=alert_type,
            device_id=device_id,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            offset=offset
        )

    def get_alert(self, alert_id: Any) -> Optional[Dict[str, Any]]:
        return alert_service.get_alert_by_id(alert_id)

    def acknowledge_alert(self, alert_id: Any, officer_email: str = "admin", notes: Optional[str] = None) -> Dict[str, Any]:
        res = alert_service.acknowledge_alert(alert_id, acknowledged_by=officer_email, notes=notes)
        return res or {}

    def resolve_alert(self, alert_id: Any, officer_email: str = "admin", resolution_notes: Optional[str] = "Resolved") -> Dict[str, Any]:
        res = alert_service.resolve_alert(alert_id, resolved_by=officer_email, resolution_notes=resolution_notes)
        return res or {}


incident_alert_service = IncidentAlertService()
