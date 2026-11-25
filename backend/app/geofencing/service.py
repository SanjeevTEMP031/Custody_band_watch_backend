"""
Geofence Business Logic Service
Handles CRUD for circular and polygon perimeters, device assignments,
event histories, and compliance checks.
"""

import json
import uuid
from typing import Dict, Any, List, Optional
from ..utils.datetime_utils import current_iso_timestamp
from ..utils.logger import app_logger
from .models import (
    get_db_connection,
    init_geofence_database,
    sync_geofence_to_firestore,
)
from .geometry import (
    validate_polygon_coordinates,
    calculate_haversine_distance_meters,
    is_point_in_circle,
    is_point_in_polygon,
)
from .schemas import GeofenceCreate, GeofenceUpdate


class GeofenceService:
    def __init__(self):
        init_geofence_database()

    def create_geofence(
        self, geo_in: GeofenceCreate, created_by: Optional[str] = None
    ) -> Dict[str, Any]:
        """Creates a new circular or polygon geofence with optional device assignments."""
        geo_type = (geo_in.type or "CIRCLE").upper()
        fence_type = (geo_in.fence_type or "INCLUSION").upper()
        now_iso = current_iso_timestamp()
        geofence_id = f"geo_{uuid.uuid4().hex[:10]}"

        poly_coords_json: Optional[str] = None
        c_lat: Optional[float] = None
        c_lon: Optional[float] = None
        c_radius: Optional[float] = None

        if geo_type == "POLYGON":
            is_valid, err, cleaned_coords = validate_polygon_coordinates(geo_in.polygon_coordinates or [])
            if not is_valid:
                raise ValueError(err or "Invalid polygon coordinates")
            poly_coords_json = json.dumps(cleaned_coords)
            # Calculate polygon centroid as center fallback for map displays
            lats = [p[0] for p in cleaned_coords]
            lons = [p[1] for p in cleaned_coords]
            c_lat = round(sum(lats) / len(lats), 6)
            c_lon = round(sum(lons) / len(lons), 6)
            c_radius = 0.0
        else:
            # Circle
            if geo_in.center_latitude is None or geo_in.center_longitude is None:
                raise ValueError("Circle geofences require center_latitude and center_longitude")
            c_lat = float(geo_in.center_latitude)
            c_lon = float(geo_in.center_longitude)
            c_radius = float(geo_in.radius_meters or 500.0)

        assigned_devices = list(geo_in.device_ids or [])
        if geo_in.assigned_device_id and geo_in.assigned_device_id not in assigned_devices:
            assigned_devices.append(geo_in.assigned_device_id)

        legacy_assigned = assigned_devices[0] if assigned_devices else None

        conn = get_db_connection()
        try:
            cur = conn.cursor()

            cur.execute("""
                INSERT INTO geofences (
                    name, description, type, fence_type, center_latitude,
                    center_longitude, radius_meters, polygon_coordinates, active,
                    entry_alert, exit_alert, created_by, created_at, updated_at, assigned_device_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                geo_in.name, geo_in.description, geo_type, fence_type,
                c_lat, c_lon, c_radius, poly_coords_json,
                1 if geo_in.active else 0,
                1 if geo_in.entry_alert else 0,
                1 if geo_in.exit_alert else 0,
                created_by or "admin", now_iso, now_iso, legacy_assigned
            ))

            new_id = cur.lastrowid
            geofence_id = str(new_id)

            # Assign devices
            for dev_id in assigned_devices:
                assign_id = f"assign_{uuid.uuid4().hex[:10]}"
                cur.execute("""
                    INSERT OR REPLACE INTO device_geofence_assignments (
                        id, device_id, geofence_id, assigned_at, active
                    ) VALUES (?, ?, ?, ?, 1)
                """, (assign_id, dev_id, geofence_id, now_iso))

            conn.commit()
        finally:
            conn.close()

        record = self.get_geofence(geofence_id)
        if record:
            sync_geofence_to_firestore(record)
        return record or {}

    def get_geofence(self, geofence_id: Any) -> Optional[Dict[str, Any]]:
        """Retrieves a single geofence by ID with its assigned device list."""
        conn = get_db_connection()
        try:
            cur = conn.cursor()

            row = cur.execute("SELECT * FROM geofences WHERE id = ?", (str(geofence_id),)).fetchone()
            if not row:
                return None

            d = dict(row)
            d["id"] = str(d["id"])
            d["active"] = bool(d.get("active"))
            d["is_active"] = d["active"]
            d["entry_alert"] = bool(d.get("entry_alert", 1))
            d["exit_alert"] = bool(d.get("exit_alert", 1))

            # Aliases for frontend map components
            d["latitude"] = d.get("center_latitude")
            d["longitude"] = d.get("center_longitude")
            d["radius_m"] = d.get("radius_meters")

            poly_raw = d.get("polygon_coordinates")
            if poly_raw and isinstance(poly_raw, str):
                try:
                    d["polygon_coordinates"] = json.loads(poly_raw)
                except Exception:
                    d["polygon_coordinates"] = []
            elif not poly_raw:
                d["polygon_coordinates"] = []

            # Get assigned devices
            dev_rows = cur.execute("""
                SELECT device_id FROM device_geofence_assignments
                WHERE geofence_id = ? AND active = 1
            """, (str(geofence_id),)).fetchall()
            assigned = [r["device_id"] for r in dev_rows]

            # Merge legacy assigned_device_id if not present
            if d.get("assigned_device_id") and d["assigned_device_id"] not in assigned:
                assigned.append(d["assigned_device_id"])

            d["assigned_devices"] = assigned
            d["assigned_device_count"] = len(assigned)

            return d
        finally:
            conn.close()

    def list_geofences(
        self,
        active: Optional[bool] = None,
        device_id: Optional[str] = None,
        skip: int = 0,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """List all geofences with optional active and device filters."""
        conn = get_db_connection()
        cur = conn.cursor()

        query = "SELECT DISTINCT g.id FROM geofences g"
        params = []
        joins = []
        wheres = ["1=1"]

        if device_id:
            joins.append("LEFT JOIN device_geofence_assignments a ON g.id = a.geofence_id")
            wheres.append("((a.device_id = ? AND a.active = 1) OR g.assigned_device_id = ?)")
            params.extend([str(device_id), str(device_id)])

        if active is not None:
            wheres.append("g.active = ?")
            params.append(1 if active else 0)

        sql = f"{query} {' '.join(joins)} WHERE {' AND '.join(wheres)} ORDER BY g.created_at DESC LIMIT ? OFFSET ?"
        params.extend([limit, skip])

        rows = cur.execute(sql, params).fetchall()
        conn.close()

        results = []
        for r in rows:
            g = self.get_geofence(r["id"])
            if g:
                results.append(g)
        return results

    def update_geofence(
        self, geofence_id: Any, geo_up: GeofenceUpdate
    ) -> Optional[Dict[str, Any]]:
        """Updates parameters and geometry of an existing geofence."""
        clean_id = str(geofence_id)
        existing = self.get_geofence(clean_id)
        if not existing:
            # Upsert fallback: if geofence ID was purged or client is restoring it
            if clean_id.isdigit():
                conn = get_db_connection()
                try:
                    cur = conn.cursor()
                    now_iso = current_iso_timestamp()
                    cur.execute("""
                        INSERT OR REPLACE INTO geofences (
                            id, name, description, type, fence_type, center_latitude,
                            center_longitude, radius_meters, polygon_coordinates, active,
                            entry_alert, exit_alert, created_by, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        int(clean_id),
                        geo_up.name or f"Geofence {clean_id}",
                        geo_up.description,
                        (geo_up.type or "CIRCLE").upper(),
                        (geo_up.fence_type or "INCLUSION").upper(),
                        float(geo_up.center_latitude) if geo_up.center_latitude is not None else 0.0,
                        float(geo_up.center_longitude) if geo_up.center_longitude is not None else 0.0,
                        float(geo_up.radius_meters) if geo_up.radius_meters is not None else 100.0,
                        json.dumps(geo_up.polygon_coordinates) if geo_up.polygon_coordinates else None,
                        1 if geo_up.active is not False else 0,
                        1 if geo_up.entry_alert is not False else 0,
                        1 if geo_up.exit_alert is not False else 0,
                        "admin", now_iso, now_iso
                    ))
                    conn.commit()
                finally:
                    conn.close()
                existing = self.get_geofence(clean_id)
            if not existing:
                return None

        now_iso = current_iso_timestamp()
        fields = []
        params = []

        if geo_up.name is not None:
            fields.append("name = ?")
            params.append(geo_up.name.strip())

        if geo_up.description is not None:
            fields.append("description = ?")
            params.append(geo_up.description.strip())

        if geo_up.fence_type is not None:
            fields.append("fence_type = ?")
            params.append(geo_up.fence_type.upper())

        geo_type = (geo_up.type or existing["type"]).upper()
        if geo_up.type is not None:
            fields.append("type = ?")
            params.append(geo_type)

        if geo_type == "POLYGON" and geo_up.polygon_coordinates is not None:
            is_valid, err, cleaned_coords = validate_polygon_coordinates(geo_up.polygon_coordinates)
            if not is_valid:
                raise ValueError(err or "Invalid polygon geometry")
            fields.append("polygon_coordinates = ?")
            params.append(json.dumps(cleaned_coords))
            # Recalculate centroid
            lats = [p[0] for p in cleaned_coords]
            lons = [p[1] for p in cleaned_coords]
            fields.append("center_latitude = ?")
            params.append(round(sum(lats) / len(lats), 6))
            fields.append("center_longitude = ?")
            params.append(round(sum(lons) / len(lons), 6))
        else:
            if geo_up.center_latitude is not None:
                fields.append("center_latitude = ?")
                params.append(float(geo_up.center_latitude))
            if geo_up.center_longitude is not None:
                fields.append("center_longitude = ?")
                params.append(float(geo_up.center_longitude))
            if geo_up.radius_meters is not None:
                fields.append("radius_meters = ?")
                params.append(float(geo_up.radius_meters))

        if geo_up.active is not None:
            fields.append("active = ?")
            params.append(1 if geo_up.active else 0)

        if geo_up.entry_alert is not None:
            fields.append("entry_alert = ?")
            params.append(1 if geo_up.entry_alert else 0)

        if geo_up.exit_alert is not None:
            fields.append("exit_alert = ?")
            params.append(1 if geo_up.exit_alert else 0)

        fields.append("updated_at = ?")
        params.append(now_iso)

        conn = get_db_connection()
        cur = conn.cursor()

        if fields:
            params.append(clean_id)
            cur.execute(f"UPDATE geofences SET {', '.join(fields)} WHERE id = ?", params)

        # Handle device assignments if provided
        if geo_up.device_ids is not None:
            cur.execute("DELETE FROM device_geofence_assignments WHERE geofence_id = ?", (clean_id,))
            for dev_id in geo_up.device_ids:
                cur.execute("""
                    INSERT OR REPLACE INTO device_geofence_assignments (
                        id, device_id, geofence_id, assigned_at, active
                    ) VALUES (?, ?, ?, ?, 1)
                """, (f"assign_{uuid.uuid4().hex[:10]}", dev_id, clean_id, now_iso))

        conn.commit()
        conn.close()

        updated = self.get_geofence(clean_id)
        if updated:
            sync_geofence_to_firestore(updated)
        return updated

    def delete_geofence(self, geofence_id: Any) -> bool:
        """Deletes a geofence and cleans up device assignments and states."""
        clean_id = str(geofence_id)
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("DELETE FROM geofences WHERE id = ?", (clean_id,))
        rows = cur.rowcount
        cur.execute("DELETE FROM device_geofence_assignments WHERE geofence_id = ?", (clean_id,))
        cur.execute("DELETE FROM device_geofence_states WHERE geofence_id = ?", (clean_id,))

        conn.commit()
        conn.close()
        return rows > 0

    def assign_devices(self, geofence_id: Any, device_ids: List[str]) -> Dict[str, Any]:
        """Assigns multiple devices to a geofence."""
        clean_id = str(geofence_id)
        existing = self.get_geofence(clean_id)
        if not existing:
            raise ValueError(f"Geofence {clean_id} not found")

        now_iso = current_iso_timestamp()
        conn = get_db_connection()
        cur = conn.cursor()

        assigned_count = 0
        for dev_id in device_ids:
            cur.execute("""
                INSERT OR REPLACE INTO device_geofence_assignments (
                    id, device_id, geofence_id, assigned_at, active
                ) VALUES (?, ?, ?, ?, 1)
            """, (f"assign_{uuid.uuid4().hex[:10]}", dev_id.strip(), clean_id, now_iso))
            assigned_count += 1

        if device_ids:
            cur.execute("UPDATE geofences SET assigned_device_id = ? WHERE id = ?", (device_ids[0].strip(), clean_id))

        conn.commit()
        conn.close()

        updated = self.get_geofence(clean_id)
        if updated:
            sync_geofence_to_firestore(updated)

        return {
            "geofence_id": clean_id,
            "assigned_count": assigned_count,
            "assigned_devices": updated.get("assigned_devices", []) if updated else []
        }

    def remove_device(self, geofence_id: Any, device_id: str) -> bool:
        """Removes a device assignment from a geofence."""
        clean_id = str(geofence_id)
        dev_str = str(device_id).strip()
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            DELETE FROM device_geofence_assignments
            WHERE geofence_id = ? AND device_id = ?
        """, (clean_id, dev_str))

        # Clear legacy assigned_device_id column if it matches
        cur.execute("""
            UPDATE geofences
            SET assigned_device_id = NULL
            WHERE id = ? AND assigned_device_id = ?
        """, (clean_id, dev_str))

        # Also remove from states
        cur.execute("""
            DELETE FROM device_geofence_states
            WHERE geofence_id = ? AND device_id = ?
        """, (clean_id, dev_str))

        conn.commit()
        conn.close()
        return True

    def get_assigned_devices(self, geofence_id: Any) -> List[Dict[str, Any]]:
        """List all devices currently assigned to this geofence."""
        clean_id = str(geofence_id)
        conn = get_db_connection()
        cur = conn.cursor()

        has_devices = cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='devices'").fetchone()
        if has_devices:
            rows = cur.execute("""
                SELECT a.device_id, a.assigned_at, d.maker, d.wearer_name
                FROM device_geofence_assignments a
                LEFT JOIN devices d ON a.device_id = d.device_id
                WHERE a.geofence_id = ? AND a.active = 1
            """, (clean_id,)).fetchall()
        else:
            rows = cur.execute("""
                SELECT a.device_id, a.assigned_at
                FROM device_geofence_assignments a
                WHERE a.geofence_id = ? AND a.active = 1
            """, (clean_id,)).fetchall()

        conn.close()
        return [dict(r) for r in rows]

    def get_geofence_events(
        self,
        geofence_id: Any,
        event_type: Optional[str] = None,
        device_id: Optional[str] = None,
        skip: int = 0,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Retrieves history of geofence transition events."""
        clean_id = str(geofence_id)
        conn = get_db_connection()
        cur = conn.cursor()

        query = "SELECT * FROM geofence_events WHERE geofence_id = ?"
        params = [clean_id]

        if event_type:
            query += " AND event_type = ?"
            params.append(event_type.upper())
        if device_id:
            query += " AND device_id = ?"
            params.append(str(device_id))

        query += " ORDER BY event_time DESC LIMIT ? OFFSET ?"
        params.extend([limit, skip])

        rows = cur.execute(query, params).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def check_position_compliance(
        self, device_id: str, lat: float, lon: float
    ) -> Dict[str, Any]:
        """
        Backward-compatible ad-hoc position compliance check against assigned geofences.
        """
        fences = self.list_geofences(active=True, device_id=device_id)
        violations = []

        for f in fences:
            geo_type = (f.get("type") or "CIRCLE").upper()
            rule_type = (f.get("fence_type") or "INCLUSION").upper()

            inside = False
            dist = 0.0
            if geo_type == "POLYGON":
                inside = is_point_in_polygon(lat, lon, f.get("polygon_coordinates") or [])
            else:
                c_lat = f.get("center_latitude")
                c_lon = f.get("center_longitude")
                if c_lat is not None and c_lon is not None:
                    dist = calculate_haversine_distance_meters(lat, lon, c_lat, c_lon)
                    inside = dist <= (f.get("radius_meters") or 500.0)

            is_violation = False
            if rule_type == "INCLUSION" and not inside:
                is_violation = True
                msg = f"Breach! Left designated inclusion zone '{f['name']}'"
            elif rule_type == "EXCLUSION" and inside:
                is_violation = True
                msg = f"Breach! Entered prohibited exclusion zone '{f['name']}'"

            if is_violation:
                violations.append({
                    "geofence_id": f["id"],
                    "geofence_name": f["name"],
                    "fence_type": rule_type,
                    "distance_meters": round(dist, 1),
                    "allowed_radius": f.get("radius_meters"),
                    "message": msg
                })

        return {
            "device_id": device_id,
            "latitude": lat,
            "longitude": lon,
            "is_compliant": len(violations) == 0,
            "violations_count": len(violations),
            "violations": violations
        }


geofence_service = GeofenceService()
