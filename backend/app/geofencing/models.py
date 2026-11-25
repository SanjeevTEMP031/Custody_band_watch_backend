"""
Geofence Database Models & Persistence Module
Manages SQLite schema creation, migrations, indexes, and Firestore synchronization.
"""

import json
import sqlite3
import threading
from pathlib import Path
from typing import Dict, Any, Optional, List

from ..core.firebase import get_firestore_client, is_firebase_active
from ..utils.logger import app_logger

BASE_DIR = Path(__file__).resolve().parent.parent.parent
LOCAL_DB = BASE_DIR / "custody_watch.db"


def get_db_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(str(LOCAL_DB), timeout=30.0)
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.row_factory = sqlite3.Row
    return conn


def init_geofence_database():
    """
    Ensure all geofencing tables, indexes, and schema migrations exist.
    Preserves existing data and adds new columns cleanly.
    """
    conn = get_db_connection()
    try:
        cur = conn.cursor()

        # 1. Base geofences table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS geofences (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                description TEXT,
                type TEXT NOT NULL DEFAULT 'CIRCLE',
                fence_type TEXT NOT NULL DEFAULT 'INCLUSION',
                center_latitude REAL DEFAULT 0.0,
                center_longitude REAL DEFAULT 0.0,
                radius_meters REAL DEFAULT 0.0,
                polygon_coordinates TEXT,
                active INTEGER DEFAULT 1,
                entry_alert INTEGER DEFAULT 1,
                exit_alert INTEGER DEFAULT 1,
                created_by TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                assigned_device_id TEXT
            )
        """)

        # Check for missing columns in existing geofences table (migration)
        existing_cols = [r[1] for r in cur.execute("PRAGMA table_info(geofences)").fetchall()]
        column_defs = {
            "description": "TEXT",
            "type": "TEXT NOT NULL DEFAULT 'CIRCLE'",
            "polygon_coordinates": "TEXT",
            "active": "INTEGER DEFAULT 1",
            "entry_alert": "INTEGER DEFAULT 1",
            "exit_alert": "INTEGER DEFAULT 1",
            "created_by": "TEXT",
            "updated_at": "TEXT",
        }
        for col_name, col_type in column_defs.items():
            if col_name not in existing_cols:
                try:
                    cur.execute(f"ALTER TABLE geofences ADD COLUMN {col_name} {col_type}")
                    app_logger.info(f"[GEOFENCE MIGRATION] Added column '{col_name}' to geofences table")
                except Exception as e:
                    app_logger.debug(f"[GEOFENCE MIGRATION] Note adding column {col_name}: {e}")

        # 2. Device Geofence Assignments Table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS device_geofence_assignments (
                id TEXT PRIMARY KEY,
                device_id TEXT NOT NULL,
                geofence_id TEXT NOT NULL,
                assigned_at TEXT NOT NULL,
                active INTEGER DEFAULT 1,
                UNIQUE(device_id, geofence_id)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_dev_geo_assign 
            ON device_geofence_assignments(device_id, active)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_geo_dev_assign 
            ON device_geofence_assignments(geofence_id, active)
        """)

        # 3. Device Geofence States Table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS device_geofence_states (
                device_id TEXT NOT NULL,
                geofence_id TEXT NOT NULL,
                confirmed_state TEXT NOT NULL,
                candidate_state TEXT,
                candidate_count INTEGER DEFAULT 0,
                last_valid_gps_time TEXT,
                last_evaluated_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (device_id, geofence_id)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_dev_geo_states 
            ON device_geofence_states(device_id, confirmed_state)
        """)

        # 4. Geofence Events Table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS geofence_events (
                id TEXT PRIMARY KEY,
                device_id TEXT NOT NULL,
                geofence_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                latitude REAL,
                longitude REAL,
                event_time TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_geo_events_time 
            ON geofence_events(geofence_id, event_time)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_dev_events_time 
            ON geofence_events(device_id, event_time)
        """)

        # 5. Geofence Alerts Table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS geofence_alerts (
                id TEXT PRIMARY KEY,
                event_id TEXT NOT NULL,
                device_id TEXT NOT NULL,
                geofence_id TEXT NOT NULL,
                severity TEXT NOT NULL DEFAULT 'HIGH',
                status TEXT NOT NULL DEFAULT 'ACTIVE',
                requires_alarm INTEGER DEFAULT 1,
                title TEXT,
                message TEXT,
                latitude REAL,
                longitude REAL,
                acknowledged_by TEXT,
                acknowledged_at TEXT,
                resolved_by TEXT,
                resolved_at TEXT,
                core_alert_id TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)

        existing_alert_cols = [r[1] for r in cur.execute("PRAGMA table_info(geofence_alerts)").fetchall()]
        if "core_alert_id" not in existing_alert_cols:
            try:
                cur.execute("ALTER TABLE geofence_alerts ADD COLUMN core_alert_id TEXT")
            except Exception:
                pass
        if "requires_alarm" not in existing_alert_cols:
            try:
                cur.execute("ALTER TABLE geofence_alerts ADD COLUMN requires_alarm INTEGER DEFAULT 1")
            except Exception:
                pass
        # Persist the transition type on the alert itself so alert history keeps its
        # label even when the matching geofence_events row is unavailable.
        for col_name in ("event_type", "event_time"):
            if col_name not in existing_alert_cols:
                try:
                    cur.execute(f"ALTER TABLE geofence_alerts ADD COLUMN {col_name} TEXT")
                except Exception:
                    pass
        # Backfill only from real event rows; unmatched legacy alerts stay NULL.
        cur.execute("""
            UPDATE geofence_alerts
            SET event_type = (SELECT ge.event_type FROM geofence_events ge WHERE ge.id = geofence_alerts.event_id),
                event_time = COALESCE(event_time,
                    (SELECT ge.event_time FROM geofence_events ge WHERE ge.id = geofence_alerts.event_id))
            WHERE event_type IS NULL
              AND EXISTS (SELECT 1 FROM geofence_events ge WHERE ge.id = geofence_alerts.event_id)
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_geo_alerts_status 
            ON geofence_alerts(status, device_id)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_geo_alerts_geo 
            ON geofence_alerts(geofence_id, created_at)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_geo_alerts_core_id 
            ON geofence_alerts(core_alert_id)
        """)

        conn.commit()
    finally:
        conn.close()


def sync_geofence_to_firestore(doc_data: Dict[str, Any]):
    """Asynchronously sync geofence definition to Firestore."""
    if not is_firebase_active():
        return

    def _sync():
        try:
            db = get_firestore_client()
            if db:
                db.collection("geofences").document(str(doc_data["id"])).set(doc_data, merge=True)
        except Exception as e:
            app_logger.debug(f"[FIRESTORE GEOFENCE SYNC] Note: {e}")

    threading.Thread(target=_sync, daemon=True).start()


def sync_event_to_firestore(doc_data: Dict[str, Any]):
    """Asynchronously sync geofence event to Firestore."""
    if not is_firebase_active():
        return

    def _sync():
        try:
            db = get_firestore_client()
            if db:
                db.collection("geofence_events").document(str(doc_data["id"])).set(doc_data, merge=True)
                # Also attach to device subcollection
                db.collection("devices").document(doc_data["device_id"]).collection("geofence_events").document(str(doc_data["id"])).set(doc_data, merge=True)
        except Exception as e:
            app_logger.debug(f"[FIRESTORE GEOFENCE EVENT SYNC] Note: {e}")

    threading.Thread(target=_sync, daemon=True).start()


def sync_alert_to_firestore(doc_data: Dict[str, Any]):
    """Asynchronously sync geofence alert to Firestore."""
    if not is_firebase_active():
        return

    def _sync():
        try:
            db = get_firestore_client()
            if db:
                db.collection("geofence_alerts").document(str(doc_data["id"])).set(doc_data, merge=True)
                # Also save to top-level alerts collection so dashboard alerts list displays it
                db.collection("alerts").document(str(doc_data["id"])).set(doc_data, merge=True)
        except Exception as e:
            app_logger.debug(f"[FIRESTORE GEOFENCE ALERT SYNC] Note: {e}")

    threading.Thread(target=_sync, daemon=True).start()
