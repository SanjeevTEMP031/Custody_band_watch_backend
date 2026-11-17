"""
Health & Biometrics Service
Tracks wearer pulse trends, step counts, and abnormal physiological patterns.
"""

import sqlite3
from pathlib import Path
from typing import List, Dict, Any, Optional
from ..services.device_service import device_service

BASE_DIR = Path(__file__).resolve().parent.parent.parent
LOCAL_DB = BASE_DIR / "custody_watch.db"


class HealthService:
    def __init__(self):
        self._ensure_table()

    def _ensure_table(self):
        try:
            conn = sqlite3.connect(str(LOCAL_DB))
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS heart_rate (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    device_id TEXT,
                    timestamp TEXT,
                    bpm INTEGER,
                    blood_pressure TEXT,
                    spo2 INTEGER
                )
            """)
            conn.commit()
            conn.close()
        except Exception:
            pass

    def record_heart_rate(self, device_id: str, bpm: int, bp: str = "120/80", spo2: int = 98):
        from ..utils.datetime_utils import current_iso_timestamp
        self._ensure_table()
        try:
            conn = sqlite3.connect(str(LOCAL_DB))
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO heart_rate (device_id, timestamp, bpm, blood_pressure, spo2)
                VALUES (?, ?, ?, ?, ?)
            """, (device_id, current_iso_timestamp(), bpm, bp, spo2))
            conn.commit()
            conn.close()
        except Exception:
            pass

    def get_heart_rate_history(self, device_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        self._ensure_table()
        try:
            conn = sqlite3.connect(str(LOCAL_DB))
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            rows = cur.execute("""
                SELECT id, device_id, timestamp, bpm, blood_pressure, spo2 FROM heart_rate
                WHERE device_id = ? ORDER BY id DESC LIMIT ?
            """, (device_id, limit)).fetchall()
            res = [dict(r) for r in rows]
            conn.close()
            return res
        except Exception:
            return []

    def get_health_summary(self, device_id: str) -> Dict[str, Any]:
        dev = device_service.get_device_by_id(device_id) or {}
        hr_history = self.get_heart_rate_history(device_id, limit=30)

        bpms = [h["bpm"] for h in hr_history if h.get("bpm", 0) > 0]
        device_hr = dev.get("heart_rate")
        curr_bpm: Optional[Any] = (
            device_hr if device_hr is not None
            else (hr_history[0]["bpm"] if hr_history else None)
        )
        avg_bpm = round(sum(bpms) / len(bpms)) if bpms else curr_bpm
        max_bpm = max(bpms) if bpms else curr_bpm
        min_bpm = min(bpms) if bpms else curr_bpm

        battery = dev.get("battery")
        if battery is None:
            battery = dev.get("battery_percent")

        blood_pressure = None
        if hr_history and hr_history[0].get("blood_pressure") not in (None, ""):
            blood_pressure = hr_history[0].get("blood_pressure")
        if blood_pressure is None:
            blood_pressure = dev.get("blood_pressure")

        spo2 = None
        if hr_history and hr_history[0].get("spo2") is not None:
            spo2 = hr_history[0].get("spo2")
        if spo2 is None:
            spo2 = dev.get("blood_oxygen")
        if spo2 is None:
            spo2 = dev.get("spo2")

        return {
            "device_id": device_id,
            "imei": device_id,
            "wearer_name": dev.get("wearer_name") or "Custody Subject",
            "name": dev.get("wearer_name") or "Custody Subject",
            "current_pulse_bpm": curr_bpm,
            "pulse": curr_bpm,
            "bpm": curr_bpm,
            "heart_rate": curr_bpm,
            "average_bpm": avg_bpm,
            "avg_bpm": avg_bpm,
            "max_bpm": max_bpm,
            "min_bpm": min_bpm,
            "today_steps": dev.get("steps"),
            "steps": dev.get("steps"),
            "battery_percent": battery,
            "battery": battery,
            "status": dev.get("status", "ONLINE"),
            "online": dev.get("status", "ONLINE") in ("ONLINE", "ACTIVE"),
            "blood_pressure": blood_pressure,
            "spo2": spo2,
            "temperature": dev.get("temperature"),
            "blood_oxygen": spo2,
            "blood_glucose": dev.get("blood_glucose"),
            "glucose_mmol_l": dev.get("glucose_mmol_l"),
            "hrv": dev.get("hrv"),
            "sleep": dev.get("sleep"),
            "sleep_report": dev.get("sleep_report"),
            "sleep_allday": dev.get("sleep_allday"),
            "recent_pulse_readings": hr_history[:10],
            "history": hr_history
        }


health_service = HealthService()
