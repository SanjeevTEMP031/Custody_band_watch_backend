"""
Global test isolation.

Loaded by pytest before any test module is imported. Guarantees that the test
suite can never touch production data:

1. Every sqlite3.connect() to a file named ``custody_watch.db`` (the live
   database, wherever a module resolves it from) is redirected to a temporary
   per-session database. Tests that already use their own temp DB paths are
   unaffected.
2. Firebase/Firestore is disabled, so nothing is written to the cloud project.
   Tests that need Firestore must mock ``get_firestore_client`` explicitly.
3. Application loggers do not write to ``logs/*.log``.
"""

import logging
import sqlite3
import sys
import tempfile
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

LIVE_DB_NAME = "custody_watch.db"

_session_dir = tempfile.TemporaryDirectory(prefix="custody_tests_")
TEST_DB_PATH = Path(_session_dir.name) / "test_custody_watch.db"

# --- 1. Redirect any connection to the live DB file -------------------------
_real_connect = sqlite3.connect


def _guarded_connect(database, *args, **kwargs):
    try:
        if Path(str(database)).name == LIVE_DB_NAME:
            database = str(TEST_DB_PATH)
    except (TypeError, ValueError):
        pass
    return _real_connect(database, *args, **kwargs)


sqlite3.connect = _guarded_connect

# --- 3. Keep test output out of the production log files --------------------
# app.utils.logger skips adding file handlers when a logger already has handlers.
for _name in ("custody_app", "custody_tcp", "custody_gps"):
    _logger = logging.getLogger(_name)
    if not _logger.handlers:
        _logger.addHandler(logging.NullHandler())

# --- 2. Disable Firebase/Firestore -------------------------------------------
from app.core import firebase as _firebase  # noqa: E402

_firebase._firebase_initialized = True
_firebase._firestore_client = None
_firebase._firebase_auth = None

# Create the schemas in the temporary DB so tests behave like a fresh install.
from app.services.firestore_service import firestore_service  # noqa: E402,F401
from app.geofencing.models import init_geofence_database  # noqa: E402
from app.journeys.models import init_journey_database  # noqa: E402

init_geofence_database()
init_journey_database()


def pytest_sessionfinish(session, exitstatus):
    sqlite3.connect = _real_connect
    try:
        _session_dir.cleanup()
    except OSError:
        pass  # Windows may still hold a WAL handle; the OS temp cleaner will remove it.
