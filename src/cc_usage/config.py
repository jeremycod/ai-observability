"""Runtime paths and tuning. CC_USAGE_HOME overrides the data dir (used by tests and
doctor's isolated self-check so synthetic data can never touch the real database)."""
import os
from pathlib import Path

HOME = Path.home()
DATA_DIR = Path(os.environ.get("CC_USAGE_HOME", str(HOME / ".local" / "share" / "cc-usage")))
DB_PATH = DATA_DIR / "usage.db"
PID_PATH = DATA_DIR / "collector.pid"
LOG_PATH = DATA_DIR / "collector.log"
HOST = "127.0.0.1"
PORT = int(os.environ.get("CC_USAGE_PORT", "43182"))
SPOOL_MAX_BYTES = int(os.environ.get("CC_USAGE_SPOOL_MAX", str(200 * 1024 * 1024)))
SWEEP_INTERVAL = float(os.environ.get("CC_USAGE_SWEEP_INTERVAL", "60"))
PROJECTS_DIR = Path(os.environ.get("CC_USAGE_PROJECTS_DIR",
                                   str(HOME / ".claude" / "projects")))
LEGACY_MODULE = DATA_DIR / "cc_usage_legacy_v1.py"
