"""Shared test helpers. Every test runs against an isolated CC_USAGE_HOME — synthetic
data can never mix into the real database (reviewer safeguard 6)."""
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SRC = REPO / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

FIXTURES = REPO / "tests" / "fixtures"


def temp_home():
    d = tempfile.mkdtemp(prefix="cc-usage-test-")
    os.environ["CC_USAGE_HOME"] = d
    return Path(d)


def fresh_db(home=None):
    from cc_usage.db import connect, migrate
    home = home or Path(os.environ["CC_USAGE_HOME"])
    db = connect(home / "usage.db")
    migrate(db)
    return db


def write_transcript(path, records):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, separators=(",", ":")) + "\n")
    return str(path)


def assistant(session_id, message_id, ts, model="glm-5.3-flash", usage=None,
              sidechain=False, extra=None):
    rec = {
        "type": "assistant", "sessionId": session_id, "timestamp": ts,
        "parentUuid": "p", "uuid": "u-" + (message_id or "x"), "isSidechain": sidechain,
        "cwd": "/tmp", "gitBranch": "main", "version": "2.1.206", "entrypoint": "cli",
        "message": {"id": message_id, "model": model, "stop_reason": "end_turn",
                    "usage": usage if usage is not None else {
                        "input_tokens": 100, "output_tokens": 10,
                        "cache_read_input_tokens": 900, "cache_creation_input_tokens": 0}},
    }
    if extra:
        rec.update(extra)
    return rec


def make_subagent_dir(home, parent_session, agent_hex, records, meta=None):
    d = home / "projects" / "testproj" / parent_session / "subagents"
    d.mkdir(parents=True, exist_ok=True)
    j = d / ("agent-%s.jsonl" % agent_hex)
    with open(j, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, separators=(",", ":")) + "\n")
    if meta is not None:
        (d / ("agent-%s.meta.json" % agent_hex)).write_text(
            json.dumps(meta), encoding="utf-8")
    return str(j)


class Base(unittest.TestCase):
    def setUp(self):
        self.home = temp_home()

    def tearDown(self):
        pass  # temp dirs left in place; no auto-deletion anywhere (spec §11)
