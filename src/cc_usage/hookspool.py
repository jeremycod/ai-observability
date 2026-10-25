"""Hook spool: hooks NEVER write SQLite (single-writer policy, spec v3.1 §11).

enqueue() appends one JSON line to spool/hooks/events.jsonl in <1 ms; the collector
consumes the file through an offset state (spool/hooks/.state.json). Nothing is deleted.
"""
import json
import os
import sys

from .db import now_iso
from .identity import config_snapshot_from_env


def events_path(data_dir):
    d = os.path.join(str(data_dir), "spool", "hooks")
    return os.path.join(d, "events.jsonl"), d


# Fields persisted from hook payloads. Everything else (notably `prompt` and
# `command_args` text) is reduced to derived, non-sensitive fields below — raw prompt
# or command text must never reach disk (spec §11; critic P0-1).
_PERSIST_FIELDS = ("hook_event_name", "session_id", "prompt_id", "cwd", "source",
                   "command_name", "command_source", "permission_mode", "version", "model")


def reduce_payload(hook_data):
    """Keep allowlisted fields + derived non-sensitive values only."""
    data = hook_data or {}
    out = {k: data.get(k) for k in _PERSIST_FIELDS if data.get(k) is not None}
    from .identity import jira_from
    prompt = data.get("prompt")
    args = data.get("command_args")
    if prompt is not None:
        out["prompt_length"] = len(str(prompt))
        out["prompt_jira"] = jira_from(prompt)
    if args is not None:
        out["command_args_jira"] = jira_from(args)
    return out


def enqueue(data_dir, hook_data):
    """Serialize a REDUCED hook payload + allowlisted env snapshot. Never raises to the
    caller; must stay <~1 ms and must never block Claude Code."""
    try:
        path, d = events_path(data_dir)
        os.makedirs(d, exist_ok=True)
        payload = {"captured_at": now_iso()}
        payload.update(reduce_payload(hook_data))
        payload["env_snapshot"] = config_snapshot_from_env()
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, separators=(",", ":"), default=str) + "\n")
        return True
    except OSError:
        return False


def hook_command(data_dir):
    """Entry point for `cc-usage hook`: read hook JSON from stdin, enqueue, exit 0."""
    try:
        data = json.load(sys.stdin)
    except Exception:
        return 0
    if isinstance(data, dict):
        enqueue(data_dir, data)
    return 0


def read_events(data_dir, state_dir=None):
    """Yield (state_dict, [payloads]) not yet consumed. The state file records the byte
    offset; spooled data is never deleted."""
    path, d = events_path(data_dir)
    state_path = os.path.join(state_dir or d, ".state.json")
    offset = 0
    try:
        with open(state_path, "r", encoding="utf-8") as f:
            offset = int(json.load(f).get("offset", 0))
    except (OSError, ValueError):
        offset = 0
    payloads = []
    try:
        size = os.path.getsize(path)
        if size < offset:
            offset = 0
        with open(path, "rb") as f:
            f.seek(offset)
            chunk = f.read()
        pos = 0
        while True:
            nl = chunk.find(b"\n", pos)
            if nl < 0:
                break
            line = chunk[pos:nl]
            pos = nl + 1
            if line.strip():
                try:
                    payloads.append(json.loads(line))
                except ValueError:
                    pass
    except OSError:
        return {"offset": offset}, []
    return {"offset": offset + pos, "state_path": state_path}, payloads


def commit_state(state):
    sp = state.get("state_path")
    if not sp:
        return
    tmp = sp + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"offset": state["offset"]}, f)
        os.replace(tmp, sp)
    except OSError:
        pass
