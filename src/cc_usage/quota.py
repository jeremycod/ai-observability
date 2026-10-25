"""Phase D: Z.ai quota windows — snapshots in, shared-window rule out (spec v3.1 §8).

- NO token→quota conversion exists anywhere: snapshots and token series are stored
  separately and only ever juxtaposed with the mandatory caption.
- When >1 session is active in a window, consumption is SHARED EVIDENCE; per-ticket or
  per-session quota attribution is forbidden by construction (no view computes it).
- Window exhaustion events (Z.ai codes 1302/1308/1310) arrive via provider_events with
  reset timestamps when the provider includes them.
- Capacity remaining is computed ONLY from snapshots.
"""
import datetime as dt
import json
import re

from .db import now_iso
from .ingest.transcripts import ts_epoch

RESET_RE = re.compile(r"reset[^0-9]{0,20}(\d{4}-\d{2}-\d{2}[T ][0-9:]+)", re.I)
QUOTA_CODES = {"1302": "rate_limit", "1308": "quota_5h_exhausted", "1310": "quota_weekly_exhausted"}


def import_snapshot(db, path):
    """Import a quota snapshot file. Accepted forms (schema-flexible):
      {"kind": "5h"|"weekly", "consumed_pct": 87, "window_start": "...", "reset_at": "..."}
      [{...}, {...}]  (multiple windows)
      key=value lines text file (kind=5h, consumed_pct=87, ...)
    `captured_at` is stored separately from imported_at (pasted snapshots may be late)."""
    text = open(path, encoding="utf-8").read()
    entries = []
    stripped = text.strip()
    if stripped.startswith("[") or stripped.startswith("{"):
        data = json.loads(stripped)
        entries = data if isinstance(data, list) else [data]
    else:
        entry = {}
        for line in stripped.splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                entry[k.strip()] = v.strip()
        entries = [entry]
    n = 0
    for e in entries:
        kind = (e.get("kind") or e.get("window") or "").lower()
        if kind not in ("5h", "weekly"):
            raise SystemExit("snapshot entry needs kind=5h|weekly: %r" % (e,))
        consumed = e.get("consumed_pct", e.get("consumed", e.get("used_pct")))
        if consumed is None:
            raise SystemExit("snapshot entry needs consumed_pct: %r" % (e,))
        db.execute(
            "INSERT INTO quota_windows(provider,kind,window_start,window_end,"
            "consumed_pct_or_units,source,captured_at,imported_at,note) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            ("zai", kind, e.get("window_start"), e.get("reset_at") or e.get("window_end"),
             float(consumed), "snapshot", e.get("captured_at"), now_iso(),
             e.get("note")))
        n += 1
    db.commit()
    print("imported %d quota snapshot window(s)" % n)
    return 0


def extract_quota_event(db, session_id, error_text, ts, source="transcript"):
    """Extract Z.ai quota/rate-limit codes from an error string into provider_events;
    store the code + reset hint, never the raw error text."""
    if not error_text:
        return None
    m = re.search(r"\b(1302|1308|1310)\b", str(error_text))
    if not m:
        return None
    code = m.group(1)
    reset = None
    rm = RESET_RE.search(str(error_text))
    if rm:
        reset = rm.group(1)
    db.execute("INSERT INTO provider_events(ts,provider,gateway,session_id,kind,code,detail,"
               "source,reset_at) VALUES(?,?,?,?,?,?,?,?,?)",
               (ts, "zai", None, session_id, QUOTA_CODES[code], code, None, source, reset))
    return code


def active_sessions(db, window_start_e, window_end_e):
    """Sessions overlapping a window (shared-window rule reporting)."""
    from .ingest.transcripts import ts_epoch
    rows = db.execute("SELECT session_id, started_at, ended_at FROM sessions").fetchall()
    out = []
    for r in rows:
        s = ts_epoch(r["started_at"]) or None if r["started_at"] else None
        e = ts_epoch(r["ended_at"]) or None if r["ended_at"] else None
        if s is None:
            continue
        e = e or dt.datetime.now(dt.timezone.utc).timestamp()
        if s <= window_end_e and e >= window_start_e:
            out.append(r["session_id"])
    return out


def show(db, limit=12):
    rows = db.execute("SELECT * FROM quota_windows ORDER BY imported_at DESC LIMIT ?",
                      (limit,)).fetchall()
    print("quota windows (snapshots; NO conversion to tokens exists — the two series are "
          "shown side-by-side with shared-window captions only)")
    if not rows:
        print("  (none imported — cc-usage quota import <file>)")
    for r in rows:
        s = ts_epoch(r["window_start"]) or None if r["window_start"] else None
        e = ts_epoch(r["window_end"]) or None if r["window_end"] else None
        act = len(active_sessions(db, s, e)) if (s and e) else None
        cap = ("" if r["consumed_pct_or_units"] is None else
               " capacity~%.0f%%" % max(0.0, 100.0 - float(r["consumed_pct_or_units"])))
        shared = (" [SHARED: %d active sessions — consumption is shared evidence, not "
                  "per-session attribution]" % act) if act else ""
        print("  %-6s %s..%s used=%s%%%s src=%s%s" % (
            r["kind"], (r["window_start"] or "?")[:16], (r["window_end"] or "?")[:16],
            r["consumed_pct_or_units"], cap, r["source"], shared))
    ev = db.execute("SELECT ts, kind, code, reset_at FROM provider_events "
                    "WHERE kind LIKE 'quota%' OR kind='rate_limit' ORDER BY ts DESC LIMIT 8").fetchall()
    if ev:
        print("\nexhaustion/rate-limit events (observed):")
        for r in ev:
            print("  %s %-24s code=%s reset=%s" % (r["ts"][:19], r["kind"], r["code"],
                                                   r["reset_at"] or "-"))
