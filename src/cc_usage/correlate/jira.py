"""Phase C: Jira snapshot import (spec v3.1 §7). No credentials, no fetcher — the user
exports issues to JSON (e.g. via the Atlassian MCP or a JQL export) and imports.
Schema-flexible: accepts [issue,...] or {"issues":[...]} with key/status/summary.
"""
import json

from ..db import now_iso


def import_snapshot(db, path):
    raw = json.load(open(path, encoding="utf-8"))
    issues = raw.get("issues") if isinstance(raw, dict) else raw
    if not isinstance(issues, list):
        raise SystemExit("snapshot must be an array of issues or {issues: [...]}")
    n = 0
    for it in issues:
        key = it.get("key")
        if not key:
            continue
        f = it.get("fields") or {}
        issuetype = it.get("issuetype")
        if not issuetype and isinstance(f.get("issuetype"), dict):
            issuetype = f["issuetype"].get("name")
        db.execute("INSERT INTO jira_issues(key,status,summary,issuetype,created,updated,"
                   "imported_at) VALUES(?,?,?,?,?,?,?) ON CONFLICT(key) DO UPDATE SET "
                   "status=excluded.status,summary=excluded.summary,"
                   "issuetype=excluded.issuetype,created=excluded.created,"
                   "updated=excluded.updated,imported_at=excluded.imported_at",
                   (key, it.get("status") or f.get("status"),
                    (it.get("summary") or f.get("summary") or "")[:300],
                    issuetype,
                    it.get("created") or f.get("created"),
                    it.get("updated") or f.get("updated"), now_iso()))
        n += 1
    db.commit()
    print("imported %d jira issues" % n)
    return 0
