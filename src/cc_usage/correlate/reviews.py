"""Phase C: critic-artifact severity parsing from .claude/reviews/ (spec v3.1 §7).

Formats vary across review flows, so this is deliberately a conservative COUNTING
parser: severity markers are counted per markdown file, finding TEXT is never stored.
Confidence is 'low' by construction — findings supplement PR review truth, they never
override it. Dispositions come only from explicit decision filenames.
"""
import os
import re

from ..db import now_iso

SEVERITY_PATTERNS = (
    ("p0", re.compile(r"\bP0\b|\bBLOCKER\b|\bCRITICAL\b", re.I)),
    ("p1", re.compile(r"\bP1\b|\bMAJOR\b|\bHIGH\b", re.I)),
    ("p2", re.compile(r"\bP2\b|\bMINOR\b|\bMEDIUM\b", re.I)),
    ("p3", re.compile(r"\bP3\b|\bNIT\b|\bLOW\b", re.I)),
)
DISPOSITION_FILES = re.compile(r"(final|arbiter|decision|resolved|verdict)", re.I)
DISPOSITION_MAP = (
    ("accepted", re.compile(r"\b(accept|approved|pass(ed)?|no blockers?)\b", re.I)),
    ("changes_requested", re.compile(r"\b(changes? requested|revise|blocked?|fail(ed)?)\b", re.I)),
)


def scan_root(db, root):
    """Scan one .claude/reviews root directory. Returns (topics, files_scanned)."""
    root = os.path.abspath(os.path.expanduser(root))
    if not os.path.isdir(root):
        print("reviews root not found: %s" % root)
        return (0, 0)
    topics = files = 0
    for topic in sorted(os.listdir(root)):
        tdir = os.path.join(root, topic)
        if not os.path.isdir(tdir):
            continue
        topics += 1
        counts = {}
        disposition = None
        t_files = 0
        for dirpath, _dirs, names in os.walk(tdir):
            for name in names:
                if not name.lower().endswith((".md", ".txt")):
                    continue
                path = os.path.join(dirpath, name)
                try:
                    text = open(path, encoding="utf-8", errors="replace").read()
                except OSError:
                    continue
                files += 1
                t_files += 1
                if DISPOSITION_FILES.search(name):
                    for label, rx in DISPOSITION_MAP:
                        if rx.search(text[-2000:]):  # decision files: tail only
                            disposition = label
                            break
                for label, rx in SEVERITY_PATTERNS:
                    hits = len(rx.findall(text))
                    if hits:
                        counts[label] = counts.get(label, 0) + hits
        db.execute("DELETE FROM critic_findings WHERE root=? AND topic=?", (root, topic))
        for label, hits in sorted(counts.items()):
            db.execute("INSERT OR REPLACE INTO critic_findings(root,topic,severity,hits,at) "
                       "VALUES(?,?,?,?,?)", (root, topic, label, hits, now_iso()))
        db.execute("INSERT INTO review_artifacts(root,topics,files,disposition,scanned_at) "
                   "VALUES(?,?,?,?,?) ON CONFLICT(root) DO UPDATE SET "
                   "topics=excluded.topics, files=review_artifacts.files + excluded.files, "
                   "disposition=COALESCE(excluded.disposition, review_artifacts.disposition), "
                   "scanned_at=excluded.scanned_at",
                   (root, 1, t_files, disposition, now_iso()))
    db.commit()
    return (topics, files)


def summary(db, root=None):
    q = ("SELECT topic, severity, hits FROM critic_findings" +
         (" WHERE root=?" if root else "") + " ORDER BY topic, severity")
    rows = db.execute(q, (root,) if root else()).fetchall()
    by_topic = {}
    for r in rows:
        by_topic.setdefault(r["topic"], {})[r["severity"]] = r["hits"]
    print("critic findings by severity (%d topics)" % len(by_topic))
    for topic in sorted(by_topic):
        sev = by_topic[topic]
        print("  %-42s %s" % (topic, "  ".join("%s=%d" % (k, sev[k]) for k in sorted(sev))))
