"""cc-usage v2 entrypoint: new Phase A commands + 100% legacy delegation.

Delegated commands run through the preserved v1 module (cc_usage_legacy_v1.py) so
statusline/env/report/backup behavior is byte-identical. New commands: hook (spool
enqueue), collector v2, doctor v2, ingest, backfill (event-time window), reconcile.
"""
import argparse
import datetime as dt
import importlib.util
import os
import sys
from pathlib import Path

from . import __version__
from .config import DATA_DIR, DB_PATH, LEGACY_MODULE, PROJECTS_DIR
from .db import connect, migrate

LEGACY_COMMANDS = ("statusline", "env", "backup", "restore", "providers", "skills",
                   "models", "jira", "matrix", "sessions", "today", "week", "month")


def _legacy_path():
    # Resolve at call time: CC_USAGE_HOME may differ per process/invocation.
    home = os.environ.get("CC_USAGE_HOME")
    if home:
        return Path(home) / "cc_usage_legacy_v1.py"
    return LEGACY_MODULE


def _run_legacy(args):
    path = _legacy_path()
    if not path.exists():
        raise SystemExit("legacy cc_usage module not found at %s; run install.sh" % path)
    spec = importlib.util.spec_from_file_location("cc_usage_legacy_v1", str(path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    sys.argv = ["cc-usage"] + list(args)
    try:
        return mod.main()
    except SystemExit as e:  # argparse exits; propagate code
        return e.code if isinstance(e.code, int) else 0


def _sweep(db, projects_dir, ts_window=None):
    from .collector import transcript_files
    from .ingest.transcripts import ingest_file
    touched = 0
    for path in transcript_files(Path(projects_dir)):
        r = ingest_file(db, path, ts_window=ts_window)
        if r and r["records"]:
            touched += 1
    db.commit()
    return touched


def build_parser():
    p = argparse.ArgumentParser(prog="cc-usage",
                                description="local Claude Code usage profiler v2")
    p.add_argument("--version", action="version", version="cc-usage %s" % __version__)
    sp = p.add_subparsers(dest="cmd")
    c = sp.add_parser("collector", help="manage local OTLP collector (v2)")
    c.add_argument("action", choices=["start", "stop", "restart", "status", "foreground"])
    sp.add_parser("hook", help=argparse.SUPPRESS)
    sp.add_parser("doctor", help="validate installation (v2)")
    ing = sp.add_parser("ingest", help="run one transcript sweep now")
    ing.add_argument("--projects-dir", default=str(PROJECTS_DIR))
    bf = sp.add_parser("backfill", help="event-time-windowed historical ingest")
    bf.add_argument("--days", type=int, default=30)
    bf.add_argument("--since", default=None)
    bf.add_argument("--until", default=None)
    bf.add_argument("--projects-dir", default=str(PROJECTS_DIR))
    rec = sp.add_parser("reconcile", help="recompute a session's totals from raw transcripts")
    rec.add_argument("session")
    sp.add_parser("db", help="print database path")
    sp.add_parser("help", help="show command guide")
    return p


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    # Pre-scan the first positional token BEFORE argparse: legacy commands must reach
    # the v1 module untouched (argparse would reject them as invalid choices — this was
    # a live regression caught by the post-install compatibility smoke test).
    first = next((a for a in argv if not a.startswith("-")), None)
    if first is None or first == "help":
        return _run_legacy(["help"])
    if first == "env":
        # Launchers call `cc-usage env <launcher> <provider>`; v1 also (re)started its own
        # collector here. Emit the IDENTICAL export lines but ensure the v2 collector.
        from .collector import collector_alive, port_open, start_collector
        if not (collector_alive() and port_open()):
            start_collector()
        path = _legacy_path()
        spec = importlib.util.spec_from_file_location("cc_usage_legacy_v1", str(path))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        positional = [a for a in argv if not a.startswith("-")][1:]
        mod.env_lines(positional[0] if positional else "unknown",
                      positional[1] if len(positional) > 1 else "unknown")
        return 0
    if first in LEGACY_COMMANDS:
        return _run_legacy(argv)

    parser = build_parser()
    args, rest = parser.parse_known_args(argv)
    cmd = getattr(args, "cmd", None)

    if cmd == "hook":
        from .hookspool import hook_command
        return hook_command(DATA_DIR)
    if cmd == "collector":
        from . import collector
        return collector.dispatch(getattr(args, "action", "status"))
    if cmd == "doctor":
        from .doctor import run as doctor_run
        return doctor_run()
    if cmd == "ingest":
        db = connect(DB_PATH)
        migrate(db)
        touched = _sweep(db, args.projects_dir)
        print("ingest: %d files had new records" % touched)
        db.close()
        return 0
    if cmd == "backfill":
        start = args.since or (dt.datetime.now(dt.timezone.utc)
                               - dt.timedelta(days=int(args.days or 30))).isoformat()
        end = args.until
        db = connect(DB_PATH)
        migrate(db)
        touched = _sweep(db, args.projects_dir, ts_window=(start, end))
        print("backfill window %s..%s: %d files had in-window records" % (start, end or "now", touched))
        db.close()
        return 0
    if cmd == "reconcile":
        from .reconcile import reconcile_session
        db = connect(DB_PATH, timeout=30)
        code = reconcile_session(db, args.session)
        db.close()
        return code
    if cmd == "db":
        print(DB_PATH)
        return 0
    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
