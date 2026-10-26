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
    ph = sp.add_parser("phases", help="workflow phase attribution (build/show)")
    ph.add_argument("action", nargs="?", choices=["build", "show"], default="build")
    ph.add_argument("session_pos", nargs="?", default=None)
    ph.add_argument("--session", default=None)
    ph.add_argument("--lineage", default=None)
    prp = sp.add_parser("pr", help="GitHub PR correlation (sync/show/outcomes)")
    prp.add_argument("action", choices=["sync", "show", "outcomes"])
    prp.add_argument("number", nargs="?", type=int, default=None)
    prp.add_argument("--repo", default=None)
    prp.add_argument("--days", type=int, default=90, help="sync/outcomes window (event-time)")
    rvw = sp.add_parser("reviews", help="critic-artifact severity scan (scan/summary)")
    rvw.add_argument("action", choices=["scan", "summary"], default="summary", nargs="?")
    rvw.add_argument("--root", default="~/Sources/RustLearning/rustfolio/.claude/reviews")
    rv = sp.add_parser("jira", help="Jira snapshot import")
    rv.add_argument("action", choices=["import"])
    rv.add_argument("file")
    q = sp.add_parser("quota", help="Z.ai quota windows (import/show)")
    q.add_argument("action", choices=["import", "show"])
    q.add_argument("file", nargs="?", default=None)
    cst = sp.add_parser("cost", help="four-measure cost report (estimates labeled per §9)")
    cst.add_argument("--days", type=int, default=30)
    dash = sp.add_parser("dashboard", help="local web dashboard on 127.0.0.1 (read-only)")
    dash.add_argument("--port", type=int, default=4399)
    dash.add_argument("--db", default=None)
    dash.add_argument("--open", action="store_true", help="open the browser")
    costp = sp.add_parser("config", help="settings (set/show)")
    costp.add_argument("action", choices=["set", "show"])
    costp.add_argument("key", nargs="?", default=None)
    costp.add_argument("value", nargs="?", default=None)
    sp.add_parser("spool", help="explicit spool maintenance (`spool prune` removes "
                                "replayed payloads; never runs automatically)")
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
    if first == "spool":
        if len(argv) > 1 and argv[1] == "prune":
            from .collector import spool_prune
            return spool_prune()
        print("usage: cc-usage spool prune")
        return 1
    if first == "jira" and len(argv) > 1 and argv[1] == "import":
        # new-phase C command; plain `cc-usage jira [key]` still delegates to v1 report
        from .correlate.jira import import_snapshot
        if len(argv) < 3:
            print("usage: cc-usage jira import <snapshot.json>")
            return 1
        db = connect(DB_PATH, timeout=30)
        migrate(db)
        code = import_snapshot(db, argv[2])
        db.close()
        return code
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
        # CLI never writes SQLite directly — a request marker is consumed by the
        # collector's sweeper (single-writer contract, spec §11; critic P1-2).
        from .collector import submit_request
        submit_request("ingest")
        print("ingest requested (processed by the collector's next sweep)")
        return 0
    if cmd == "backfill":
        start = args.since or (dt.datetime.now(dt.timezone.utc)
                               - dt.timedelta(days=int(args.days or 30))).isoformat()
        end = args.until
        from .collector import submit_request
        submit_request("backfill", ts_window=(start, end))
        print("backfill requested window %s..%s (uses separate backfill offsets; "
              "processed by the collector's next sweep)" % (start, end or "now"))
        return 0
    if cmd == "reconcile":
        from .reconcile import reconcile_session
        db = connect(DB_PATH, timeout=30)
        code = reconcile_session(db, args.session)
        db.close()
        return code
    if cmd == "phases":
        from .attribution import subagents, workflow
        db = connect(DB_PATH, timeout=60)
        migrate(db)
        sessions = [args.session] if args.session else None
        if args.action == "build":
            n = workflow.build(db, sessions)
            d = subagents.build_dispatches(db, sessions)
            j = subagents.join_otel_durations(db, sessions)
            print("phases: %d spans, %d dispatch stamps, %d measured-duration joins"
                  % (n, d, j))
            db.close()
            return 0
        # show
        if args.session is None and getattr(args, "session_pos", None):
            args.session = args.session_pos
        if args.lineage:
            rows = db.execute(
                "SELECT p.phase, COUNT(r.id) responses, "
                "SUM(r.input_tokens+r.output_tokens+r.cache_read_tokens+r.cache_creation_tokens) tok "
                "FROM workflow_phases p JOIN api_requests r ON r.phase_id=p.id "
                "WHERE p.lineage_id=? GROUP BY p.phase ORDER BY tok DESC", (args.lineage,)).fetchall()
            print("lineage %s" % args.lineage)
            for r in rows:
                print("  %-14s %6s responses  %12s tok" % (r["phase"], r["responses"], r["tok"] or 0))
            db.close()
            return 0
        sid = args.session
        if not sid:
            print("usage: cc-usage phases show --session <id> [--lineage <ticket:RUS-xxxx>]")
            db.close()
            return 1
        rows = db.execute("SELECT id, phase, started_at, ended_at, trigger_kind, confidence, basis "
                          "FROM workflow_phases WHERE session_id=? ORDER BY started_at", (sid,)).fetchall()
        print("phases for %s (%d spans; durations are a timeline, not disjoint wall-clock)" % (sid, len(rows)))
        for r in rows:
            print("  %-16s %s -> %s  [%s/%s] %s"
                  % (r["phase"], (r["started_at"] or "")[:19], (r["ended_at"] or "")[:19],
                     r["confidence"], r["basis"], r["trigger_kind"] or ""))
        tot = db.execute(
            "SELECT p.phase, COUNT(r.id) n, "
            "SUM(r.input_tokens+r.output_tokens+r.cache_read_tokens+r.cache_creation_tokens) tok "
            "FROM api_requests r JOIN workflow_phases p ON p.id=r.phase_id "
            "WHERE r.session_id=? GROUP BY p.phase ORDER BY tok DESC", (sid,)).fetchall()
        print("token split:")
        for t in tot:
            print("  %-16s %5d responses %14d tok" % (t["phase"], t["n"], t["tok"] or 0))
        db.close()
        return 0
    if cmd == "pr":
        from .correlate import github
        db = connect(DB_PATH, timeout=180)
        migrate(db)
        if args.action == "sync":
            synced, contribs = github.sync(db, days=args.days, repo=args.repo)
            ref = github.apply_timeline_refinements(db)
            print("pr sync: %d PRs synced, %d contribution rows, %d post-PR tail refinements"
                  % (synced, contribs, ref))
        elif args.action == "show" and args.number:
            p = db.execute("SELECT * FROM pull_requests WHERE number=?", (args.number,)).fetchone()
            if not p:
                print("PR %d not synced" % args.number)
                db.close()
                return 1
            print("PR #%s %s [%s -> %s] state=%s merged=%s rounds=%d" % (
                p["number"], (p["title"] or "")[:70], p["branch"], p["base_branch"],
                p["state"], p["merged"], p["review_rounds"]))
            print("  tickets=%s net=%s(+%s gen) stacked_base=%s leaf=%s" % (
                p["ticket_keys"],
                "%d/- %d" % (p["net_add"] or 0, p["net_del"] or 0) if p["net_add"] is not None else "n/a",
                "(gen +%d/-%d)" % (p["gen_add"] or 0, p["gen_del"] or 0),
                p["base_pr_number"], p["is_stacked_leaf"]))
            for r in db.execute("SELECT author,state,at FROM pr_reviews WHERE pr_number=? "
                                "ORDER BY at", (args.number,)).fetchall():
                print("  review: %-12s %-18s %s" % (r["author"], r["state"], r["at"]))
            for c in db.execute("SELECT provider, role, tokens, responses FROM pr_contributions "
                                "WHERE pr_number=? ORDER BY tokens DESC",
                                (args.number,)).fetchall():
                print("  contrib: %-14s %-10s %10d tok %4d resp"
                      % (c["provider"], c["role"], c["tokens"] or 0, c["responses"] or 0))
        elif args.action == "outcomes":
            _pr_outcomes(db, args.days)
        else:
            print("usage: cc-usage pr sync|outcomes [--days N] | pr show <number>")
        db.close()
        return 0
    if cmd == "reviews":
        from .correlate import reviews as rvmod
        db = connect(DB_PATH)
        migrate(db)
        if args.action == "scan":
            t, fcount = rvmod.scan_root(db, args.root)
            print("scanned %d topics / %d files under %s" % (t, fcount, args.root))
        rvmod.summary(db, os.path.expanduser(args.root) if args.action == "scan" else None)
        db.close()
        return 0
    if cmd == "quota":
        from . import quota
        db = connect(DB_PATH)
        migrate(db)
        if args.action == "import":
            if not args.file:
                print("usage: cc-usage quota import <snapshot.json|kv.txt>")
                db.close()
                return 1
            code = quota.import_snapshot(db, args.file)
        else:
            quota.show(db)
            code = 0
        db.close()
        return code
    if cmd == "cost":
        db = connect(DB_PATH, timeout=30)
        migrate(db)
        _cost_report(db, period_days=int(getattr(args, "days", 30) or 30))
        db.close()
        return 0
    if cmd == "config":
        from . import cost
        db = connect(DB_PATH)
        migrate(db)
        if args.action == "set":
            if not args.key or args.value is None:
                print("usage: cc-usage config set <key> <value>")
                db.close()
                return 1
            cost.set_setting(db, args.key, args.value)
            print("set %s" % args.key)
        else:
            for r in db.execute("SELECT key, value FROM settings ORDER BY key").fetchall():
                print("%s = %s" % (r["key"], r["value"]))
            if not args.key:
                pass
        db.close()
        return 0
    if cmd == "dashboard":
        from .dashboard import server
        return server.run(port=args.port, db_path=args.db, open_browser=args.open)
    if cmd == "spool":
        from .collector import spool_prune
        return spool_prune()
    if cmd == "db":
        print(DB_PATH)
        return 0
    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())


def _pr_outcomes(db, days=None):
    """Provider outcome rollup with the shared-bucket rule (spec §7): a PR counts once;
    multi-provider PRs land in `shared`, never in each contributing provider."""
    from .correlate.github import outcomes
    rows = outcomes(db, days)
    stats = {}
    for r in rows:
        key = r["bucket"] if r["bucket"] in ("shared", "none") else r["bucket"]
        s = stats.setdefault(key, {"prs": 0, "merged": 0, "rounds": 0, "net_add": 0})
        s["prs"] += 1
        s["merged"] += r["merged"]
        s["rounds"] += r["rounds"]
        s["net_add"] += r["net_add"] or 0
    print("PR outcomes (bucket rule: multi-provider PRs counted once as 'shared')")
    print("%-16s %5s %7s %8s %12s %8s" % ("provider/bucket", "PRs", "merged", "avg_rnd",
                                          "net_add", "merge%"))
    for k in sorted(stats, key=lambda x: -stats[x]["prs"]):
        s = stats[k]
        avg = s["rounds"] / float(s["prs"]) if s["prs"] else 0
        mpct = (100.0 * s["merged"] / s["prs"]) if s["prs"] else 0
        print("%-16s %5d %7d %8.1f %12d %7.0f%%" % (k, s["prs"], s["merged"], avg,
                                                    s["net_add"], mpct))
    role_q = ("SELECT c.provider p, c.role role, SUM(c.tokens) tok FROM pr_contributions c "
              "JOIN pull_requests pr ON pr.number=c.pr_number %s GROUP BY 1,2"
              % ("WHERE pr.created_at >= datetime('now', '-%d days')" % days if days else ""))
    by = {}
    for r in db.execute(role_q).fetchall():
        by.setdefault(r["p"], {})[r["role"]] = (r["tok"] or 0) / 1e6
    print("\ntokens by role (M):")
    print("%-16s %10s %10s %10s %10s %10s" % ("provider", "first_impl", "review",
                                              "revision", "pr_work", "merge"))
    for p in sorted(by):
        r = by[p]
        print("%-16s %10.1f %10.1f %10.1f %10.1f %10.1f" % (
            p, r.get("first_impl", 0), r.get("review", 0), r.get("revision", 0),
            r.get("pr_work", 0), r.get("merge", 0)))


def _cost_report(db, period_days=30):
    """Four-measure report (spec §9): observed / estimate / allocation / marginal are
    labeled separately; Z.ai never gets an estimated_api figure."""
    from . import cost
    start = (dt.datetime.now(dt.timezone.utc)
             - dt.timedelta(days=period_days)).isoformat()
    rows = db.execute(
        "SELECT provider, model, input_tokens, output_tokens, cache_read_tokens, "
        "cache_creation_tokens, record_ts FROM v_model_responses WHERE record_ts >= ?",
        (start,)).fetchall()
    by_provider = {}
    est_rows = []
    for r in rows:
        p = r["provider"] or "unknown"
        b = by_provider.setdefault(p, {"tok": 0, "resp": 0})
        b["tok"] += (r["input_tokens"] or 0) + (r["output_tokens"] or 0) + \
            (r["cache_read_tokens"] or 0) + (r["cache_creation_tokens"] or 0)
        b["resp"] += 1
        est_rows.append((p, r["model"], r["input_tokens"] or 0, r["output_tokens"] or 0,
                         r["cache_read_tokens"] or 0, r["cache_creation_tokens"] or 0,
                         ts_epoch(r["record_ts"])))
    est, priced, unpriced = cost.estimate_provider_usd(db, est_rows)
    inv = cost.get_setting(db, cost.INVOICE_KEY)
    print("cost report, last %d days (labels: observed / estimate / allocation / marginal)" % period_days)
    print("%-10s %6s %14s %12s" % ("provider", "resp", "tokens", "basis"))
    for p in sorted(by_provider):
        basis = ("subscription (no per-token estimate exists)" if p in cost.PLAN_PROVIDERS
                 else "pay-per-token")
        print("%-10s %6d %14s %12s" % (p, by_provider[p]["resp"], fmt_m(by_provider[p]["tok"]), basis))
    print("\nestimated_api_usd  : $%.2f  [estimate; priced=%d responses, unpriced=%d;"
          " prices=%s, DeepSeek off-peak ~%s applied]" % (
              est, priced, unpriced, cost.PRICE_VERSION, cost.OFFPEAK["note"]))
    if inv:
        try:
            inv = float(inv)
            print("subscription        : $%.2f invoice on file (%s)" % (inv, cost.INVOICE_KEY))
            print("amortized_usd       : $%.2f  [allocation: invoice divided across the "
                  "billing month; NEVER ticket grain]" % (inv / 30.0 * period_days))
        except ValueError:
            print("subscription        : %s is not numeric" % cost.INVOICE_KEY)
    else:
        print("subscription        : set your actual invoice for amortization: "
              "cc-usage config set %s <amount>" % cost.INVOICE_KEY)
    print("marginal cash       : plan windows with spare capacity -> $0 additional; "
          "API providers -> per-token rates above [estimate]")
    print("quota opportunity   : UNPRICED by design — consuming scarce plan quota can "
          "displace future work; see cc-usage quota show (shared windows)")


def ts_epoch(ts):
    from .ingest.transcripts import ts_epoch as _e
    return _e(ts)


def fmt_m(n):
    return "%.2fM" % (n / 1e6) if n >= 1e6 else str(int(n))
