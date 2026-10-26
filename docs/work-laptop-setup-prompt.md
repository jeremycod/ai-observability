# Work-laptop setup prompt

Paste everything below the line into a fresh Claude Code session on the work laptop.

---

## Mission: install and connect the ai-observability telemetry collector

I run an AI-coding-efficiency observability system across my laptops. The main laptop
already collects; this laptop must join it. Everything is documented in the repo you
will clone — trust the README and the spec there, verify every step, and do NOT touch
anything outside what these steps require.

Source repo: https://github.com/jeremycod/ai-observability (private; `gh` is
authenticated on this machine — if not, stop and ask me to `gh auth login`).
Aggregator: http://10.0.0.207:4398 (staging receiver, already running, open mode —
no token). Aggregate dashboard: http://10.0.0.207:4399.

### Step 0 — preconditions (verify, report, stop if failing)
1. `claude --version` works (this laptop runs Claude Code already).
2. My older tool exists: `ls ~/.local/share/cc-usage/cc_usage.py` and
   `cat ~/.local/bin/cc-usage` (a shim). If EITHER is missing, stop and tell me —
   the installer preserves/upgrades that v1 install and I need to decide how to
   bootstrap a machine without it.
3. `git`, `python3 --version` (≥3.9), `gh auth status` succeed.

### Step 1 — clone and install
```bash
git clone https://github.com/jeremycod/ai-observability.git ~/ai-observability
cd ~/ai-observability && ./install/install.sh
```
The installer runs the test suite, proves the migration on a copy of the local DB,
backs up the v1 module and DB, installs to `~/.local/share/cc-usage`, and starts a
collector. Read its output; if any gate fails, show me the error — do not improvise
around a red gate.

### Step 2 — verify local capture
1. `cc-usage doctor` — all lines must be OK.
2. `cc-usage device` — prints this machine's device id; note it.
3. Run any trivial Claude prompt or wait for the next sweep, then
   `sqlite3 -readonly ~/.local/share/cc-usage/usage.db "SELECT COUNT(*), MAX(record_ts)
   FROM api_requests WHERE device_id IS NOT NULL"` — rows must exist and the newest
   timestamp must be recent (this machine is now capturing its own sessions).

### Step 3 — connect to the aggregator
```bash
echo 'export CC_USAGE_SYNC_URL=http://10.0.0.207:4398' >> ~/.zshrc
source ~/.zshrc
cc-usage sync push        # first push = full local history; then incremental
cc-usage sync status      # watermarks recorded
```
(A token is NOT needed — the receiver runs open mode on my private network.)

### Step 4 — verify aggregation (from this laptop)
1. `curl -s http://10.0.0.207:4399/api/devices` — must list TWO device ids: this
   laptop's and the main laptop's.
2. `curl -s "http://10.0.0.207:4399/api/summary?days=30&device=<this-device-id>"`
   must return this laptop's numbers, and `device=all` must be the union.

### Step 5 — report back
Print: device id, doctor verdict, pushed row counts, the two device ids on the
aggregate, and anything that deviated from this script.

### Constraints
- Do not modify anything under my AlphaFolio repositories.
- Do not store or transmit prompt/assistant text anywhere (the system never does —
  keep it that way).
- Do not create tickets, PRs, or commits to any repo except nothing at all is needed
  for this task (the repo clone stays local).
- If `~/.zshrc` is not my shell config, ask which rc file to use instead of guessing.
