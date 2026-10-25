"""Provider identity, model canonicalization, request identity, and config snapshots.

Spec v3.1 §3 (request identity contract) and §4 (four provider dimensions).
Privacy: config snapshots capture an ALLOWLIST of non-secret env keys only.
"""
import hashlib
import os

# (gateway, host substring, billing provider). Order matters: first match wins.
GATEWAYS = (
    ("zai", "api.z.ai", "zai-plan"),
    ("openrouter", "openrouter.ai", "openrouter-api"),
    ("deepseek", "api.deepseek.com", "deepseek-api"),
    ("anthropic", "api.anthropic.com", "anthropic-subscription"),
    ("aws-bedrock", "bedrock", "aws-org"),
)

# model-id prefix -> provider_inferred. NEVER treated as authoritative; kept out of
# authoritative comparisons (spec §4).
MODEL_INFERRED = (
    ("glm", "zai"),
    ("deepseek", "deepseek"),
)

# Allowlisted env keys captured into config_snapshots. Credential-bearing variables
# (AUTH_TOKEN, API_KEY, ...) are structurally excluded; test_config_snapshots enforces it.
ENV_ALLOWLIST = (
    "ANTHROPIC_BASE_URL",
    "ANTHROPIC_MODEL",
    "ANTHROPIC_DEFAULT_OPUS_MODEL",
    "ANTHROPIC_DEFAULT_SONNET_MODEL",
    "ANTHROPIC_DEFAULT_HAIKU_MODEL",
    "CLAUDE_CODE_SUBAGENT_MODEL",
    "CLAUDE_CODE_AUTO_COMPACT_WINDOW",
    "CLAUDE_CODE_EFFORT_LEVEL",
    "CLAUDE_EFFORT",
    "CLAUDE_CODE_ENTRYPOINT",
    "API_TIMEOUT_MS",
    "CC_LAUNCHER",
    "CC_PROVIDER",
)


def normalize_base_url(url):
    if not url:
        return None
    u = str(url).strip()
    for prefix in ("https://", "http://"):
        if u.startswith(prefix):
            u = u[len(prefix):]
    return u.rstrip("/")


def gateway_from_base_url(url):
    """Returns (gateway_name, raw_normalized_or_None). Unknown URLs stay 'unmapped'."""
    n = normalize_base_url(url)
    if not n:
        return (None, None)
    for name, host, _billing in GATEWAYS:
        if host in n:
            return (name, n)
    return ("unmapped", n)


def billing_for(gateway):
    for name, _host, billing in GATEWAYS:
        if name == gateway:
            return billing
    return None


def canonical_model(model):
    """'glm-5.3-flash[1m]' -> ('glm-5.3-flash', '1m'); the [1m] suffix is a context-window
    variant marker, not a distinct model (spec §4)."""
    if not model:
        return (None, None)
    m = str(model)
    if m.endswith("[1m]"):
        return (m[:-4], "1m")
    return (m, None)


def usage_fp(usage):
    """Normalized usage fingerprint: integers with null==0 (spec §3.3)."""
    if not isinstance(usage, dict):
        return "0/0/0/0"
    def n(k):
        v = usage.get(k)
        if v is None or v == "":
            return 0
        try:
            return int(v)
        except (TypeError, ValueError):
            return 0
    return "%d/%d/%d/%d" % (n("input_tokens"), n("output_tokens"),
                            n("cache_read_input_tokens"), n("cache_creation_input_tokens"))


def identity_key(message_id, session_id, source_file, byte_offset):
    """'mid:<provider message id>' or deterministic surrogate. The surrogate excludes
    timestamps so it is stable across file rewrites (spec §3.4)."""
    if message_id:
        return "mid:" + str(message_id)
    basis = "%s|%s|%d" % (session_id or "", source_file, byte_offset)
    return "sur:" + hashlib.sha256(basis.encode("utf-8")).hexdigest()


def order_key(record_ts, source_file, byte_offset):
    """Deterministic total order for duplicate resolution, valid ACROSS files
    (byte offsets are file-local and never compared directly — spec §3.1)."""
    return (record_ts or "", str(source_file), int(byte_offset))


def provider_inferred(model):
    if not model:
        return None
    for prefix, provider in MODEL_INFERRED:
        if str(model).startswith(prefix):
            return provider
    return None


def config_snapshot_from_env(env=None, session_id=None, cli_version=None, source="hook"):
    env = os.environ if env is None else env
    snap = {"session_id": session_id, "ts": None, "cli_version": cli_version, "source": source}
    for key in ENV_ALLOWLIST:
        if key in env and env[key] != "":
            snap[key] = env[key]
    snap["orca_pane_key_present"] = 1 if env.get("ORCA_PANE_KEY") else 0
    gw, raw = gateway_from_base_url(env.get("ANTHROPIC_BASE_URL"))
    snap["gateway"] = gw
    snap["base_url"] = raw
    return snap
