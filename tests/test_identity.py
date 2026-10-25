import os
import unittest

import common  # noqa: F401 — sets sys.path + isolated CC_USAGE_HOME

from cc_usage.identity import (billing_for, canonical_model, config_snapshot_from_env,
                               gateway_from_base_url, identity_key, order_key,
                               provider_inferred, usage_fp)


class TestGatewayRegistry(unittest.TestCase):
    def test_known_gateways(self):
        self.assertEqual(gateway_from_base_url("https://api.z.ai/api/anthropic")[0], "zai")
        self.assertEqual(gateway_from_base_url("https://api.z.ai/api/anthropic/")[0], "zai")
        self.assertEqual(gateway_from_base_url("https://openrouter.ai/api")[0], "openrouter")
        self.assertEqual(gateway_from_base_url("https://api.deepseek.com/anthropic")[0], "deepseek")
        self.assertEqual(gateway_from_base_url("https://api.anthropic.com")[0], "anthropic")
        self.assertEqual(gateway_from_base_url("https://x.bedrock.amazonaws.com")[0], "aws-bedrock")

    def test_unknown_stays_unmapped_and_raw(self):
        name, raw = gateway_from_base_url("https://gateway.example.com/v1")
        self.assertEqual(name, "unmapped")
        self.assertEqual(raw, "gateway.example.com/v1")

    def test_billing_map(self):
        self.assertEqual(billing_for("zai"), "zai-plan")
        self.assertEqual(billing_for("deepseek"), "deepseek-api")
        self.assertIsNone(billing_for("unmapped"))


class TestCanonicalModel(unittest.TestCase):
    def test_ctx_variant_split(self):
        self.assertEqual(canonical_model("glm-5.3-flash[1m]"), ("glm-5.3-flash", "1m"))
        self.assertEqual(canonical_model("glm-5.3-flash"), ("glm-5.3-flash", None))

    def test_inferred_excludes_unknown(self):
        self.assertEqual(provider_inferred("glm-5.3-flash"), "zai")
        self.assertEqual(provider_inferred("deepseek-flash"), "deepseek")
        self.assertIsNone(provider_inferred("claude-sonnet-4"))  # could be bedrock: never infer


class TestFingerprintAndIdentity(unittest.TestCase):
    def test_usage_fp_null_equals_zero(self):
        a = usage_fp({"input_tokens": 5, "output_tokens": None,
                      "cache_read_input_tokens": 0, "cache_creation_input_tokens": ""})
        b = usage_fp({"input_tokens": 5, "output_tokens": 0,
                      "cache_read_input_tokens": None, "cache_creation_input_tokens": 0})
        self.assertEqual(a, b)

    def test_surrogate_is_rewrite_stable(self):
        k1 = identity_key(None, "s", "/a/file.jsonl", 123)
        k2 = identity_key(None, "s", "/a/file.jsonl", 123)
        self.assertEqual(k1, k2)
        self.assertTrue(k1.startswith("sur:"))

    def test_native_identity(self):
        self.assertEqual(identity_key("msg_1", "s", "/f", 0), "mid:msg_1")

    def test_order_key_cross_file(self):
        # ts dominates; file path only breaks ties; offsets never compared across files
        self.assertLess(order_key(1.0, "/a", 999999), order_key(2.0, "/b", 1))


class TestConfigSnapshotRedaction(unittest.TestCase):
    def test_secrets_never_captured(self):
        env = {"ANTHROPIC_AUTH_TOKEN": "sk-super-secret-value",
               "ANTHROPIC_API_KEY": "another-secret",
               "ANTHROPIC_BASE_URL": "https://api.z.ai/api/anthropic",
               "ANTHROPIC_MODEL": "glm-5.3-flash[1m]",
               "DEEPSEEK_API_KEY": "x", "GITHUB_TOKEN": "y"}
        snap = config_snapshot_from_env(env)
        self.assertEqual(snap["gateway"], "zai")
        self.assertEqual(snap["ANTHROPIC_MODEL"], "glm-5.3-flash[1m]")
        blob = repr(snap)
        for secret in ("super-secret", "another-secret", "AUTH_TOKEN", "API_KEY"):
            self.assertNotIn(secret, blob)

    def test_orca_presence_flag_only(self):
        env = {"ORCA_PANE_KEY": "pane-123"}
        snap = config_snapshot_from_env(env)
        self.assertEqual(snap["orca_pane_key_present"], 1)
        self.assertNotIn("pane-123", repr(snap))


if __name__ == "__main__":
    unittest.main()


class TestLegacyDelegation(unittest.TestCase):
    def test_statusline_and_reports_delegate(self):
        """Regression: argparse must never reject legacy subcommands (live bug 2026-10-25)."""
        import io
        import os
        from contextlib import redirect_stdout
        from cc_usage.app import main
        # stub the v1 module inside the isolated CC_USAGE_HOME so delegation is observable
        stub = os.path.join(os.environ["CC_USAGE_HOME"], "cc_usage_legacy_v1.py")
        with open(stub, "w") as f:
            f.write("import sys\n"
                    "def main():\n"
                    "    print('LEGACY-OK ' + ' '.join(sys.argv[1:]))\n"
                    "    return 0\n")
        for args in (["statusline"], ["today"], ["providers", "--period", "week"]):
            out = io.StringIO()
            with redirect_stdout(out):
                code = main(args)
            self.assertEqual(code, 0, "%s failed" % args)
            self.assertIn("LEGACY-OK", out.getvalue(), "%s did not delegate to v1" % args)
            self.assertNotIn("invalid choice", out.getvalue())
