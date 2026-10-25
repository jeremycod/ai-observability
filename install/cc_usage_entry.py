#!/usr/bin/env python3
"""cc-usage entrypoint installed at ~/.local/share/cc-usage/cc_usage.py.
Imports the v2 package; the v1 module is preserved beside it as
cc_usage_legacy_v1.py (rollback: restore it over this file)."""
import os
import sys

_here = os.path.dirname(os.path.abspath(__file__))
if _here not in sys.path:
    sys.path.insert(0, _here)

from cc_usage.app import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
