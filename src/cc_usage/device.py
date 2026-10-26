"""Device identity (Phase G): stable, human-readable per-machine id used to tag every
synced row so aggregates can filter personal / work / everything.

Format: <short-hostname>-<8 hex of IOPlatformUUID hash>. Override with CC_USAGE_DEVICE
for labeling the aggregate (the machine hash stays stable underneath).
"""
import hashlib
import os
import socket
import subprocess


def _platform_uuid():
    try:
        out = subprocess.check_output(
            ["ioreg", "-rd1", "-c", "IOPlatformExpertDevice"],
            stderr=subprocess.DEVNULL, text=True, timeout=5)
        for line in out.splitlines():
            if "IOPlatformUUID" in line:
                return line.split('"')[-2]
    except Exception:  # noqa: BLE001 — non-mac hosts (staging receiver) fall back
        pass
    return ""


def device_id():
    host = (socket.gethostname() or "unknown").split(".")[0].lower()
    uuid = _platform_uuid()
    basis = uuid or "%s:%s" % (host, os.getuid())
    return "%s-%s" % (host, hashlib.sha1(basis.encode()).hexdigest()[:8])
