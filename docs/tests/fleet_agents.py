#!/usr/bin/env python3
"""Print, as one JSON line, the agents a controller's registry lists (``listagents`` through its wsapi).

Usage: fleet_agents.py --port <wsapi port> --key <service key> [--pin <wsapi certificate fingerprint>] [--host 127.0.0.1]
                       [--ping <region>/<agent> ...]
Output: {"agents": ["<region>/<agent>", ...], "ping": {"<region>/<agent>": true|false}} or {"error": "..."}; exit 0 either
way (the caller judges). --ping sends each node's controller one RPC (iscontrolleractive) and reports whether it answered.
Needs pycrescolib on PYTHONPATH.
"""
import argparse
import json
import sys

from pycrescolib.clientlib import clientlib


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--key", required=True)
    ap.add_argument("--pin", default="")
    ap.add_argument("--ping", action="append", default=[])
    a = ap.parse_args()
    c = clientlib(a.host, a.port, a.key, fingerprint=a.pin) if a.pin else clientlib(a.host, a.port, a.key)
    try:
        if not c.connect():
            print(json.dumps({"error": "wsapi connect failed"}))
            return 0
        agents = c.globalcontroller.get_agent_list()
        names = sorted("%s/%s" % (x.get("region_id", x.get("region")), x.get("agent_id", x.get("agent"))) for x in agents)
        pings = {}
        for node in a.ping:
            region, agent = node.split("/", 1)
            try:
                r = c.messaging.global_agent_msgevent(True, "EXEC", {"action": "iscontrolleractive"}, region, agent, timeout=10.0)
                pings[node] = str((r or {}).get("is_controller_active", "")).lower() in ("true", "1")
            except Exception:
                pings[node] = False
        print(json.dumps({"agents": names, "ping": pings}))
    except Exception as e:  # reported, judged by the caller
        print(json.dumps({"error": "%s: %s" % (type(e).__name__, e)}))
    finally:
        try:
            c.close()
        except Exception:
            pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
