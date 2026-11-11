"""Live probe of "authority follows the tree" (authority_live_test.sh): from each node's gateway, as the same admin, send
the management action getagentinfo (direction DOWN, read-only) to every node, and one open action; print what came back.
Usage: authority_live_probe.py --key <service key> --node <name>=<region>/<agent>@<wsapi port>[#<fingerprint>] ...
Prints one JSON object: {"<gateway>-><target>": "ok" | "403: <status_desc>" | "error: ..."} and "open:<gateway>-><target>".
"""
import argparse
import json
import sys

from pycrescolib.clientlib import clientlib


def call(client, region, agent, action, msg_type):
    try:
        reply = client.messaging.global_agent_msgevent(True, msg_type, {"action": action}, region, agent, timeout=20.0)
    except Exception as e:                                  # a timeout or a dead socket is a result to report
        return f"error: {type(e).__name__}: {e}"
    if not isinstance(reply, dict):
        return f"error: reply {reply!r}"
    code = str(reply.get("status_code", reply.get("status", "")))
    if code == "403":
        return "403: " + str(reply.get("status_desc", ""))
    if reply.get("gateway_status"):
        return f"error: gateway {reply.get('gateway_status')}: {reply.get('status_desc')}"
    return "ok"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--key", required=True)
    ap.add_argument("--node", action="append", required=True)
    a = ap.parse_args()
    nodes = {}
    for spec in a.node:
        name, rest = spec.split("=", 1)
        where, port = rest.split("@", 1)
        port, _, pin = port.partition("#")
        region, agent = where.split("/", 1)
        nodes[name] = (region, agent, int(port), pin or None)
    out = {}
    for gw, (_, _, port, pin) in nodes.items():
        c = clientlib("127.0.0.1", port, a.key, fingerprint=pin) if pin else clientlib("127.0.0.1", port, a.key)
        try:
            if not c.connect():
                for target in nodes:
                    out[f"{gw}->{target}"] = "error: could not connect to the gateway"
                continue
            for target, (region, agent, _, _) in nodes.items():
                out[f"{gw}->{target}"] = call(c, region, agent, "getagentinfo", "CONFIG")
                out[f"open:{gw}->{target}"] = call(c, region, agent, "getcontrollerstatus", "EXEC")
        finally:
            c.close()
    json.dump(out, sys.stdout, sort_keys=True)
    print()


if __name__ == "__main__":
    main()
