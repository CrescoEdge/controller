#!/usr/bin/env python3
"""Capability-inventory probe: pull the fabric tool catalog, validate it's LLM-tool-ready, and invoke an
action using ONLY its descriptor (via the MCP tool-runner). Emits RESULT lines the shell harness greps.
"""
import os
import sys
import json

sys.path.insert(0, "/Users/cody/code/cresco/code/pycrescolib")
from pycrescolib.clientlib import clientlib
from mcp_tool_runner import CapabilityToolRunner

HOST = os.environ.get("CRESCO_HOST", "localhost")
PORT = int(os.environ.get("CRESCO_PORT", "8282"))
KEY = os.environ.get("CRESCO_KEY", "test-service-key-0001")


def well_formed(tool):
    if not tool.get("name") or not tool.get("description"):
        return False
    s = tool.get("input_schema")
    return isinstance(s, dict) and s.get("type") == "object" and isinstance(s.get("properties"), dict) \
        and isinstance(s.get("required"), list)


def main():
    import time
    client = clientlib(HOST, PORT, KEY)
    if not client.connect():
        print("RESULT connect=fail")
        return 1
    print("RESULT connect=ok")

    inv = {}
    for _ in range(15):
        inv = client.globalcontroller.get_capability_inventory(scope="global", include_plugins=True, include_osgi=True)
        if inv and (inv.get("children") or {}):
            break
        time.sleep(3)
    if not inv:
        print("RESULT inventory=empty")
        return 1

    runner = CapabilityToolRunner(client)
    tools = runner.load(inv)
    specs = runner.tool_specs()

    # which namespaces / tiers are represented
    names = [t["name"] for t in tools]
    namespaces = sorted({n.split("_")[1] for n in names if n.startswith("cresco_")})
    all_wf = all(well_formed(t) for t in specs)
    osgi_present = "osgi" in inv or any(isinstance(c, dict) and "osgi" in c for c in (inv.get("children") or {}).values())

    print("=== capability tool catalog ===")
    print(f"tools={len(tools)} namespaces={namespaces}")
    # show one representative tool verbatim (proof of the exact LLM-ready shape)
    sample = next((t for t in specs if t["name"] == "cresco_global_listagents"), specs[0])
    print("sample tool:\n" + json.dumps(sample, indent=2)[:900])

    print("RESULT tools=%d" % len(tools))
    print("RESULT namespaces=%s" % ",".join(namespaces))
    print("RESULT all_well_formed=%s" % ("1" if all_wf else "0"))
    print("RESULT has_global=%s" % ("1" if any(n.startswith("cresco_global_") for n in names) else "0"))
    print("RESULT has_agent=%s" % ("1" if any(n.startswith("cresco_agent_") for n in names) else "0"))
    print("RESULT has_stunnel=%s" % ("1" if any(n.startswith("cresco_stunnel_") for n in names) else "0"))
    print("RESULT has_sysinfo=%s" % ("1" if any(n.startswith("cresco_sysinfo_") for n in names) else "0"))
    print("RESULT has_getcaps=%s" % ("1" if any(n.endswith("_getcapabilities") for n in names) else "0"))
    print("RESULT osgi_present=%s" % ("1" if osgi_present else "0"))

    # THE PROOF: invoke a described action using ONLY its descriptor/binding (no hand-written glue).
    invoked_ok = 0
    try:
        reply = runner.call_tool("cresco_global_listagents", {})
        if reply and ("agentslist" in reply):
            invoked_ok = 1
        print("invoked cresco_global_listagents from descriptor -> keys=%s" % (list(reply.keys()) if reply else None))
    except Exception as e:
        print("invoke error:", e)
    print("RESULT invoked_from_descriptor=%d" % invoked_ok)

    try:
        client.close()
    except Exception:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
