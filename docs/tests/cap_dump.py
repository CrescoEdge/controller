#!/usr/bin/env python3
"""Dump the live capability catalog + real sample replies to ground the tools reference doc."""
import os, sys, json
sys.path.insert(0, "/Users/cody/code/cresco/code/pycrescolib")
from pycrescolib.clientlib import clientlib
from mcp_tool_runner import CapabilityToolRunner

c = clientlib("localhost", 8282, os.environ.get("CRESCO_KEY", "test-service-key-0001"))
c.connect()
inv = c.globalcontroller.get_capability_inventory(scope="global", include_plugins=True, include_osgi=True)
runner = CapabilityToolRunner(c); tools = runner.load(inv)

out = {"tools": runner.tool_specs(), "bindings": {t["name"]: t["cresco_binding"] for t in tools}}
# sample replies from real calls (global-target, no routing)
samples = {}
def grab(name, args=None):
    try:
        r = runner.call_tool(name, args or {})
        # decompress/deserialize a couple known compressed fields for readability
        samples[name] = {k: (str(v)[:400]) for k, v in (r or {}).items()}
    except Exception as e:
        samples[name] = {"error": str(e)}
for n in ["cresco_global_listregions", "cresco_global_listagents", "cresco_global_listplugins",
          "cresco_global_resourceinfo", "cresco_global_getmetricinventory"]:
    grab(n)
out["samples"] = samples
out["osgi_sample"] = (inv.get("osgi") or [])[:3]
with open("/tmp/cap-dump.json", "w") as f:
    json.dump(out, f, indent=2)
print("tools=%d" % len(tools))
print("names=" + ",".join(sorted(t["name"] for t in tools)))
