#!/usr/bin/env python3
"""B-2 unified-metrics probe: connect to the global and pull the unified metric inventory.

Prints a compact per-source group summary and emits machine-readable RESULT lines the shell
harness greps. Exercises the Python client's new globalcontroller.get_metric_inventory().
"""
import os
import sys
import json

sys.path.insert(0, "/Users/cody/code/cresco/code/pycrescolib")
from pycrescolib.clientlib import clientlib

HOST = os.environ.get("CRESCO_HOST", "localhost")
PORT = int(os.environ.get("CRESCO_PORT", "8282"))
KEY = os.environ.get("CRESCO_KEY", "test-service-key-0001")


def groups_of(node_inv):
    """Return {source: [groups...]} for one node inventory dict."""
    out = {}
    for src, view in (node_inv.get("metrics_by_source") or {}).items():
        if isinstance(view, dict):
            out[src] = sorted(view.keys())
    return out


def main():
    import time
    client = clientlib(HOST, PORT, KEY)
    if not client.connect():
        print("RESULT connect=fail")
        return 1
    print("RESULT connect=ok")

    # whole-mesh unified inventory. Retry: the global's view of the mesh (regions/agents) propagates a
    # beat after each node registers, so poll until the fan-out reaches children (or give up after ~45s).
    inv = {}
    for attempt in range(15):
        inv = client.globalcontroller.get_metric_inventory(scope="global", include_plugins=True, include_resource=True)
        if inv and (inv.get("children") or {}):
            break
        time.sleep(3)
    if not inv:
        print("RESULT inventory=empty")
        return 1

    print("=== unified metric inventory (scope=global) ===")
    print("node:", inv.get("node"), "scope:", inv.get("scope"))

    # the global node itself
    top = groups_of(inv)
    for src, groups in top.items():
        print(f"  [self] {src} -> {groups}")
    all_sources = dict(top)
    all_groups = set(g for gs in top.values() for g in gs)

    # mesh children (region + agent nodes reached via fan-out)
    children = inv.get("children") or {}
    print(f"children (mesh nodes reached): {list(children.keys())}")
    for cname, cinv in children.items():
        if isinstance(cinv, dict):
            for src, groups in groups_of(cinv).items():
                print(f"  [{cname}] {src} -> {groups}")
                all_sources[src] = groups
                all_groups.update(groups)

    has_resource = "resource_summary" in inv or any(
        isinstance(c, dict) and "resource_summary" in c for c in children.values()
    )

    # controller-own unified groups (always present) + the newly un-stubbed role gauges
    controller_sources = [s for s in all_sources if s.endswith(":io.cresco.agent.controller")]
    plugin_sources = [s for s in all_sources if not s.endswith(":io.cresco.agent.controller")]

    print("RESULT sources=%d" % len(all_sources))
    print("RESULT controller_sources=%d" % len(controller_sources))
    print("RESULT plugin_sources=%d" % len(plugin_sources))
    print("RESULT groups=%s" % ",".join(sorted(all_groups)))
    print("RESULT children=%d" % len(children))
    print("RESULT has_jvm=%s" % ("1" if "jvm" in all_groups else "0"))
    print("RESULT has_controller_group=%s" % ("1" if "controller" in all_groups else "0"))
    print("RESULT has_role_gauge=%s" % ("1" if ("regional" in all_groups or "global" in all_groups) else "0"))
    print("RESULT has_resource_summary=%s" % ("1" if has_resource else "0"))
    print("RESULT has_wsapi=%s" % ("1" if "wsapi" in all_groups else "0"))
    print("RESULT has_sysinfo_group=%s" % ("1" if ("processor" in all_groups or "memory" in all_groups) else "0"))
    print("RESULT has_repo=%s" % ("1" if "repo" in all_groups else "0"))
    print("RESULT has_stunnel=%s" % ("1" if "stunnel" in all_groups else "0"))

    try:
        client.close()
    except Exception:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
