#!/usr/bin/env python3
"""Exhaustive Cresco tool suite: exercise EVERY capability-inventory tool against a live mesh and capture
the real request + real response for each, so the tools reference can document actual behavior.

Tools are categorized:
  read          - safe, no side effects; always exercised live
  mutate_safe   - reversible side effects (log level, tunnels, CEP); exercised live + cleaned up
  artifact      - needs a jar/pipeline artifact or scheduler state; exercised where a real target exists,
                  else the request shape is recorded with a note
  destructive   - kills/restarts a node; NEVER invoked live (would tear down the suite). Request shape +
                  effect are recorded from the descriptor.

Writes /tmp/tool-results.json ({tool: {category, target, request, response, note}}) and prints a summary.
"""
import os, sys, json, time
sys.path.insert(0, "/Users/cody/code/cresco/code/pycrescolib")
from pycrescolib.clientlib import clientlib
from pycrescolib.utils import decompress_param, json_deserialize
from mcp_tool_runner import CapabilityToolRunner

KEY = os.environ.get("CRESCO_KEY", "test-service-key-0001")

DESTRUCTIVE = {"cresco_agent_killjvm", "cresco_agent_stopcontroller", "cresco_agent_restartcontroller",
               "cresco_agent_restartframework", "cresco_agent_controllerupdate"}


def short(v, n=600):
    s = v if isinstance(v, str) else json.dumps(v)
    return s if len(s) <= n else s[:n] + "…(truncated)"


def readable(reply, binding):
    """Return a compact, human-readable view of a reply: decompress documented compressed params."""
    if not reply:
        return None
    out = {}
    comp = {r["name"] for r in (binding.get("returns") or []) if r.get("compressed")}
    for k, v in reply.items():
        if k.startswith("callId-") or k in ("ttl", "is_rpc", "routepath-global-controller"):
            continue
        if k in comp:
            try:
                out[k] = json_deserialize(decompress_param(v))
            except Exception:
                out[k] = short(v)
        else:
            out[k] = short(v)
    return out


def main():
    c = clientlib("localhost", 8282, KEY)
    if not c.connect():
        print("RESULT connect=fail"); return 1
    print("RESULT connect=ok")
    runner = CapabilityToolRunner(c)

    # slow tools whose reply legitimately takes longer than the default RPC timeout
    TIMEOUTS = {"cresco_sysinfo_getbenchmark": 60.0, "cresco_agent_getbroadcastdiscovery": 25.0}

    def safe_call(name, args, timeout=None):
        """Invoke a tool; if the WS connection drops (a slow call exceeding the client timeout poisons the
        socket), reconnect and retry so one slow tool can't cascade. Also reconnect after a null/timeout
        reply, since a timed-out RPC leaves the client socket in a refuse-to-send state."""
        to = timeout if timeout is not None else TIMEOUTS.get(name, 20.0)
        for attempt in range(2):
            try:
                r = runner.call_tool(name, dict(args), timeout=to)
                if r is None and attempt == 0:
                    try:
                        c.connect(); time.sleep(1)
                    except Exception:
                        pass
                return r
            except Exception as e:
                if attempt == 0 and "connection" in str(e).lower():
                    try:
                        c.connect(); time.sleep(1)
                    except Exception:
                        pass
                    continue
                raise

    inv = c.globalcontroller.get_capability_inventory(scope="global", include_plugins=True, include_osgi=True)
    tools = runner.load(inv)
    by_name = {t["name"]: t for t in tools}

    # ---- discover context: region/agent + plugin ids on the global ----
    gr = c.api.get_global_region(); ga = c.api.get_global_agent()
    plugins = {}   # pluginname -> pluginid, on the global agent
    try:
        r = c.messaging.global_controller_msgevent(True, "EXEC", {"action": "listplugins", "action_region": gr, "action_agent": ga})
        pl = json_deserialize(decompress_param(r["pluginslist"]))
        for p in pl.get("plugins", []):
            if p.get("pluginname"):
                plugins[p["pluginname"]] = p.get("name") or p.get("pluginid")
    except Exception as e:
        print("context plugin discovery error:", e)
    ctx = {"gr": gr, "ga": ga, "plugins": plugins}
    # region controller for exercising the 'regional' tier (fixed test topology brought up by tool_suite.sh)
    rr, rc = "edge-region", "edge-controller"
    print("RESULT context region=%s agent=%s region_ctrl=%s/%s plugins=%s" % (gr, ga, rr, rc, ",".join(plugins.keys())))

    # ---- per-tool args (only where params/routing are needed) ----
    def P(name):  # routing for a plugin tool: region/agent/pluginid on the global
        return {"region": gr, "agent": ga, "pluginid": plugins.get(name, "")}

    ARGS = {
        # global reads with params
        "cresco_global_listplugins": {"action_region": gr, "action_agent": ga},
        "cresco_global_listpluginsbytype": {"action_plugintype_id": "pluginname", "action_plugintype_value": "io.cresco.sysinfo"},
        "cresco_global_plugininfo": {"action_region": gr, "action_agent": ga, "action_plugin": plugins.get("io.cresco.sysinfo", "")},
        "cresco_global_pluginkpi": {"action_region": gr, "action_agent": ga, "action_plugin": plugins.get("io.cresco.sysinfo", "")},
        "cresco_global_resourceinfo": {"action_region": gr, "action_agent": ga},
        "cresco_global_getgpipelinestatus": {},
        # agent reads with routing
        "cresco_agent_listagents": {"region": gr, "agent": ga},
        "cresco_agent_getcontrollerstatus": {"region": gr, "agent": ga},
        "cresco_agent_iscontrolleractive": {"region": gr, "agent": ga},
        "cresco_agent_getbroadcastdiscovery": {"region": gr, "agent": ga},
        "cresco_agent_getagentinfo": {"region": gr, "agent": ga},
        "cresco_agent_getmetricinventory": {"region": gr, "agent": ga},
        "cresco_agent_getcapabilities": {"region": gr, "agent": ga},
        "cresco_agent_getcapabilityinventory": {"region": gr, "agent": ga},
        "cresco_agent_getislogdp": {"region": gr, "agent": ga, "session_id": "suite-sess"},
        # regional (route to the region controller, not the global node)
        "cresco_regional_getcapabilities": {"region": rr, "agent": rc},
        "cresco_regional_getcapabilityinventory": {"region": rr, "agent": rc},
        "cresco_regional_getmetricinventory": {"region": rr, "agent": rc},
        "cresco_regional_ping": {"region": rr, "agent": rc},
        # plugin reads
        "cresco_sysinfo_getsysinfo": P("io.cresco.sysinfo"),
        "cresco_sysinfo_getmetrics": P("io.cresco.sysinfo"),
        "cresco_sysinfo_getcapabilities": P("io.cresco.sysinfo"),
        "cresco_stunnel_listtunnels": P("io.cresco.stunnel"),
        "cresco_stunnel_getmetrics": P("io.cresco.stunnel"),
        "cresco_stunnel_getcapabilities": P("io.cresco.stunnel"),
        "cresco_wsapi_globalinfo": P("io.cresco.wsapi"),
        "cresco_wsapi_getmetrics": P("io.cresco.wsapi"),
        "cresco_wsapi_getcapabilities": P("io.cresco.wsapi"),
        "cresco_repo_repolist": P("io.cresco.repo"),
        "cresco_repo_getmetrics": P("io.cresco.repo"),
        "cresco_repo_getcapabilities": P("io.cresco.repo"),
        # agent reads (CONFIG-typed but read-only) + more
        "cresco_agent_pluginlist": {"region": gr, "agent": ga},
        "cresco_agent_pluginstatus": {"region": gr, "agent": ga, "pluginid": plugins.get("io.cresco.sysinfo", "")},
        "cresco_agent_getlog": {"region": gr, "agent": ga, "action_inmessage": "true"},
        # mutate_safe
        "cresco_agent_setloglevel": {"region": gr, "agent": ga, "session_id": "suite-sess", "loglevel": "Info", "baseclassname": "io.cresco.sysinfo"},
        "cresco_agent_setlogdp": {"region": gr, "agent": ga, "session_id": "suite-sess", "setlogdp": "false"},
    }

    # tools we exercise live (read + mutate_safe subset). Everything else is recorded shape-only.
    LIVE = set(ARGS.keys()) | {
        "cresco_global_listregions", "cresco_global_listagents", "cresco_global_listpluginsrepo",
        "cresco_global_listrepoinstances", "cresco_global_netresourceinfo", "cresco_global_resourceinventory",
        "cresco_global_plugininventory", "cresco_global_getmetricinventory", "cresco_global_getcapabilities",
        "cresco_global_getcapabilityinventory", "cresco_global_ping",
    }

    def category(name):
        if name in DESTRUCTIVE:
            return "destructive"
        if name in ("cresco_agent_setloglevel", "cresco_agent_setlogdp"):
            return "mutate_safe"
        if name in LIVE:
            return "read"
        return "artifact"

    results = {}
    for t in tools:
        name = t["name"]
        b = t["cresco_binding"]
        cat = category(name)
        entry = {"category": cat, "target": b["target"], "msg_type": b["msg_type"], "action": b["action"],
                 "description": t["description"], "input_schema": t["input_schema"], "returns": b.get("returns", [])}
        if cat == "destructive":
            entry["note"] = "NOT invoked live (would tear down the node). Request shape recorded from descriptor."
            entry["request"] = {"action": b["action"], **{p: "<...>" for p in b.get("routing_params", [])}}
            entry["response"] = None
        elif cat == "artifact":
            entry["note"] = "Requires a real artifact/scheduler target not provisioned by this suite; request shape recorded."
            entry["request"] = {"action": b["action"], **{p: "<...>" for p in b.get("routing_params", [])}}
            entry["response"] = None
        else:
            args = ARGS.get(name, {})
            entry["request"] = {"action": b["action"], **args}
            try:
                reply = safe_call(name, args)
                entry["response"] = readable(reply, b)
                entry["ok"] = bool(reply)
            except Exception as e:
                entry["response"] = {"error": str(e)}
                entry["ok"] = False
            time.sleep(0.15)
        results[name] = entry

    # ---- stunnel tunnel lifecycle (mutate_safe, needs create->status->remove) ----
    try:
        c.connect()  # ensure a healthy socket before the lifecycle sequence
    except Exception:
        pass
    st = plugins.get("io.cresco.stunnel")
    if st:
        try:
            import gzip, base64
            cfg = {"stunnel_id": "suite-tun", "src_port": "0", "dst_host": "127.0.0.1", "dst_port": "9",
                   "dst_region": gr, "dst_agent": ga, "dst_plugin": st}
            comp = base64.b64encode(gzip.compress(json.dumps(cfg).encode())).decode()
            r1 = c.messaging.global_plugin_msgevent(True, "CONFIG", {"action": "configsrctunnel", "action_tunnel_config": comp}, gr, ga, st, 12.0)
            results["cresco_stunnel_configsrctunnel"] = {"category": "mutate_safe", "target": "plugin", "msg_type": "CONFIG",
                "action": "configsrctunnel", "request": {"action": "configsrctunnel", "action_tunnel_config": "<gzip {stunnel_id,src_port,...}>"},
                "response": readable(r1, by_name["cresco_stunnel_configsrctunnel"]["cresco_binding"]), "ok": bool(r1)}
            r2 = c.messaging.global_plugin_msgevent(True, "EXEC", {"action": "gettunnelstatus", "action_stunnel_id": "suite-tun"}, gr, ga, st, 12.0)
            results["cresco_stunnel_gettunnelstatus"] = {"category": "read", "target": "plugin", "msg_type": "EXEC",
                "action": "gettunnelstatus", "request": {"action": "gettunnelstatus", "action_stunnel_id": "suite-tun"},
                "response": readable(r2, by_name["cresco_stunnel_gettunnelstatus"]["cresco_binding"]), "ok": bool(r2)}
            r2b = c.messaging.global_plugin_msgevent(True, "EXEC", {"action": "gettunnelconfig", "action_stunnel_id": "suite-tun"}, gr, ga, st, 12.0)
            results["cresco_stunnel_gettunnelconfig"] = {"category": "read", "target": "plugin", "msg_type": "EXEC",
                "action": "gettunnelconfig", "request": {"action": "gettunnelconfig", "action_stunnel_id": "suite-tun"},
                "response": readable(r2b, by_name["cresco_stunnel_gettunnelconfig"]["cresco_binding"]), "ok": bool(r2b)}
            r2c = c.messaging.global_plugin_msgevent(True, "EXEC", {"action": "tunnelhealthcheck", "action_stunnel_id": "suite-tun"}, gr, ga, st, 12.0)
            results["cresco_stunnel_tunnelhealthcheck"] = {"category": "read", "target": "plugin", "msg_type": "EXEC",
                "action": "tunnelhealthcheck", "request": {"action": "tunnelhealthcheck", "action_stunnel_id": "suite-tun"},
                "response": readable(r2c, by_name["cresco_stunnel_tunnelhealthcheck"]["cresco_binding"]), "ok": bool(r2c)}
            r3 = c.messaging.global_plugin_msgevent(True, "CONFIG", {"action": "removesrctunnel", "action_stunnel_id": "suite-tun"}, gr, ga, st, 12.0)
            results["cresco_stunnel_removesrctunnel"] = {"category": "mutate_safe", "target": "plugin", "msg_type": "CONFIG",
                "action": "removesrctunnel", "request": {"action": "removesrctunnel", "action_stunnel_id": "suite-tun"},
                "response": readable(r3, by_name["cresco_stunnel_removesrctunnel"]["cresco_binding"]), "ok": bool(r3)}
        except Exception as e:
            print("stunnel lifecycle error:", e)

    # ---- sysinfo getbenchmark last, on a fresh connection (slow SciMark run must not poison others) ----
    si = plugins.get("io.cresco.sysinfo")
    if si:
        try:
            c.connect(); time.sleep(1)
            rb = c.messaging.global_plugin_msgevent(True, "EXEC", {"action": "getbenchmark"}, gr, ga, si, 90.0)
            results["cresco_sysinfo_getbenchmark"] = {"category": "read", "target": "plugin", "msg_type": "EXEC",
                "action": "getbenchmark", "request": {"action": "getbenchmark", "region": gr, "agent": ga, "pluginid": si},
                "response": readable(rb, by_name["cresco_sysinfo_getbenchmark"]["cresco_binding"]), "ok": bool(rb)}
        except Exception as e:
            print("getbenchmark error:", e)

    with open("/tmp/tool-results.json", "w") as f:
        json.dump(results, f, indent=2)

    live = [n for n, e in results.items() if e.get("response") is not None]
    ok = [n for n, e in results.items() if e.get("ok")]
    print("RESULT total=%d live_exercised=%d ok=%d destructive=%d artifact=%d" % (
        len(results), len(live), len(ok),
        sum(1 for e in results.values() if e["category"] == "destructive"),
        sum(1 for e in results.values() if e["category"] == "artifact")))
    for n in sorted(ok):
        print("OK   " + n)
    for n, e in sorted(results.items()):
        if not e.get("ok") and e["category"] in ("read", "mutate_safe"):
            print("MISS " + n + " -> " + short(json.dumps(e.get("response")), 120))
    try: c.close()
    except Exception: pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
