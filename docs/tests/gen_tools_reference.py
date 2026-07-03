#!/usr/bin/env python3
"""Generate docs/cresco-tools-reference.md from the live tool-suite results (/tmp/tool-results.json) plus
per-tool 'chain of events' prose. Exhaustive: every tool gets purpose, params, returns, chain of events,
examples, and a REAL captured request+response where the suite exercised it live.
"""
import json, os, datetime

RESULTS = "/tmp/tool-results.json"
OUT = "/Users/cody/code/cresco/docs/cresco-tools-reference.md"

# Fallback one-line descriptions for tools whose suite entry was built manually (no descriptor merged).
DESC = {
    "cresco_stunnel_configsrctunnel": "Create the source (listener) side of a TCP tunnel.",
    "cresco_stunnel_gettunnelstatus": "Get the live status of one tunnel (ACTIVE/RECOVERING/DOWN/UNKNOWN).",
    "cresco_stunnel_gettunnelconfig": "Get the full configuration of one tunnel.",
    "cresco_stunnel_tunnelhealthcheck": "Check whether a tunnel config exists on this node.",
    "cresco_stunnel_removesrctunnel": "Tear down the source (listener) side of a tunnel.",
    "cresco_sysinfo_getbenchmark": "Run and return a SciMark2 CPU benchmark result as compressed JSON.",
}

# Per-tool chain-of-events / what-it-interacts-with. Authored from the controller/plugin source.
CHAINS = {
 # ---- global tier (GlobalExecutor on the global controller; reports are fabric-wide) ----
 "cresco_global_region_enable": "GlobalExecutor.executeCONFIG → getGDB().nodeUpdate() registers the region node in the Derby global DB and clears stale region/agent configs. Sent automatically when a region federates; makes the region visible to placement and to listregions.",
 "cresco_global_region_disable": "GlobalExecutor → getGDB().removeNode() removes the region from the global DB. Sent on graceful region shutdown so the fabric view stays accurate.",
 "cresco_global_regionalimport": "Internal region onboarding — registers a regional node (mode=REGION) via getGDB().nodeUpdate().",
 "cresco_global_addplugin": "Schedules an iNode: getGDB().addINode() with status 0 (scheduled). The global scheduler (PollAddPipeline/ResourceScheduler) later emits a CONFIG pluginadd to the chosen agent, which drives AgentExecutor → PluginAdmin.addPlugin() (OSGi bundle install+start).",
 "cresco_global_removeplugin": "Marks an iNode 'scheduled for removal'; the scheduler drives PollRemovePipeline → a CONFIG pluginremove on the agent → PluginAdmin stops+uninstalls the bundle.",
 "cresco_global_gpipelinesubmit": "The main application-deploy path. GlobalExecutor creates a pipeline record (getGDB().createPipelineRecord) and queues it to AppScheduler, which resolves placement (buildNodeMaps) and fans out addplugin CONFIGs to agents across regions. Returns the assigned pipeline id. Interacts with: global DB, AppScheduler, every target agent's PluginAdmin.",
 "cresco_global_gpipelineremove": "Async undeploy via the PollRemovePipeline executor — tears down every iNode of the pipeline across all agents that host it.",
 "cresco_global_plugindownload": "Fetches a plugin JAR from a URL into the local repo (cache-checked unless forced). Interacts with the io.cresco.repo plugin and the HTTP source.",
 "cresco_global_setinodestatus": "Direct DB write: getGDB().setINodeParam() twice to set an iNode's status code + description. Lifecycle bookkeeping for a deployed plugin instance.",
 "cresco_global_savetorepo": "Broadcasts the JAR to EVERY repo instance in the fabric via RPC putjar (interacts with all io.cresco.repo plugins) so the artifact is replicated fabric-wide.",
 "cresco_global_listregions": "GlobalExecutor reads the global DB region list AND queries live inter-broker bridges (getConnectedRegions over the ActiveMQ NetworkConnectors). Reports BOTH registered regions and live bridge topology — this is the fabric-wide view of every region the global sees.",
 "cresco_global_listagents": "getGDB().getAgentList(region) reads agents from the global DB; for a non-local region it RPCs that region. GLOBAL scope = every agent in every region.",
 "cresco_global_listplugins": "getGDB().getPluginList(region,agent) reads plugin instances from the global DB, optionally filtered by region/agent.",
 "cresco_global_listpluginsbytype": "getGDB().getPluginListByType(key,value) — every plugin instance matching e.g. pluginname=io.cresco.repo, across the fabric.",
 "cresco_global_listpluginsrepo": "getGDB().getPluginListRepo() — the union of plugin artifacts available across all repos.",
 "cresco_global_listrepoinstances": "getGDB().getPluginListByType('pluginname','io.cresco.repo') — locates every repo server in the fabric.",
 "cresco_global_plugininfo": "getGDB().getPluginInfo(region,agent,plugin) — one plugin instance's full metadata/config.",
 "cresco_global_pluginkpi": "getPerfControllerMonitor().getIsAttachedMetrics(...) — the metrics attached to a specific plugin instance.",
 "cresco_global_resourceinfo": "getPerfControllerMonitor().getResourceInfo(region,agent) → RPCs each agent's io.cresco.sysinfo getsysinfo and aggregates cpu/mem/disk. GLOBAL scope aggregates across every agent it can reach.",
 "cresco_global_netresourceinfo": "getGDB().getNetResourceInfo() — network-wide resource totals from the global DB.",
 "cresco_global_getenvstatus": "Counts nodes whose environment index matches key=value — query fabric composition by attribute.",
 "cresco_global_getinodestatus": "getGDB().getInodeMap(inode_id) — the status map of one iNode (plugin instance).",
 "cresco_global_resourceinventory": "getGDB().getResourceTotal() — high-level fabric resource totals.",
 "cresco_global_plugininventory": "Scans the global node's local repo JAR directory (reads manifests) → a name=version list of artifacts held locally.",
 "cresco_global_getmetricinventory": "Delegates to PerfControllerMonitor.getMetricInventory(scope). scope=global fans out CONCURRENTLY to every region+agent, merging each node's controller Micrometer groups + every plugin's getmetrics + the sysinfo resource summary into one document.",
 "cresco_global_getgpipeline": "getGDB().getGPipeline(id) — a pipeline (distributed application) definition.",
 "cresco_global_getgpipelineexport": "getGDB().getGPipelineExport(id) — the pipeline in portable export format for re-deployment elsewhere.",
 "cresco_global_getgpipelinestatus": "getGDB().getPipelineInfo() — the status of one pipeline (or all pipelines if none specified).",
 "cresco_global_getisassignmentinfo": "getGDB().getIsAssignedInfo(inode,resource) — placement diagnostics for an iNode/resource pair.",
 "cresco_global_ping": "pingReply(): records the region's advertised rolled-up mesh health (MeshHealthPing.recordChild), stamps the global's own health back onto the reply, and returns pong + remote_ts. This is the region→global liveness/RTT link.",
 "cresco_global_getcapabilities": "CapabilityResponder scans GlobalExecutor's @CrescoAction annotations (bundle name+version from the OSGi manifest) → the global tier's own tool document.",
 "cresco_global_getcapabilityinventory": "CapabilityInventory: statically scans the three controller-tier executor classes, RPCs each local plugin's getcapabilities, and scans the OSGi surface. scope=global fans out to every region+agent → one fabric-wide LLM tool catalog.",
 # ---- agent tier (AgentExecutor on the target agent) ----
 "cresco_agent_pluginadd": "AgentExecutor → ControllerEngine.getPluginAdmin().addPlugin(): installs and starts an OSGi bundle (Felix), whose DS @Component wires a new Executor. Returns the assigned pluginid.",
 "cresco_agent_pluginremove": "PluginAdmin stops and uninstalls the plugin's OSGi bundle by pluginid.",
 "cresco_agent_pluginlist": "PluginAdmin enumerates the plugins currently loaded on THIS agent.",
 "cresco_agent_pluginstatus": "PluginAdmin.getPluginStatus(pluginid) — one local plugin's status code/desc.",
 "cresco_agent_pluginupload": "Writes an uploaded JAR to the agent's plugin dir and registers it (pluginUpdate), making it available to load.",
 "cresco_agent_pluginrepopull": "Validates a plugin set against the repo (pluginAdmin.remotePluginMap) and pulls any missing artifacts before deploy.",
 "cresco_agent_setloglevel": "Sets the SLF4J/logback level for a class (or globally) for a session. If data-plane logging is enabled for the session, matching log records also stream over the dataplane.",
 "cresco_agent_getislogdp": "Queries whether data-plane log streaming is enabled for a session (DataPlaneLogger).",
 "cresco_agent_setlogdp": "Enables/disables data-plane log streaming for a session — turns a live log feed on/off.",
 "cresco_agent_controllerupdate": "Stages a controller JAR for the next restart by writing conf/version.ini; the new JAR is swapped in on restartcontroller.",
 "cresco_agent_stopcontroller": "CoreState.stopController() — async graceful controller stop. DESTRUCTIVE: the node leaves the fabric.",
 "cresco_agent_restartcontroller": "CoreState.restartController() — async controller restart (state reload). DESTRUCTIVE mid-call.",
 "cresco_agent_restartframework": "CoreState.restartFramework() — async full OSGi framework restart. DESTRUCTIVE.",
 "cresco_agent_killjvm": "CoreState.killJVM() — async hard stop of the agent JVM process. DESTRUCTIVE.",
 "cresco_agent_cepadd": "DataPlaneService.createCEP() installs a Siddhi Complex-Event-Processing query over the named dataplane input stream, emitting to an output stream. Returns the cepid.",
 "cresco_agent_getagentinfo": "Returns the agent's cresco_data_location (on-disk data directory) from config.",
 "cresco_agent_getlog": "Reads cresco-data/cresco-logs/main.log — compressed inline when action_inmessage=true, else attached as a file for chunked transfer.",
 "cresco_agent_getfileinfo": "Returns md5 + size for a file on the agent (status 10=found / 9=not found) — the first step of a chunked file transfer.",
 "cresco_agent_getfiledata": "Seeks skiplength and reads partsize bytes of a file → a compressed payload chunk. Used to stream large files in parts.",
 "cresco_agent_getcontrollerstatus": "Reads the ControllerState FSM → the integer state code.",
 "cresco_agent_iscontrolleractive": "Boolean readiness check on the controller FSM.",
 "cresco_agent_getbroadcastdiscovery": "Triggers a UDP/TCP broker discovery scan (PerfMonitorNet Broker Search) and returns the discovered list. SLOW (~10s) — the scan blocks; give the call a generous timeout.",
 "cresco_agent_listagents": "Lists the agents in THIS agent's region (agent-local view; the global tier lists the whole fabric).",
 "cresco_agent_getmetricinventory": "Node-scoped metric inventory (this node only, no fan-out). The global/region getmetricinventory fan-out calls exactly this on each agent.",
 "cresco_agent_getcapabilities": "Scans AgentExecutor's annotations → the agent tier's own tool document.",
 "cresco_agent_getcapabilityinventory": "This node's node-scoped capability catalog (controller tiers + local plugins + OSGi). The global/region fan-out calls this on each agent.",
 # ---- regional tier (RegionalExecutor on a region controller) ----
 "cresco_regional_agent_enable": "RegionalExecutor.executeCONFIG → getGDB().nodeUpdate() registers an agent in THIS region's registry (region-scoped, vs the global fabric registry).",
 "cresco_regional_agent_disable": "getGDB().removeNode() unregisters an agent from this region.",
 "cresco_regional_ping": "Agent→region liveness: records the agent's advertised health (MeshHealthPing), stamps the region's health back, replies pong. The region-scoped analogue of the region→global ping.",
 "cresco_regional_getmetricinventory": "REGION-scoped metric inventory — this region's node view. Same action as the global one but scoped to one region rather than aggregating the whole fabric (delegated to the region's GlobalExecutor). Primarily reached via the global fan-out, which addresses region controllers as children.",
 "cresco_regional_getcapabilities": "The regional tier's own tool document (RegionalExecutor annotations).",
 "cresco_regional_getcapabilityinventory": "REGION-scoped capability catalog — this region's nodes. The single-region counterpart to the global fabric-wide catalog; reached via the global fan-out (region controllers appear as children).",
 # ---- sysinfo plugin ----
 "cresco_sysinfo_getsysinfo": "SysInfoBuilder.getSysInfoMap() reads OSHI (JNA-native) → a full OS/CPU/memory/disk/filesystem/network snapshot as compressed JSON.",
 "cresco_sysinfo_getbenchmark": "Benchmark runs the SciMark2 CPU suite → a compute score. SLOW (seconds) — run it in isolation with a long timeout.",
 "cresco_sysinfo_getmetrics": "SysInfoBuilder.getMetricsJson() → MeasurementEngine gauges (cpu/mem/disk/net + sensors/power/gpu/devices) grouped by group. Folded into getmetricinventory.",
 "cresco_sysinfo_getcapabilities": "Scans sysinfo's annotations → its tool document.",
 # ---- stunnel plugin ----
 "cresco_stunnel_configsrctunnel": "SocketController.startSrcTunnel opens a local ServerSocket listener bound to src_port; on each client connection it opens a fabric-spanning channel to the paired dst tunnel and pumps bytes. Interacts with: the dst agent's stunnel (via configdstsession), the local TCP client.",
 "cresco_stunnel_configdsttunnel": "Registers the destination side (the agent nearest the target host:port); it dials the real service when a session opens.",
 "cresco_stunnel_configdstsession": "Per-connection dst setup — SocketController.createDstSession connects to the target host:port for one client session.",
 "cresco_stunnel_removesrctunnel": "SocketController.removeSrcTunnel closes the listener + all channels for the src tunnel.",
 "cresco_stunnel_removedsttunnel": "SocketController.removeDstTunnel closes the dst side.",
 "cresco_stunnel_nettuning": "SocketController.applyNetTuning applies buffer/block sizes pushed by the controller AutoTuner to live tunnels.",
 "cresco_stunnel_tunnelhealthcheck": "Reads SocketController state — is a config present for this tunnel id.",
 "cresco_stunnel_listtunnels": "SocketController.getActiveTunnels() — all tunnels + live status.",
 "cresco_stunnel_gettunnelstatus": "SocketController.getTunnelStatus(id) — ACTIVE (listener open) / RECOVERING (down, reconnect scheduled) / DOWN / UNKNOWN.",
 "cresco_stunnel_gettunnelconfig": "SocketController.getTunnelConfig(id) — the tunnel's src/dst wiring + tuning.",
 "cresco_stunnel_getmetrics": "SocketController metrics → active tunnels/clients/targets gauges.",
 "cresco_stunnel_getcapabilities": "Scans stunnel's annotations → its tool document.",
 # ---- wsapi plugin ----
 "cresco_wsapi_nettuning": "NettyWsServer.applyNetTuning applies socket-buffer/read-chunk/write-high-water sizes to the live wss server; new connections read the updated values.",
 "cresco_wsapi_globalinfo": "Reads AgentState → the global controller's region+agent identity.",
 "cresco_wsapi_getmetrics": "MeasurementEngine gauges: active dataplane WebSocket connections, total bytes, total messages.",
 "cresco_wsapi_getcapabilities": "Scans wsapi's annotations → its tool document.",
 # ---- repo plugin ----
 "cresco_repo_repolist": "Scans the repo directory + PluginBuilder.getPluginInventory → {plugins:[{pluginname,md5,jarfile}], server:[{region,agent,pluginid}]}. The catalog of deployable artifacts this repo holds.",
 "cresco_repo_getjar": "Looks up a jar by pluginname+md5 in the inventory and returns its bytes — the artifact-fetch path used during deployment.",
 "cresco_repo_putjar": "Writes an uploaded jar to disk and verifies its md5 — the artifact-publish path (savetorepo calls this on every repo).",
 "cresco_repo_getmetrics": "MeasurementEngine gauges: repo plugin count + total bytes on disk.",
 "cresco_repo_getcapabilities": "Scans repo's annotations → its tool document.",
}

NS_INTRO = {
 "global": "**Global tier** — routed to the **global controller** (`global_controller_msgevent`). These are the fabric-wide control API: registry, discovery, resource inventory, and global application (pipeline) scheduling. **Global-tier reports aggregate across every region the global sees** — e.g. `listregions`/`listagents`/`getmetricinventory scope=global` return the whole fabric, not one region.",
 "agent": "**Agent tier** — routed to a specific **agent controller** (`global_agent_msgevent` with region+agent). Agent-local operations: manage plugins on that node, controller lifecycle, logs/files, CEP rules, and node-scoped queries.",
 "regional": "**Regional tier** — answered by a **region controller** (`RegionalExecutor`). These mirror global/agent actions but are **scoped to a single region**. Same action name as a global tool ≠ duplicate: the global version aggregates all regions, the regional version reports one region. Region-tier inventories are normally reached via the global `getcapabilityinventory`/`getmetricinventory` fan-out (region controllers appear as children).",
 "sysinfo": "**sysinfo plugin** — host telemetry: OS/CPU/memory/disk/network snapshot, CPU benchmark, and unified live metrics. Routed with `global_plugin_msgevent(region, agent, pluginid)`.",
 "stunnel": "**stunnel plugin** — secure TCP tunnels across the fabric (forward a local port to a remote host:port via a src/dst tunnel pair). Routed with `global_plugin_msgevent(region, agent, pluginid)`.",
 "wsapi": "**wsapi plugin** — the WebSocket gateway external clients connect through (control-plane RPC + dataplane streaming). Routed with `global_plugin_msgevent(region, agent, pluginid)`.",
 "repo": "**repo plugin** — the plugin-artifact repository: list/serve/store plugin JARs for the fabric. Routed with `global_plugin_msgevent(region, agent, pluginid)`.",
}

NS_ORDER = ["global", "agent", "regional", "sysinfo", "stunnel", "wsapi", "repo"]


def ns_of(name):
    return name.split("_")[1]


def params_table(schema):
    props = (schema or {}).get("properties", {})
    req = set((schema or {}).get("required", []))
    if not props:
        return "_none_"
    rows = ["| param | type | required | description |", "|---|---|---|---|"]
    for k, v in props.items():
        rows.append(f"| `{k}` | {v.get('type','string')} | {'yes' if k in req else 'no'} | {v.get('description','')} |")
    return "\n".join(rows)


def returns_table(rets):
    if not rets:
        return "_none / status only_"
    rows = ["| return param | type | compressed | description |", "|---|---|---|---|"]
    for r in rets:
        rows.append(f"| `{r['name']}` | {r.get('type','string')} | {'yes' if r.get('compressed') else 'no'} | {r.get('description','')} |")
    return "\n".join(rows)


def py_example(name, e):
    b = e
    tgt = e["target"]
    action = e["action"]
    schema = e.get("input_schema", {})
    props = list((schema or {}).get("properties", {}).keys())
    if tgt == "global":
        return f'reply = client.messaging.global_controller_msgevent(True, "{e["msg_type"]}", {{"action": "{action}"}})'
    if tgt == "agent" or tgt == "regional":
        extra = "".join(f', "{p}": ...' for p in props if p not in ("region", "agent"))
        return f'reply = client.messaging.global_agent_msgevent(True, "{e["msg_type"]}", {{"action": "{action}"{extra}}}, region, agent)'
    # plugin
    extra = "".join(f', "{p}": ...' for p in props if p not in ("region", "agent", "pluginid"))
    return f'reply = client.messaging.global_plugin_msgevent(True, "{e["msg_type"]}", {{"action": "{action}"{extra}}}, region, agent, pluginid)'


def render_tool(name, e):
    desc = e.get("description") or DESC.get(name, "")
    cat = e.get("category", "read")
    out = [f"### `{name}`", ""]
    out.append(f"**Tier:** {e['target']} · **MsgEvent:** {e['msg_type']} · **Action:** `{e['action']}` · **Category:** {cat}")
    out.append("")
    if desc:
        out.append(desc.split("\n\n")[0])
        out.append("")
    if name in CHAINS:
        out.append(f"**Chain of events / interacts with:** {CHAINS[name]}")
        out.append("")
    out.append("**Parameters:**\n\n" + params_table(e.get("input_schema", {})) + "\n")
    out.append("**Returns:**\n\n" + returns_table(e.get("returns", [])) + "\n")
    # tool-call example
    props = (e.get("input_schema") or {}).get("properties", {})
    example_args = {k: "…" for k in props}
    out.append("**Tool call:**\n\n```json\n" + json.dumps({"name": name, "input": example_args}, indent=2) + "\n```\n")
    out.append("**pycrescolib:**\n\n```python\n" + py_example(name, e) + "\n```\n")
    # real captured response
    if e.get("ok") and e.get("response") is not None:
        rj = json.dumps(e["response"], indent=2)
        if len(rj) > 1400:
            rj = rj[:1400] + "\n… (truncated)"
        out.append("**Live response** (captured by `tool_suite.py`):\n\n```json\n" + rj + "\n```\n")
    elif cat == "destructive":
        out.append("**Not exercised live** — destructive (would tear down the node). Effect: see chain above.\n")
    else:
        out.append("**Not exercised live** — requires an artifact/scheduler target not provisioned by the suite. Request shape and effect are documented above.\n")
    return "\n".join(out)


def main():
    with open(RESULTS) as f:
        results = json.load(f)
    stamp = os.environ.get("DOC_DATE", "2026-07-03")
    live = sum(1 for v in results.values() if v.get("ok"))

    L = []
    L.append("# Cresco Tools — Exhaustive Reference\n")
    L.append(f"> Generated from a live 3-node mesh by `run/tests/tool_suite.py` on {stamp}. "
             f"{len(results)} tools; {live} exercised live with real captured responses; the remainder "
             f"(mutating/destructive/artifact-dependent) are documented with request shape + chain of events.\n")
    L.append("""## 1. What a "tool" is

Every Cresco capability is a **message action** — a `MsgEvent` of a given `Type` (`EXEC`, `CONFIG`, …)
carrying an `action` parameter. An external client reaches Cresco only over the **WebSocket message bus**
(the `wsapi` plugin), so **every tool is a MsgEvent action** — there are no direct method calls. Each tool
here has a self-describing entry in the fabric's capability inventory (`getcapabilityinventory`), which is
directly consumable as an LLM tool definition (`name` / `description` / `input_schema`) plus a
`cresco_binding` block telling a runner how to build and route the MsgEvent.

**Tool name:** `cresco_<namespace>_<action>` (e.g. `cresco_global_listagents`).

## 2. Routing tiers (the `target`) and how a tool call becomes a MsgEvent

| target | reached with | handled by |
|---|---|---|
| `global`   | `global_controller_msgevent(is_rpc, type, payload)` | GlobalExecutor on the global controller |
| `agent`    | `global_agent_msgevent(is_rpc, type, payload, region, agent)` | AgentExecutor on that agent |
| `regional` | (region controller) — usually via the global fan-out | RegionalExecutor on a region controller |
| `plugin`   | `global_plugin_msgevent(is_rpc, type, payload, region, agent, pluginid)` | that plugin's Executor |

A tool call `{name, input}` becomes a MsgEvent by: taking the `cresco_binding` (`msg_type`, `target`,
`action`), pulling the routing identity (`region`/`agent`/`pluginid`) out of `input`, and putting the
rest of `input` as action params. The reply is a MsgEvent whose params carry the result.

## 3. Scope: global vs regional are NOT duplicates

Several actions exist on both the **global** and **regional** tiers with the same name. They differ by
**scope**: the **global** tier aggregates across **every region it sees** (a fabric-wide report); the
**regional** tier reports a **single region**. E.g. `listregions`/`getmetricinventory scope=global` cover
the whole mesh, while a region controller answers only for its own region. Treat the tier as part of the
tool's identity.

## 4. Replies and compressed params

RPC replies are `MsgEvent` params. Large/structured values are **gzip+base64 compressed** (marked
*compressed* in the Returns tables) — read them with `decompress_param()` + `json_deserialize()` (Python)
or `getCompressedParam()` (Java). A `status` of `10` conventionally means success; `9` failure.

## 5. How the examples were captured

`run/tests/tool_suite.sh` brings up a global + region + agent mesh (with the sysinfo/stunnel/wsapi/repo
plugins auto-loaded on the global), then `tool_suite.py` invokes every read/safe tool via the MCP
tool-runner and records the real request + response to `/tmp/tool-results.json`. Destructive tools
(`killjvm`, `stopcontroller`, …) and artifact-dependent tools (needing a real JAR or pipeline) are
documented from their descriptor + source, not invoked live.

---
""")

    # group by namespace
    by_ns = {}
    for name, e in results.items():
        by_ns.setdefault(ns_of(name), {})[name] = e

    # TOC
    L.append("## 6. Tools by namespace\n")
    for ns in NS_ORDER:
        if ns in by_ns:
            L.append(f"- **{ns}** — {len(by_ns[ns])} tools")
    L.append("")

    for ns in NS_ORDER:
        if ns not in by_ns:
            continue
        L.append(f"\n---\n\n## {ns} tools\n")
        L.append(NS_INTRO.get(ns, ""))
        L.append("")
        for name in sorted(by_ns[ns].keys()):
            L.append(render_tool(name, by_ns[ns][name]))
            L.append("")

    with open(OUT, "w") as f:
        f.write("\n".join(L))
    print("wrote", OUT, "(%d tools, %d live)" % (len(results), live))


if __name__ == "__main__":
    main()
