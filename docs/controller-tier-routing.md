# Controller tiers: what a global serves, what a regional serves

Status: implemented on branch `fix/routing-tiers` (from 1.3 `1d466ba`). The code-level source of truth is
`io.cresco.agent.controller.communication.ControllerTiers`; this page renders it. A unit test
(`ControllerTierRoutingTest.theTableCoversEveryDeclaredGlobalAndRegionalAction`) fails if an action declared by
`GlobalExecutor` / `RegionalExecutor` (`@CrescoAction`) is missing from the table or placed on the wrong tier.

Owner decision (2026-10-16), on whether a `global_controller_msgevent` addressed to a region id should run on that
region's controller: *"Generally no, we need to think about what should go to a region vs a real global controller.
There are some functions that globals need to serve, and other functions regionals need to serve. Make sure it is
going the right place."*

## The rule

A controller message (`global_controller_msgevent` / `regional_controller_msgevent`: the regional flag, optionally
the global flag, no destination plugin) carries a **function** (its `action`) and an **address**
(`region_id`/`agent_id`, default: the sender's own node).

**The function decides the tier. The address only picks the node inside a tier that has more than one.**

| Tier of the function | Global flag (`global_controller_msgevent`) | Regional flag (`regional_controller_msgevent`) |
|---|---|---|
| **GLOBAL** | runs on the real global, whatever the address | **rerouted** to the real global, whatever the address |
| **REGIONAL** | **refused** with an error, run nowhere | runs on the regional controller of the addressed region (an address naming a plain agent goes to that agent's regional controller) |
| **EITHER** | runs on the real global | runs on the addressed controller (a plain agent answers for itself; an agent's default-addressed message goes to its regional controller) |
| not a controller function (e.g. `pluginlist`) | the global answers with an error ("not a global-controller function") | the addressed node's agent executor (as before) |

Why refuse rather than reroute a regional function sent with the global flag: the caller asked *the global* for a
*region's* function (membership, region-local placement). Nothing in the message says which region was meant with
certainty, and these functions mutate region state, so guessing is worse than a clear error. The error text tells
the caller to send it as a regional controller message to the region's controller.

Why reroute a global function sent with the regional flag: there is exactly one node that can serve it (the
global), so the target is unambiguous. Before this change such messages fell through the RegionalExecutor to the
AgentExecutor ("Unknown configtype", RPC timeout).

Replies are never re-classified: a reply carries its caller's call id as destination and is routed back as before.

## The table

Only CONFIG and EXEC carry actions; the other message types keep flag routing (see below).

### GLOBAL: needs the global DB, schedulers, registry or repo index

| Type | Action | Why it is global |
|---|---|---|
| CONFIG | `region_enable` | region federation: registers a region in the global DB |
| CONFIG | `region_disable` | region federation: unregisters a region from the global DB |
| CONFIG | `regionalimport` | imports a region's state into the global DB |
| CONFIG | `addplugin` | global resource scheduler queue (iNode placement) |
| CONFIG | `removeplugin` | global resource scheduler queue (iNode removal) |
| CONFIG | `gpipelinesubmit` | global application scheduler (cross-region pipelines) |
| CONFIG | `gpipelineremove` | global application scheduler |
| CONFIG | `plugindownload` | global plugin repository cache |
| CONFIG | `setinodestatus` | iNode table of the global DB |
| CONFIG | `savetorepo` | fabric-wide repo replication from the global's repo index |
| EXEC | `listregions` | region registry (global DB) + the global's bridges |
| EXEC | `listplugins` | fabric-wide plugin rows in the global DB |
| EXEC | `listpluginsbytype` | fabric-wide plugin rows in the global DB |
| EXEC | `listpluginsrepo` | fabric-wide repo index |
| EXEC | `listrepoinstances` | fabric-wide repo index |
| EXEC | `plugininfo` | plugin rows of any region (global DB) |
| EXEC | `pluginkpi` | KPI rows of any region (global DB) |
| EXEC | `resourceinfo` | resource aggregates of any region (global DB) |
| EXEC | `netresourceinfo` | fabric resource totals (global DB) |
| EXEC | `resourceinventory` | fabric resource totals (global DB) |
| EXEC | `getenvstatus` | environment index over every node (global DB) |
| EXEC | `getinodestatus` | iNode table of the global DB |
| EXEC | `getisassignmentinfo` | iNode/resource assignment of the global scheduler |
| EXEC | `getgpipeline` | pipeline table of the global DB |
| EXEC | `getgpipelineexport` | pipeline table of the global DB |
| EXEC | `getgpipelinestatus` | pipeline table of the global DB |
| EXEC | `plugininventory` | the global node's plugin jar repository |

### REGIONAL: region membership and region-local placement (the region's DB and agents)

| Type | Action | Why it is regional |
|---|---|---|
| CONFIG | `agent_enable` | region membership: registers an agent in the region DB |
| CONFIG | `agent_disable` | region membership: unregisters an agent from the region DB |
| CONFIG | `rpipelinesubmit` | region-local placement on the region's agents (escalates cross-region work to a coordinator itself) |

### EITHER: both tiers answer, each with its own scope; the flag picks the tier

| Type | Action | Global answers | Regional answers |
|---|---|---|---|
| EXEC | `ping` | a region's liveness ping (global flag) | an agent's liveness ping (regional flag) |
| EXEC | `listagents` | any region, from the global DB | the region's own agents |
| EXEC | `getmetricinventory` | the global's inventory, fanned out by `action_scope` | the addressed controller's |
| EXEC | `gethealthinventory` | the global's health inventory | the addressed controller's |
| EXEC | `getcapabilities` | the global capability document | the addressed controller's |
| EXEC | `getcapabilityinventory` | fanned out by `action_scope` | the addressed controller's |
| EXEC | `getnetworkstate` | the global's view of the learned graph | the addressed controller's view |
| EXEC | `getcoordinators` | the coordinator set + consensus state | the addressed controller's view |

To ask one specific node for its node-scoped inventory, send an **agent** message
(`global_agent_msgevent(region, agent)`), as pycrescolib's `get_metric_inventory(dst_region, dst_agent)` does.
Agent and plugin messages are not controller messages and are routed by address only.

### Other message types (flag routing, unchanged)

| Type | Where it runs |
|---|---|
| WATCHDOG | the child-to-parent keep-alive: regional flag from an agent = its region, global flag from a region = the global |
| KPI | the global (a region forwards it when `forward_global_kpi`, default true) |
| INFO / DISCOVER / ERROR | no-ops at both controllers |

## Where it is enforced

1. `MsgRouter.route` (first hop, before cost/source routing): `MsgRouter.tierStep` applies the table.
   A refused message is answered with an error reply (`error` + `route_refused`) and never forwarded. A message for
   the global is re-addressed to this node and handed to this node's regional controller (the global runs it, a
   region forwards it to its global); at a node with no regional controller it goes to that node's regional
   controller. A regional-only function addressed to a plain agent goes to that agent's regional controller.
2. `RegionalExecutor.dispatch` (any node that is a regional controller): the same table decides between the
   GlobalExecutor (at the global, for any address), forwarding to the global (at a region), the RegionalExecutor
   (addressed here), or a refusal. A reply to this controller's own RPC is handed to the waiting call and never
   executed again.
3. `GlobalExecutor`: an action the global does not serve is answered with an error instead of a silent null.

## Special cases

- **Single-region fabric (one node is both regional and global).** GLOBAL functions run on its GlobalExecutor,
  REGIONAL functions on its RegionalExecutor, EITHER functions by flag. Nothing is "close enough": `agent_enable`
  with the global flag is refused there too.
- **A regional controller being rebuilt (global restart).** The router uses the engine's single RegionalExecutor
  (never null). A REGIONAL function still runs on the region (membership keeps working without a global). A
  function for the global, while the region has no global joined, is answered at once with
  `route unavailable ... no global controller joined` (not waited on, never run regionally). While the global's
  own schedulers are starting (GLOBAL_INIT) it is waited for up to `route_ready_wait_ms` (default 3000).
- **An agent re-initialising (regional controller unknown).** A function that must go to its regional controller
  waits up to `route_ready_wait_ms`, then gets `route unavailable ... regional controller not known`.
- **Liveness pings** that cannot be served are never answered with an error (a pinger reads any reply as a pong).

## Behaviour changes for callers

| Call | Before | Now |
|---|---|---|
| `global_controller_msgevent(<global fn>, region_id=R, agent_id=X)` with R not the sender's region | ran on X's AgentExecutor (usually "Unknown configtype", RPC timeout) | runs on the global |
| `global_controller_msgevent(...)` addressed to a plain agent | that agent's AgentExecutor | the global |
| `regional_controller_msgevent(<global fn>)` | RegionalExecutor fell through to the AgentExecutor (timeout) | the global |
| `global_controller_msgevent(agent_enable / agent_disable / rpipelinesubmit)` | forwarded to the global: "Unknown configtype", timeout | refused at once with an error |
| `global_controller_msgevent(<agent-tier action>)` | timeout at the global (or the addressed node's AgentExecutor) | error reply from the global |
| `regional_controller_msgevent(agent_enable, region_id=R, agent_id=<plain agent>)` | that agent's AgentExecutor ("Unknown configtype") | R's regional controller |

No client in the workspace (pycrescolib, clientlib, dashboard, wsapi, stunnel, gfs) sent a controller message whose
old landing spot changes for the worse; pycrescolib sends global functions with the default address and targets
specific nodes with agent messages.

Tests: `ControllerTierRoutingTest` (the real router + executors at a node in each controller mode),
`RoutingMatrixTest` (every origin/destination/flag/function cell over a G/R1/R2/agents fabric, plus the pinned map),
`ControllerTierBrokerTest` (global + region + agent over real embedded brokers and a duplex bridge, real RPCs).

# LOST node retention

A node (region, agent or plugin) that stops answering is marked STALE, then LOST (status 50), by its parent's
node-status watchdog; the global also marks a LOST region's agents and plugins LOST. A node that crashed and never
comes back stays listed as LOST for **`lost_node_retention_days`** (default **30**), counted from **when it went
LOST**, and is then deleted from that controller's DB with everything it held (a region with its agents and
plugins, an agent with its plugins).

Why 30 days: retention is an operations SLA, not a failure detector. The common reason a node stays down is
hardware: a failed server or edge device whose replacement parts have to be ordered, shipped and installed. That
takes days to weeks. A LOST row is what tells operators (dashboard, `listagents`, health inventory) that a node is
missing and what it was running; deleting it after hours or a few days would make a node with parts on order look
like it never existed. Thirty days covers a typical order-and-repair cycle with margin; after that a node that has
not returned is treated as decommissioned. A node that does return re-registers and is ACTIVE again whether or not
its row was purged.

How it is measured:

- Each node table (`rnode`, `anode`, `pnode`) has a `lost_ts` column: set by the first write of status 50, kept by
  every later write while the node stays LOST (watchdog re-marks, a parent's full-state update), cleared when the
  node is anything but LOST. It is never derived from `watchdog_ts` (a parent's full-state update rewrites that).
- It is a column of the row in the controller's Derby DB, so a controller restart neither resets nor shortens it.
- A database created before this change gets the column at start-up; LOST rows that have no `lost_ts` are stamped
  with the start-up time, so they get the full retention from the upgrade, never less.
- Each tier counts from when it recorded the loss (a region within one watchdog scan; the global on the next state
  update). The region's purge also reaches the global through the region's next full-state reconcile.
- The sweep runs from the region and global node-status watchdogs, at most once per
  `lost_node_sweep_interval_ms`, using the `status_code` index (cost grows with the LOST rows, not the fabric).
  A node never deletes its own rows.

## Configuration

| Key | Type | Default | Meaning |
|---|---|---|---|
| `lost_node_retention_days` | double | `30` | Days a LOST node stays listed before its rows are deleted, counted from when it went LOST. `0` or negative keeps LOST nodes forever. Fractions allowed (`0.5` = 12 h). |
| `lost_node_sweep_interval_ms` | long | `3600000` | Minimum time between two retention sweeps of one controller's DB. |
| `route_ready_wait_ms` | long | `3000` | How long the router waits for a target that is being rebuilt (global schedulers starting, regional controller unknown) before answering an RPC with a `route_unavailable` error. |

Tests: `LostNodeRetentionTest` (real Derby: stamping on every write path, kept across updates, cleared on return,
survives a controller restart, default 30 days, configurable / disabled, cascade, own rows kept, migration of a
pre-retention database).
