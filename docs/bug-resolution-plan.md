# Cresco — bug resolution plan

> **STATUS (verified in-tree 2026-07-02): 11 of 13 resolved; only B13 open.**
> - **Fixed in place (kept):** B1, B2 (`MsgEvent` src-identity ctors), B8 (`pNode.addRepos`).
> - **Deleted (dead + unwanted):** B3, B4, B5, B7, B9.
> - **Gone via rewrite/removal:** B6 + B12 (wsapi `repolist` gutted; one dangling `//` at
>   `PluginExecutor.java:78`), B10 (`WSInterface` Netty rewrite), B11 (`DBManager` removed).
> - **Open:** **B13** — stunnel `SocketControllerSM` decorative → `gettunnelstatus`/`listtunnels`
>   report a static `pluginActive`. Recommend *replace the status source* (report from real
>   `SocketController` state), not wiring the UMPLE model. Needs a keep/cut call.
>
> PR1 (Tier-1) and PR2 (Tier-2) below are **done**; PR3 is down to B13. See the four-bucket
> re-tagging of the wider Appendix A tail in `broken-and-untouched-report.md` §B-3, and the
> broker-auth security gap surfaced there as **B-7**.

Derived from the code-reference dead-code sweep (see `README.md` Appendix A). Each item was
**re-verified against live call paths** before ranking — several appendix entries were
overstated and are corrected here. Severity reflects *actual runtime reachability*, not just
"the code looks wrong."

Legend: **Fix** = repair in place (capability is wanted). **Delete** = remove (dead + unwanted;
the honest resolution for buggy code that never runs). Effort: S ≤30 min, M ≤half-day, L = design + impl + test.

| ID | Bug | Tier | Live path? | Rec. | Effort |
|----|-----|------|-----------|------|--------|
| B1 | `MsgEvent` 5-arg ctor drops region/agent/plugin/body | 1 | **Yes** (discovery, scheduler, perf) | Fix | M |
| B2 | `MsgEvent` 5-arg `Map` ctor drops region/agent/plugin | 1 | Yes (factory-adjacent) | Fix | S |
| B3 | `DBEngine.setPNodePersistenceCode` malformed SQL | 2 | No (no writers) | Fix or Delete | S |
| B4 | `PluginNode.setWatchDogTimer` no-op self-assign | 2 | No (caller commented) | Delete | S |
| B5 | `StreamInfo.setIdentId` assigns wrong field | 2 | No (no callers) | Fix or Delete | S |
| B6 | wsapi `contactMap.remove(indexOf(...))` throws | 2 | No (in stubbed repolist) | Delete w/ B12 | S |
| B7 | agent `Config.getConfigAsJSON` casts entries to String | 2 | No (no callers) | Delete | S |
| B8 | library `pNode.addRepos` buggy self-add | 2 | No (no callers) | Fix or Delete | S |
| B9 | `MsgEvent.getPathList` NPE (path-trace) | 2 | No (dead feature) | Delete feature | S |
| B10 | clientlib `WSInterface.serverListening` always true | 2 | Partial (dead branch) | Fix | S |
| B11 | Regional→global DB import (DBManager) unfinished | 3 | No (start block commented) | Delete or Implement | M/L |
| B12 | wsapi `repolist` action stubbed (`//todo fix`) | 3 | Yes (returns nothing useful) | Delete or Implement | M |
| B13 | stunnel `SocketControllerSM` never advanced | 3 | Yes (status always `pluginActive`) | Replace status source or Implement | L |

---

## Tier 1 — real latent bug on a live path

### B1 — `MsgEvent(Type, region, agent, plugin, body)` silently drops 4 of 5 args
- **Location:** `library/.../messaging/MsgEvent.java:80`.
- **Root cause:** the constructor body sets only `this.msgType` + empty maps; `msgRegion`,
  `msgAgent`, `msgPlugin`, `msgBody` are ignored. The 9-arg ctor (line 56) correctly assigns
  `this.src_region/src_agent/src_plugin`.
- **Verified impact:** live callers — `DiscoveryClientWorkerIPv4/IPv6`, `TCPDiscoveryStaticHandler`,
  `TCPDiscoveryEngineHandler`, `UDPDiscoveryEngine` (the whole discovery subsystem), plus
  `PollAddPipeline` and `PerfMonitorNet`. Every message they create has **null src identity**.
  It "works" today only because those paths carry identity in params (`discovery_node`, swapped
  src/dst params) rather than the header — i.e. it survives by luck, not design. Any future code
  that reads `getSrcRegion()/getSrcAgent()/getSrcPlugin()` on these messages gets null.
- **Resolution (Fix):** mirror the 9-arg ctor —
  `this.src_region = msgRegion; this.src_agent = msgAgent; this.src_plugin = msgPlugin;` and either
  drop the vestigial `msgBody` param or store it as a param (`setParam("desc", msgBody)`). Since
  callers pass `plugin.getRegion()/getAgent()/getPluginID()` they clearly *intend* these as src.
- **Risk:** low-but-nonzero. Currently these fields are null; after the fix they carry real identity.
  Confirm no handler branches on "src == null." `PollAddPipeline` passes all-nulls, so it is unaffected.
- **Verification:** unit-assert `getSrcRegion()` after construction; then the full discovery
  regression — agent→region→global bring-up + recovery (`run/tests/exhaustive_health_test.sh`) must
  stay green, since discovery is the heaviest consumer.

### B2 — the sibling `MsgEvent(Type, region, agent, plugin, Map params)` ctor drops identity too
- **Location:** `library/.../messaging/MsgEvent.java:88` (region/agent/plugin lines commented out).
- **Root cause / Resolution:** same defect, same fix, as B1 — set the three src fields. Ships in
  the same change as B1. **Effort S** once B1's pattern is established.

---

## Tier 2 — unreachable landmines (cheap; fix or delete)

These are broken but currently **never executed**. They are not causing failures; the value of
addressing them is removing traps and clarifying intent. Default recommendation is **delete** when
the capability is clearly unwanted, **fix** when the capability is plausibly wanted soon.

### B3 — `DBEngine.setPNodePersistenceCode` builds malformed SQL
- **Location:** `controller/.../agent/db/DBEngine.java:939` — `"UPDATE pnode SET persistence_code=" + persistence_code + "' " + …` (stray unmatched `'`).
- **Verified impact:** none at runtime — the only caller is `DBInterfaceImpl.setPNodePersistenceCode`,
  which itself has **no callers**. Persistence codes are written via `addPNode`/`updatePNode` instead.
  The *readers* (`getPNodePersistenceCode` in `StaticPluginLoader`, `PluginAdmin`) are live and fine.
- **Resolution:** **Delete** both `DBEngine.setPNodePersistenceCode` and the `DBInterfaceImpl`
  wrapper (dead). *If* a dedicated setter is wanted later, reintroduce it as a **parameterized**
  `PreparedStatement` (also fixes the project-wide SQL-injection smell of string-concatenated SQL).
- **Risk:** none (deleting dead code). **Verification:** compiles; grep confirms no callers.

### B4 — `PluginNode.setWatchDogTimer(long)` ignores its argument
- **Location:** `controller/.../agentcontroller/PluginNode.java:184` — body assigns the field to
  itself (`this.watchdog_period = watchdog_period`), never reading the `watchdogtimer` arg.
- **Verified impact:** none — the only caller (`PluginAdmin:1629`, `AgentEngine.pluginMap...`) is inside
  the commented-out `enablePlugin` path and references a non-existent map.
- **Resolution:** **Delete** the method (and the dead `enablePlugin`/`disablePlugin` blocks around
  the caller). **Risk:** none.

### B5 — `wsapi StreamInfo.setIdentId(String)` assigns to `identKey`
- **Location:** `wsapi/.../websockets/StreamInfo.java:66` — `this.identKey = identKey` in a method
  named `setIdentId`; there is no working setter for `identId`.
- **Verified impact:** none — no callers; `identId` is only ever set via the constructor.
- **Resolution:** **Delete** the setter (it is unused and wrong), or **Fix** to `this.identId = identId`
  if a setter is genuinely needed. Recommend delete.

### B6 — `wsapi PluginExecutor.getNetworkAddresses` throws on `contactMap.remove(indexOf(...))`
- **Location:** `wsapi/.../PluginExecutor.java:140` — `indexOf(serverMap)` on the freshly-built map
  returns `-1`, so `remove(-1)` throws (swallowed by the surrounding try).
- **Verified impact:** none — it lives inside the **stubbed `repolist`** action (B12).
- **Resolution:** resolve together with **B12** — if `repolist` is deleted, this goes with it; if
  implemented, replace with a correct removal (`contactMap.remove(serverMap)` after computing the
  local contact, or don't add it in the first place).

### B7 — agent `Config.getConfigAsJSON()` casts entries to `String`
- **Location:** `agent/.../main/Config.java:276` — iterates `entrySet()` casting each `Map.Entry`
  to `String` (would `ClassCastException`).
- **Verified impact:** none — no callers in the agent module; the correct implementation exists in the
  library/plugin `Config` copies.
- **Resolution:** **Delete** the agent copy (dead + wrong). **Risk:** none.

### B8 — library `pNode.addRepos(List)` adds to the wrong list
- **Location:** `library/.../app/pNode.java:36` — iterates the *parameter* and re-adds each entry to the
  parameter, never touching `this.repoServers`.
- **Verified impact:** none — no callers.
- **Resolution:** **Fix** to `this.repoServers.addAll(repoServers)` (one line, obviously intended) or
  **Delete** if repo-merge is unused. Recommend fix (cheap, plausibly wanted).

### B9 — `MsgEvent` path-trace (`getPathList` NPE) is a dead feature
- **Location:** `library/.../messaging/MsgEvent.java` — `isPathTrace/setIsPathTrace/addPath/getPathList`;
  `getPathList` NPEs on a null `pathList`.
- **Verified impact:** none — no callers anywhere.
- **Resolution:** **Delete** the four methods + the `pathList` field (dead feature). **Risk:** none.

### B10 — clientlib `WSInterface.serverListening()` always returns true
- **Location:** `clientlib/.../core/WSInterface.java:115` — both the success and the caught-exception
  paths `return true`, so the "server not listening" branch in `connect()` is dead.
- **Verified impact:** low — a genuinely-down server is caught later by the connect timeout, but the
  pre-check is useless and misleading.
- **Resolution:** **Fix** — return `false` in the exception path (and on a failed probe) so the branch
  works, or **Delete** the pre-check and rely on the connect timeout. Recommend fix.

---

## Tier 3 — incomplete / decorative features (product decision first)

These are not one-line bugs; each needs a keep-or-cut decision before implementation.

### B11 — regional→global DB import (`DBManager`) is unfinished and disabled
- **Location:** `controller/.../controller/db/DBManager.java` + the commented start block at
  `ControllerSMHandler.java:750-762`; sink `DBInterfaceImpl.setDBImport` is a log-and-return stub.
- **Correction to the appendix:** it does **not** silently discard imports — `importRegionalDB` has no
  callers and the manager thread is never started, so **nothing flows**. It is an unfinished feature.
- **Decision:**
  - **Delete (recommended):** remove `DBManager`, `importQueue`, `setDBImport`, `importRegionalDB`,
    and the commented start block. The fabric already propagates topology via the watchdog config
    rollup + the regional/global JMS listeners, so this path is redundant. Effort **M**.
  - **Implement:** wire `getDBExport → importQueue → setDBImport → DBEngine` and start the thread on
    region/global. Only if a full DB snapshot sync (beyond the incremental watchdog rollup) is wanted.
    Effort **L**, and it needs its own consistency/merge tests.

### B12 — wsapi `repolist` action returns nothing (`//todo fix repo list`)
- **Location:** `wsapi/.../PluginExecutor.java` — the `repolist` EXEC action's `setCompressedParam`
  and inventory lines are commented out; `getPluginInventory` (dead) and the B6 bug live here.
- **Verified impact:** a client calling wsapi `repolist` gets an empty/meaningless reply. Clients get
  repo data via the global controller / `repo` plugin directly, so this endpoint is largely redundant.
- **Decision:**
  - **Delete (recommended):** remove the `repolist` action + `getPluginInventory` + `getNetworkAddresses`
    (takes B6 with it). Effort **S–M**.
  - **Implement:** finish it to return the local jar inventory + contact info. Effort **M**. Only if a
    wsapi-native repo listing is actually needed.

### B13 — stunnel `SocketControllerSM` is decorative (never advanced)
- **Location:** `stunnel/.../state/SocketControllerSM.java` — all 12 transition methods
  (`incomingSrcTunnelConfig`, `srcFailure`, `recoveredTunnel`, …) are never invoked; only the initial
  `pluginActive` state is read via `getState().name()`.
- **Verified impact:** tunnels **work** (the `SocketController` runs its own health-check + reconnect
  logic independently), but the `gettunnelstatus` / `listtunnels` EXEC actions always report
  `pluginActive` regardless of real tunnel state — status reporting is **wrong/static**.
- **Decision:**
  - **Replace the status source (recommended, lower risk):** report tunnel status from the real
    `SocketController` state (active channels / health-check result / reconnect state) and either
    delete the SM or keep it only as documentation. Effort **M**.
  - **Wire the SM (higher fidelity):** fire the transition methods at the actual lifecycle points
    (config received, listener up, src/dst failure, recovery) so the SM tracks reality. Effort **L**;
    the generated "DO NOT EDIT" header means changes go through the UMPLE model, not the `.java`.

---

## Recommended sequencing

1. **PR 1 — Tier-1 correctness (`library`, `controller`):** B1 + B2. Ship together; gate on the
   discovery/recovery regression (`exhaustive_health_test.sh`). *Highest value.*
2. **PR 2 — Tier-2 dead-code cleanup (`library`, `controller`, `agent`, `wsapi`, `clientlib`):**
   B3, B4, B5, B7, B8, B9, B10 — mostly deletions + two one-line fixes (B8, B10). Zero runtime risk;
   removes every landmine and shrinks the surface the next reader has to reason about.
3. **PR 3 — Feature decisions:** B11, B12 (+B6 folds into B12), B13. Each needs your keep-or-cut call
   first; I recommend **delete B11 & B12** (redundant) and **replace the status source for B13**.

Only **B1/B2** change runtime behavior; everything in PR 2 is provably inert. Splitting them keeps the
behavior-changing PR small and independently reviewable/revertible.
