# Cresco — Broken & Untouched Report

**Date:** 2026-07-02
**Author:** Claude (Opus 4.8) working session
**Scope:** Known open issues ("what is broken") and deliberate non-changes ("what was
not changed") as of the methodical left-undone pass. This report covers **my** changes and
the standing known-issue inventory. It does **not** review the concurrent developer's
recent commits (TLS/Conscrypt, net metrics, dataplane sharding, live nettuning) — see the
caveat below.

---

## 0. Tree state at time of writing

| Repo       | Branch        | HEAD      | Head subject |
|------------|---------------|-----------|--------------|
| controller | felix-hygiene | `92e36d2` | net metrics: JMX backlog, capacity ceiling, federation probe, cost hook, live tuning broadcast |
| agent      | felix-hygiene | `12cef44` | agent: optional Conscrypt (BoringSSL) JCE provider for broker TLS |
| library    | felix-hygiene | `d5437cd` | dataplane: shard-aware send/subscribe on DataPlaneService |
| wsapi      | felix-hygiene | `52dcea5` | wsapi: live buffer/block tuning via 'nettuning' CONFIG |
| clientlib  | 1.3           | `6c2e385` | client: optional native BoringSSL (netty-tcnative) TLS |
| stunnel    | felix-hygiene | `29e4b37` | stunnel: live buffer/block tuning via 'nettuning' CONFIG |

**Caveat — active concurrent development.** Between my last review and this writing the tree
advanced significantly under another developer (native TLS via Conscrypt/BoringSSL, adaptive
net perf with parallel connections + dataplane sharding, JMX/federation net metrics, live
`nettuning` broadcast). **I have not reviewed, tested, or validated that work.** Statements
below about runtime behavior reflect the tree as of my testing; the new TLS/sharding commits
may have changed some of it (in particular issue **B-1**, the startup TLS race).

My own fixes are confirmed still in history and intact:
- `controller 44fa242` — JMS session-leak fix + plugin-health name labels (ancestor of HEAD;
  `getPluginNames` and the session-close guard both verified present in the working tree).
- `controller b6a880c` — graceful region shutdown (unregister-from-global + NPE reorder).
- `agent db1741f` — `AgentEngineShutdown` graceful-shutdown entrypoint + pidfile.

---

## 1. What is broken (open issues)

Ordered by severity. "Severity" is operational impact, not effort.

### B-1 — Transient SSL/PKIX handshake race on rapid same-host launch
- **STATUS: FIXED + PROVEN (2026-07-03).** The agent's first remote broker connect is now gated on a
  quiet **trust-ready TLS probe** so the ActiveMQ failover transport only ever touches a ready endpoint.
  New `ActiveClient.waitForBrokerTlsReady(host, port, maxWaitMs)` opens a bounded, DEBUG-level TLS
  handshake to the regional broker using the node's **current** key/trust managers (the same material
  the real connection uses; hostname verification off, matching `verifyHostName=false`) and returns the
  instant the endpoint trusts-and-handshakes; `ControllerSMHandler.initIOChannels` calls it before
  `initActiveAgentConsumer` on the remote (non-`vm://`) TLS path. Flags: `broker_tls_ready_probe`
  (default **true**), `broker_tls_ready_wait_ms` (default **15000**). Bounded by construction — if the
  endpoint never becomes ready it logs one WARN and falls through to the failover transport exactly as
  before (no worse than the old behavior); in the common non-raced case the first probe handshakes in
  milliseconds. Result: the failover transport no longer sees a failing first handshake, so the PKIX
  reconnect stack-trace spam is eliminated at the source. Proven by `run/tests/b1_startup_race_test.sh`
  4/4 (gate engages on the real first-connect path; flag toggles it off cleanly; **zero** PKIX/failover
  reconnect stacks; agent registers), with `tenant_isolation_mesh_test.sh` 10/10 and
  `regional_ca_mtls_test.sh` 10/10 confirming no regression on the secured/federated connect paths.
  **Scope note:** the region→global path is a broker-to-broker **bridge** (`static:()` in `ActiveBroker`),
  not a client connect through `initIOChannels`; its trust is exchanged during global discovery before
  the bridge starts, so it is out of scope for this gate and was not the B-1 locus.
- **Severity:** Low (self-healing) / Medium (log noise, false alarm potential).
- **Symptom:** On multi-tier bring-up on a single host, a freshly-launched agent's first
  ActiveMQ TLS connection to its regional broker (`nio+ssl://127.0.0.1:<brokerport>`) fails
  with `PKIX path building failed: unable to find valid certification path to requested
  target`, throwing full `FailoverTransport` reconnect stack traces into the log.
- **Evidence:** `run/logs/edge-agent-001.out` lines ~99–218 (PKIX/SSL failures) followed by
  line ~237 `Agent: edge-agent-001 registered with Region: edge-region`, then `link:parent`
  health CRITICAL → WARN → OK by ~18:38:15.
- **Root cause:** The agent attempts the TLS handshake before the regional broker's TLS
  transport / trust material is fully ready. This is a **startup timing race**, not a
  discovery-selection bug — the config uses *static* discovery
  (`regional_controller_host` + `regional_controller_port`), so no UDP broadcast is involved.
- **Current mitigation:** ActiveMQ `FailoverTransport` retries
  (`maxReconnectAttempts=5, initialReconnectDelay=5000`) and the agent registers within a few
  seconds; the mesh self-heals. The exhaustive/massive test scripts add a 6s launch `STAGGER`
  which largely hides it.
- **Why not fixed by me:** The proper hard-fix (graceful initial-connect backoff, or provision
  the truststore before first connect) lives in `controller/.../communication/ActiveClient.java`
  — a file under active concurrent edit (TLS work). Touching it would collide with the
  in-flight Conscrypt/BoringSSL changes, which may themselves alter this behavior.
- **Recommended fix (when TLS work settles):** Suppress/downgrade the first-N PKIX failures to
  DEBUG during the AGENT_INIT window, and/or gate the agent's first connect on a broker-ready
  probe. Re-test after the Conscrypt provider lands.

### B-2 — Metrics / measurements not unified (two parallel monitoring systems)
- **STATUS: FIRST INCREMENT DELIVERED (2026-07-02).** A unified cross-bundle metrics inventory now
  exists: `MeasurementEngine.getAllMetrics()` (library) enumerates a bundle's whole Micrometer view;
  the controller `getmetricinventory` EXEC merges the controller's own metrics
  (jvm/processor/netlink/controller) with each local plugin's metrics (pulled via a standard
  `getmetrics` EXEC — stunnel implemented as the pattern) and an optional resource summary. Validated:
  one query returns processor (`system.cpu.usage`), network (`netlink` + stunnel), JVM, controller and
  plugin metrics together. **Remaining:** roll `getmetrics` out to the other plugins; fold the legacy
  `getResourceInfo`/KPI path in (currently opt-in + slow); per-agent query via AgentExecutor.
- **Severity:** Architectural debt (no functional break).
- **Symptom:** The monitoring redesign (`docs/health-check-design.md`) split monitoring into
  health / measurements / benchmarks and migrated **health** onto Felix Health Checks. The
  **measurements** half was never migrated: it remains a separate Micrometer stack
  (`library/.../metrics/*`: `MeasurementEngine`, `CrescoMeterRegistry`, `CrescoReporter`,
  `CMetric`; controller `measurement/PerfControllerMonitor`, `PerfMonitorNet`).
- **Status:** This subsystem is **live and load-bearing**, not dead. `PerfControllerMonitor`
  is constructed at boot (`ControllerSMHandler.java:539`, guarded by `enable_controllermon`),
  and `GlobalExecutor` consumes `getIsAttachedMetrics` / `getResourceInfo` for resource-aware
  placement; KPIs persist through `DBInterfaceImpl.updateKPI`.
- **Why not fixed:** Unification touches the **public library API** (`MeasurementEngine`, used
  by downstream plugins), **global placement logic**, and the **DB KPI schema**. That is a
  design-doc-first project of the same size as the health migration — out of scope for a
  bug-squash pass and too risky to start unilaterally on a live tree.
- **Recommendation:** Spin up as its own phased effort with a design doc; do not fold into
  incidental changes.

### B-3 — "Dead-code long-tail": re-tagged into 4 buckets; enumerated-bug subset ~90% already resolved
- **STATUS: RE-CHARACTERIZED + VERIFIED IN-TREE (2026-07-02).** The "~80 items" was never one
  thing. Appendix A's "dead" means **"grep found no callers"** — a *mechanical* test that
  conflates four very different categories. A blanket 80-item purge is wrong because two of the
  four buckets are load-bearing (parked features and live-path bugs), and re-verification shows
  most of the *actionable* subset is already done. Severity: cleanliness for bucket A only.
- **Verified resolution status of the enumerated bug plan** (`docs/bug-resolution-plan.md`,
  B1–B13 — the re-verified subset of Appendix A). Checked against the working tree today:
  - **Fixed in place (kept — capability wanted):** B1 + B2 (`MsgEvent` src-identity ctors — a
    *live* discovery-path bug; now assigns src, `MsgEvent.java:78-88`); B8 (`pNode.addRepos` now
    merges into `this.repoServers`, `pNode.java:36-47`).
  - **Deleted (dead + unwanted):** B3 (malformed-SQL `setPNodePersistenceCode`), B4
    (`setWatchDogTimer` no-op), B5 (`StreamInfo.setIdentId` mis-assign), B7 (agent
    `Config.getConfigAsJSON`), B9 (`MsgEvent` path-trace). All confirmed absent.
  - **Gone via rewrite/removal:** B6 + B12 (wsapi `repolist` gutted — `getPluginInventory`,
    `getNetworkAddresses`, and the `contactMap.remove(indexOf())` bug all removed; one dangling
    `//` at `PluginExecutor.java:78` remains); B10 (`WSInterface` rewritten for Netty — the
    always-true `serverListening` branch no longer exists); B11 (`DBManager` regional→global
    import removed — only a comment in `DbHealthCheck` remembers it).
  - **RESOLVED (stunnel `ca6291a`, 2026-07-03 reconcile):** **B13** — the status source was replaced
    (the recommended fix). `SocketController.getTunnelStatus()` now returns real `ACTIVE` /
    `RECOVERING` / `DOWN` / `UNKNOWN` from live channel state, and `PluginExecutor`'s `gettunnelstatus`
    and `listtunnels` both call it (no more static `pluginActive`). The decorative `SocketControllerSM`
    was retired from the live path (no callers of its transition methods) and kept only as the UMPLE
    documentation model. Enumerated bug plan B1–B13 is now **13/13 resolved**.
- **The four buckets** (apply this lens to the remainder of Appendix A that is *not* an
  enumerated bug — do **not** treat them uniformly):
  - **A. Truly dead — opportunistic-delete only, low value.** Superseded variants / no-caller
    helpers: `MsgRouter.forwardToLocalGlobal2` (alt of the live `forwardToLocalGlobal`),
    `ControllerSMHandler` string helpers, `CIDRUtils.getNetworkAddress/getBroadcastAddress`,
    `PerformanceMonitor.bitsToMegabits`, `sysinfo.getSysInfoFullMap`. Remove only when a file is
    touched for another reason.
  - **B. Commented-out feature scaffolding — KEEP (intent-bearing).** Parked, not obsolete;
    deleting erases design intent:
    - **Broker authn/authz** — `ActiveBroker.java:433-455` (`/* addUser/addPolicy/removeUser
      /removePolicy */`) + commented plugin hooks (35-36, 315-317, 355-356); the
      `CrescoAuthenticationPlugin`/`CrescoAuthorizationPlugin` classes were never built. This is
      a *security gap*, promoted out of the cleanup pile → see **B-7**.
    - **Parked instrumentation** — `PerfControllerMonitor.initRegionalMetrics/initGlobalMetrics`
      are *called* but their bodies are commented Micrometer gauges (brokered-agent count,
      resource/app queue depth). Directly wanted by the B-2 metrics work — dead-to-*finish*, not
      dead-to-delete.
    - Also: `PluginAdmin.enablePlugin/disablePlugin` (`//todo fix`), `DBType.MYSQL` branches
      (alt backend), sysinfo `getSensorInfo`/proc collection (sensor telemetry),
      `CertificateManager` cert-generation legacy blocks.
  - **C. Stubs for a capability — product keep/cut, item by item.** B13 (above); the OrientDB-era
    `DBInterface` graph methods (`addEdge`, `getNodeStatus`, …); `AgentConsumer` file-eviction
    TODO. (B11/B12 already cut.)
  - **D. Latent bugs — FIX, never delete.** All enumerated ones are now resolved (see above).
    The standing lesson: "no callers" flagged a **live-path** bug (B1/B2) that a bulk sweep would
    have deleted before it was understood.
- **Policy (unchanged):** opportunistic removal for bucket A; **keep B & C**; the
  `AgentEngineShutdown` incident (zero callers ≠ safe to delete) is exactly why buckets B and D
  exist. The enumerated tail has effectively collapsed to **one open decision (B13)** plus the
  broker-auth security decision (**B-7**).

### B-4 — Cosmetic: `[null]` regional-controller path in one warn line
- **STATUS: FIXED (2026-07-02).** The No-PONG warn line now prints `<pending-init>` instead of `[null]`
  when the parent path isn't populated yet (`AgentHealthWatcher.java`).
- **Severity:** Trivial (log cosmetics).
- **Symptom:** `AgentHealthWatcher.java:236` logs
  `No PONG from Regional Controller [null] within 5000ms` during the brief window after
  AGENT_INIT before the parent path is populated.
- **Assessment:** The `[null]` is **truthful** — `cstate.getRegionalControllerPath()` genuinely
  isn't known yet — and self-corrects on the next tick. Not worth churn; left as-is.

### B-5 — MsgRouter route-code semantic mismatch (CLARIFIED — safe cleanup applied)
- **STATUS: ADDRESSED (2026-07-02).** Not a bug — re-confirmed. The `GM` bit is a *reserved positional
  placeholder* keeping every routePath aligned with the `switch` case numbers (which top out at 29679,
  i.e. GM always 0); `isGlobal()→RM="1"` intentionally routes global messages through the regional
  cases. Applied a **behavior-preserving** rewrite (`RM = isRegional||isGlobal`) with a prominent
  comment so the "looks-wrong" line can't be "fixed" into a fabric-breaking regression (setting GM
  pushes global traffic to routePath≥32768 → switch `default` → silent drop). A true GM/RM *split*
  remains a full routing-table rework (new GM cases + a routing test matrix), not a one-liner.
- **Severity:** Latent/known — **do not touch without a full routing rework.**
- **Symptom:** In `MsgRouter.getRoutePath()` (`~line 578`), a **global** message sets the
  *regional* bit: `if (rm.isGlobal()) { RM = "1"; }` while the global bit `GM` stays `"0"`.
  By the field names this looks wrong.
- **Why it must stay:** The consuming `switch(routePath)` (`~line 109`) has **no cases** for a
  `GM=1` route (its highest case is `29679`, below the `32768` a real GM bit would produce).
  The current "wrong" mapping is what actually routes global traffic into handled cases; the
  switch's `default` branch only logs-and-drops. "Fixing" the bit would send global messages to
  `default` and silently drop them. This is compensated-for, tested, working behavior.

### B-6 — DBEngine SQL "space flag" (FALSE POSITIVE — not a bug)
- **Severity:** None (recorded to prevent re-flagging).
- **Symptom:** A linter/audit flagged the concatenated SQL in `DBEngine.java` (~lines 975–980,
  `"AND A.AGENT_ID = '" + agentId + "' " + ...`) as producing token-run-ons like `'X'AND`.
- **Assessment:** Each fragment carries proper trailing whitespace; the SQL tokenizes and
  executes correctly. **No change needed** — documented so it is not "fixed" into a regression.

### B-7 — Broker tenant isolation / authorization — IMPLEMENTED + PROVEN
- **STATUS: SHIPPED + PROVEN LIVE (2026-07-02).** No longer a gap. A working slice of broker
  authorization now enforces tenant isolation: `CrescoAuthorizationBroker` (ActiveMQ 6.2.7
  `BrokerFilter`) + `TenantPolicy` in the controller, identity asserted per-connection, gated by
  `broker_security_enabled` (default OFF). Proven in a live 2-tenant / 2-region / 1-global
  **federated** mesh — `run/tests/tenant_isolation_mesh_test.sh` 10/10 (federation forms under
  security, cross-region cross-tenant reads/writes denied at every broker) plus the Phase-0
  identity/sign/trust primitives `run/tests/security_foundation_test.sh` 15/15. Full architecture,
  what shipped, and the staged remainder (mТLS cert-DN binding, regional-CA issuance, tenant
  dataplane namespacing): **`docs/distributed-identity-trust-design.md`**.
- **Original gap (kept for context):** Previously buried in the B-3 "dead-code" pile as
  `ActiveBroker` "commented auth blocks." Surfaced as a **security decision**, then built on request.
- **Severity:** Medium (security posture) — no functional break, but a real gap.
- **State:** The ActiveMQ broker runs with **TLS transport encryption** (`nio+ssl`, now
  Conscrypt/BoringSSL) but **no broker-level authn/authz**. The intended
  `CrescoAuthenticationPlugin` / `CrescoAuthorizationPlugin` exist only as commented references
  (`ActiveBroker.java` fields 35-36, plugin wiring 315-317, `addUser`/`addPolicy` calls 355-356,
  and the whole `/* addUser/addPolicy/removeUser/removePolicy */` method block at 433-455); the
  plugin classes themselves were never built.
- **Impact:** Any peer that can reach the broker port and present the (shared) cert can connect
  and publish/subscribe to **any** topic (agent/region/global). Authorization by group/policy is
  absent. Isolation today rests on TLS + network reachability + JMS selectors — **not** on
  authenticated principals or topic ACLs.
- **Decision needed (product):**
  - **Implement** — build `CrescoAuthenticationPlugin` (per-agent broker credentials, provisioned
    alongside the existing per-agent cert material) + `CrescoAuthorizationPlugin` (topic ACLs by
    region/agent), then uncomment the wiring. Effort **L**; needs a credential-provisioning story
    and a security test matrix.
  - **Formally defer** — if TLS + network isolation is deemed sufficient for the deployment model,
    keep the commented scaffolding as the intent marker and record the accepted risk here.
- **Do not delete the commented scaffolding** — it is the only breadcrumb that broker auth was
  designed and is intentionally off (bucket B in B-3).

---

## 2. What was deliberately NOT changed (hands-off, with reasons)

| Area | Files / locus | Reason left untouched |
|------|---------------|-----------------------|
| Concurrent developer's WIP → now committed | `ActiveBroker`, `ActiveClient`, `DataPlaneServiceImpl`, agent `AgentEngine`/`pom.xml`, wsapi/clientlib/stunnel TLS+tuning | Another developer's in-flight work; I committed only my own explicitly-added files and never swept their changes. Their commits (TLS/Conscrypt, net metrics, sharding, nettuning) are theirs and **unreviewed by me**. |
| Message routing | `MsgRouter.getRoutePath()` + `switch` | Load-bearing "wrong-looking" code — see **B-5**. |
| DB query text | `DBEngine.java` SQL concatenation | False positive — see **B-6**. |
| Measurements subsystem | `library/.../metrics/*`, controller `measurement/*` | Live, load-bearing, public-API — see **B-2**. Unification deferred. |
| `AgentNode` | (deleted in `61e239d`) | Kept deleted: complete 97-line data-holder, **zero callers**, superseded by the Derby-backed regional model. Distinct from `AgentEngineShutdown` (which *was* wired scaffold and was restored). One `git checkout 61e239d~1 -- <path>` restores it if desired. |
| Public library API surface | `io.cresco.library.*` | Contract for downstream plugins; not altered to avoid breaking consumers. |
| SSL/failover connect path | `ActiveClient.java` | Overlaps active TLS work — see **B-1**. |
| Startup log cosmetics | `AgentHealthWatcher.java:236` | Truthful `[null]`, self-correcting — see **B-4**. |

---

## 3. What WAS fixed (context — so "broken vs fixed" is unambiguous)

Shipped and pushed this working track:

- **JMS session leak in bulk file transfer** — `controller 44fa242`.
  `ActiveProducerWorkerData` constructor opened a JMS session + destination that `run()` never
  used and never closed → one leaked session per bulk transfer. Removed the dead session and
  write-only fields; also close the broken session before rebuilding on the send-retry path.
- **Plugin health labels** — `controller 44fa242`.
  The `plugins` health check printed opaque pluginID UUIDs. Added `PluginAdmin.getPluginNames()`;
  detail now reads `name[id8]=code(STATUS)`.
- **Graceful region shutdown** — `controller b6a880c`.
  A region now sends `region_disable` to its global (unregister) while the broker is alive;
  reordered teardown to fix an NPE where the region health watcher was nulled before the RPC
  that routes through it.
- **Graceful-shutdown entrypoint + pidfile** — `agent db1741f`.
  `AgentEngineShutdown` resolves the agent PID (arg or `<data-dir>/agent.pid`), sends SIGTERM to
  trigger ordered OSGi teardown + parent unregister, waits for clean exit, escalates to SIGKILL
  only on timeout. `HostApplication` writes/removes the pidfile. `run/stop.sh` drives it.
- **Earlier audit-safe fixes** — `controller 61e239d`, `library ec930ab`, `agent 6b2717f`
  (Config JSON ClassCast, self-assign removal, volatile flag, KPI update flag, file-part flag).

---

## 4. How to verify the claims in this report

```bash
# B-1 startup TLS race + self-heal
grep -nE "PKIX|registered with Region|link:parent" run/logs/edge-agent-001.out

# B-2 measurements live at boot
grep -n "new PerfControllerMonitor" \
  code/controller/src/main/java/io/cresco/agent/controller/statemachine/ControllerSMHandler.java

# B-5 load-bearing route code
grep -n "isGlobal\|RM = \|case 29679\|DEFAULT ROUTE CASE" \
  code/controller/src/main/java/io/cresco/agent/controller/communication/MsgRouter.java

# My fixes present
grep -c getPluginNames \
  code/controller/src/main/java/io/cresco/agent/controller/agentcontroller/PluginAdmin.java
git -C code/controller merge-base --is-ancestor 44fa242 HEAD && echo "session-leak/health fix in history"
```

---

## 5. Open decisions for the maintainer

1. **`AgentNode`** — restore anyway, or leave deleted? (Recommendation: leave deleted.)
2. **Metrics unification (B-2)** — approve as a standalone design-first project, or keep as-is?
3. ~~**B-1 hardening**~~ — **DONE (2026-07-03):** trust-ready connect gate shipped + proven (see B-1 above).
