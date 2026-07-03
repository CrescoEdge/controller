# Felix Hygiene Refactor — Deferred Decisions & Scoped-Down Items

Running log of anything intentionally **not** done (or scoped down) during the
`felix-hygiene` phased refactor, so nothing gets silently dropped. Each item: what,
why deferred, and the follow-up.

| # | Phase | Item | Why deferred | Follow-up |
|---|-------|------|--------------|-----------|
| 1 | P2 | **HTTP/Jetty stack left at Felix http.base 5.1.8 / http.jetty 5.1.16** (only SCR, gogo.runtime, WebConsole were refreshed) | The 5.1.x Jetty bundles are recent (late-2024). Moving to `http.jetty 5.2.2` is a coordinated **Jetty 12** migration (http.base 5.2.x + servlet-api + jetty12 bundle alignment) with real regression risk to the console/HTTP path. Out of proportion to a "refresh" step. | Do a focused **Jetty 12 migration** change: bump http.base/http.jetty/http.jetty12/servlet-api as a matched set, retest console (:8080) + wsapi. Pairs with P3 wsapi→HTTP Whiteboard. |
| 2 | P0 | **library kept as an uber-bundle** (embeds + re-exports Siddhi/Micrometer/JMS/Quartz/etc.) | De-embedding requires converting every consumer to real bundle deps — a large re-architecture, not build hygiene. | Consider per-library bundles + proper Import-Package in a later structural pass. |
| 3 | P0 | **Dual Quartz (wso2 2.1.1 + 2.3.2) and old c3p0/json-path in library** left in place (only `_split-package:=merge-first` applied) | Removing one Quartz risks breaking Siddhi at runtime; needs runtime validation. | Consolidate to a single Quartz; drop unused c3p0; bump json-path (CVEs) in P4. |
| 4 | P0 | **controller Bundle-Activator (no-op) kept** | It's the critical bundle; removing its activator changes lifecycle. DS `AgentServiceImpl` is the real entry. | Remove once DS-only start is verified end-to-end. |
| 5 | P2 | **osgi.cmpn 7.0.0 kept for logger** (others moved to individual R8 artifacts) | logger uses `org.osgi.service.log` which isn't one of the individual artifacts added; cmpn 8 is not published. | Optionally add the individual `org.osgi.service.log` R8 artifact and drop cmpn from logger. |
| 6 | P4 (noted) | **jaxb 2.2.11 (wsapi) has a broken POM** (`com.sun:tools` systemPath error — non-fatal) | Pre-existing; not a P0–P2 concern. | Bump JAXB / move to jakarta.xml.bind in P4. |
| 7 | P4 (noted) | **stunnel/wsapi runtime deps were mis-scoped `provided`** (javax.websocket-api, netty) — fixed to `compile` in P0 verify | Resolved, but flags that other "provided" deps may be similarly mis-scoped. | Audit all `provided` scopes for genuinely-embedded runtime deps. |
| 8 | P3.1 | **`StaticAgentLoader` (controller) — REMOVED** (was dead code: zero references, no DS annotations, its factory PID `io.cresco.AgentServiceImpl` used nowhere). | n/a — done. | Done; class deleted. |
| 9 | P3.1 | **`PluginAdmin.startPlugin` + `checkService` PluginService polls left as bounded polls** (300×100ms) — only the *stale-cached-reference* lookups (ConfigAdmin, CoreState, AgentService) and the *unbounded* SCR shutdown loop were converted to ServiceTracker | These polls are already time-bounded and sit on the verified plugin-wiring path; converting adds risk without fixing a stated concern (stale refs / unbounded hangs). | Optionally convert to a filtered `ServiceTracker.waitForService(filter, timeout)` per plugin start. |

_Last updated: Phase 3.1 — ServiceTracker (2026-06-30)._
