# Cresco — Tenant Isolation (Glocal) — Design

**Date:** 2026-07-03
**Status:** **L1+L2 SHIPPED + PROVEN (2026-07-03), flag `tenant_namespacing` (default off).** Both controlled
channels are namespaced end-to-end and role-based authorization is live. Proven by
`run/tests/tenant_namespacing_test.sh` **12/12** (federation forms + zero self-denial under namespacing →
MsgEvent wildcard-infra-inbox + producer tenant-qualification work; TENANT confined to `T.<tenant>.*`;
SUPERUSER cross-tenant god view) with **no regression** (`tenant_isolation_mesh_test` 10/10,
`security_foundation` 15/15, `regional_ca_mtls` 10/10). L3 (dedicated per-tenant bridges) + L4 (payload
encryption) remain optional hardening tiers; core isolation already holds via client-edge ACL at every broker.

**What shipped:**
- `library/security/TenantNamespace` — pure `T.<tenant>.<raw>` qualify/wildcard/parse helper.
- `TenantPolicy` — role-aware (`SUPERUSER`/`INTERNAL`/`TENANT`) + namespaced-prefix rule (own `T.<tenant>.`
  subtree only; closes the flat-name same-region write hole).
- `CrescoAuthorizationBroker` — `roleOf()` from `broker_superuser_tenants` (default `cresco-system`).
- `AgentProducer` — stamps origin tenant (`cresco_tenant` param) + qualifies the forward destination.
- `ControllerSMHandler.initIOChannels` — infra (vm://) consumes the `T.*.` **wildcard** inbox; leaf agents
  consume their own `T.<tenant>.` inbox.
- `DataPlaneServiceImpl.getTopicName` — dataplane topics qualified `T.<tenant>.<agent|region|global>.event`.

This doc recommends the target model and the minimal grounded changes.
**Siblings:** [`distributed-identity-trust-design.md`](distributed-identity-trust-design.md) (owns
identity/trust/authz — this doc is its §5 "tenant namespacing" expanded and revised),
[`optimal-global-routing-plan.md`](optimal-global-routing-plan.md) (W2 = this doc's L1+L3; W6 = L5; W9 = L4),
[`metrics-unification-design.md`](metrics-unification-design.md).

---

## 0. Framing: "glocal" = one global invariant, enforced locally at every hop

The design goal in one sentence: **define a single global isolation invariant, and make it enforceable
from purely *local* information at every broker — including every bridge — so no broker needs global
coordination to uphold it.** That is what "glocal" means here: think globally (one invariant over the
whole federated mesh), act locally (each broker enforces it on the traffic it touches, using only what it
can see).

The mechanism that makes local enforcement of a global invariant possible is **carrying the tenant in two
places at once**: (1) *cryptographically*, in the connection identity (already done — `O=tenant` in the
cert DN, mTLS-bound), and (2) *syntactically*, in the destination name (a mandatory tenant prefix — not
done). With both, any broker can check "does this connection's tenant match this destination's tenant?"
with a local string comparison — no topology map, no controller round-trip. **Namespacing is not cosmetic;
it is the thing that turns a global rule into a local check.**

---

## 1. What's in place vs. the gap (code-grounded)

**Shipped and proven (keep):**
- Tenant bound into the cert DN; mTLS derives the authoritative principal
  ([CrescoAuthorizationBroker.java:141-167](../code/controller/src/main/java/io/cresco/agent/controller/communication/CrescoAuthorizationBroker.java)).
- Regional-CA trust distribution (O(regions), not O(n²)).
- `TenantPolicy` ACL evaluated at addConsumer/addProducer/send.

**The three real gaps — why isolation is "not implemented" for the mesh:**

1. **Destinations are not tenant-namespaced, so the strong boundary is dormant.** The primary rule —
   allow `destination == tenant || destination.startsWith(tenant + ".")`
   ([TenantPolicy.java:70-72](../code/controller/src/main/java/io/cresco/agent/controller/communication/TenantPolicy.java#L70)) —
   only matches destinations that *happen* to be tenant-prefixed. Nothing **mandates** that, and the
   fabric's own destinations (`region_agent` inboxes, `global.event`) are not prefixed. The boundary
   exists in code but has almost nothing to match.

2. **A concrete cross-tenant *write* hole in the flat scheme.** Because inboxes are `region_agent` with no
   tenant, the "same-region-write" rule
   ([TenantPolicy.java:82-85](../code/controller/src/main/java/io/cresco/agent/controller/communication/TenantPolicy.java#L82))
   lets **tenant B send into tenant A's inbox whenever they share a region** (`R_agentA` starts with
   `R_`, so WRITE is allowed). Confidentiality of A's inbox holds (B can't read), but *delivery integrity*
   does not — B can inject. Namespacing closes this: the peer rule becomes intra-tenant.

3. **The shared dataplane topics are a cross-tenant channel, and bridges bypass enforcement entirely.**
   `agent/region/global.event` are allow-listed for *all* tenants
   ([TenantPolicy.java:62-63](../code/controller/src/main/java/io/cresco/agent/controller/communication/TenantPolicy.java#L62),
   `DataPlaneServiceImpl` flat topic names). The comment's justification ("isolated by physical per-region
   broker separation") **fails on a global broker**, where multiple regions/tenants converge. And every
   broker-to-broker bridge is *blanket-exempt* —
   `ctx.isNetworkConnection() → return true`
   ([CrescoAuthorizationBroker.java:102](../code/controller/src/main/java/io/cresco/agent/controller/communication/CrescoAuthorizationBroker.java#L102)) —
   so inter-broker hops carry **all tenants with zero tenant enforcement.**

**Conclusion:** enforcement today exists only at the *client edge of a single broker*. It is neither
end-to-end nor cross-bridge. That is exactly the piece to build.

---

## 2. Isolation has three axes — name them, or you'll only build one

| Axis | "A tenant cannot …" | Covered by |
|---|---|---|
| **Confidentiality** | …read another tenant's data | L1 namespace + L2 ACL (read-deny) + L4 encryption |
| **Delivery integrity** | …inject into / misdeliver to another tenant | L1 namespace + L2 ACL (write-deny) + **L3 bridge scoping** |
| **Performance** | …starve/degrade another tenant (noisy neighbor) | **L5 per-tenant QoS (H-DWRR)** |

An ACL-only design covers confidentiality and part of delivery integrity, *on one broker*. Real isolation
across the mesh needs all three axes, at every hop.

---

## 3. The layered model (each layer independently switchable)

### L0 — Identity (done). Tenant cryptographically bound in the cert DN; the anchor everything keys off.

### L1 — Namespace (the keystone). Make the tenant a **mandatory** destination prefix, everywhere.
Every destination — inboxes **and** dataplane topics — carries its tenant:
```
Inbox / queue:   t.<tenant>.<class>.<region>.<agent>
Dataplane topic: t.<tenant>.<class>.event        (per-tenant, replacing the shared global.event)
```
`<class>` ∈ {live, ctrl, tele, bulk} — the `MsgQoS` tier, placed *before* region so a bridge or scheduler
can filter by tenant **and** class with one prefix. Use `.` as the hierarchy separator so a whole tenant
is one wildcard subtree: `t.<tenant>.>`. This single change:
- makes L2/L3 a local prefix rule (the whole point of "glocal");
- removes the shared-topic cross-tenant channel (gap #3) — `global.event` becomes N per-tenant topics;
- closes the same-region write hole (gap #2) — inboxes are now tenant-scoped;
- gives L5 (per-tenant QoS) and the router (per-tenant/per-class paths) a ready-made key.

### L2 — Local ACL (mostly done; simplify around the prefix).
`TenantPolicy` collapses to: *a connection authenticated as tenant T may only touch `t.<T>.…`* (plus
advisories, handled in L1/§5). The flat-name special cases (`region_agent` own-inbox, same-region peer
read/write asymmetry) become **intra-tenant** sub-rules under the prefix, and the cross-tenant write hole
disappears. Net: fewer rules, stronger guarantee.

### L3 — Bridge-scoped tenant forwarding (THE fix for gap #3).
Replace the blanket `isNetworkConnection → exempt` with a **per-bridge allowed-tenant set**. Two levers,
both already available in Cresco:
- **Forwarding filter (primary):** each bridge connector forwards *only* its allowed tenant namespaces via
  `setDynamicallyIncludedDestinations(t.<tenant>.>)` / `setExcludedDestinations(...)` — the *exact*
  mechanism `buildConnector` already uses for dataplane sharding
  ([ActiveBroker.java:572-586](../code/controller/src/main/java/io/cresco/agent/controller/communication/ActiveBroker.java#L572)).
  A message for a non-allowed tenant prefix is never bridged.
- **Receiving-side enforcement (defense in depth):** the bridge connection *does* have an identity (the
  peer broker's cert, chaining to a region CA). Give bridges a `BridgeTenantPolicy` (allowed-tenant set for
  that edge) instead of a free pass, so even if a mislabeled message crosses, the receiving broker's L2
  prefix check drops it.

**Shared prefix-filtered bridges (O(E)) are the default**; **dedicated per-tenant bridges (O(T·E)) are an
opt-in high-assurance tier** — mechanically just multiple connectors per host, which `bridgeGroups` /
`addBridgeConnections` already support.

### L4 — End-to-end confidentiality across bridges (opt-in).
Transport mTLS encrypts each hop *in flight*; the residual threat is **payload at rest inside a
compromised/inspected transit broker**. For tenants that can't accept that: envelope-encrypt the payload
with a tenant key (AES-256-GCM), keep routing metadata (namespaced dst + segment path) clear in JMS
headers, decrypt at egress. Reuse the shipped `MessageSigner` pattern / recipient leaf public key. Opt-in,
because it's costly and most tenants are satisfied by L1–L3 + hop TLS.

### L5 — Per-tenant performance isolation (the "QoS everywhere on the tenant level" requirement).
Hierarchical Deficit-Weighted Round Robin keyed by the L1 tenant prefix (L1 inter-tenant fair share, L2
intra-tenant `MsgQoS` priority), enforced at the contention points (ingress rate-limit + shared-bridge
egress) by extending the existing `CrescoAuthorizationBroker` filter chain. This is routing-plan W6; it is
**part of the isolation model, not an add-on** — without it, one tenant's bulk flood violates isolation
axis 3.

---

## 4. The glocal invariant (state it, enforce it, test it)

> **Global invariant:** a message stamped tenant *T* exists *only* on destinations prefixed `t.<T>.`, and
> is accessible *only* to connections cryptographically authenticated as *T*.
>
> **Local enforcement (every broker, every op — produce, consume, send, *and bridge-forward*):** compare
> the connection's cert-bound tenant to the destination prefix; allow iff they match (plus explicit
> advisory/shared carve-outs). No global state consulted.

Because tenant is both cert-bound and name-carried, the check is purely local, so it **composes**: if every
hop upholds it, the mesh upholds it. Defense in depth: a message that somehow reaches the wrong broker is
dropped by that broker's L2 check even if a bridge misforwarded it.

---

## 5. Concrete, minimal changes (grounded)

1. **Mandate the prefix at ingress.** `MsgRouter` ingress stamps/validates `t.<tenant>.<class>.…` from the
   connection's bound identity; a publish whose destination tenant ≠ authenticated tenant is dropped at
   ingress (not just denied at the broker).
2. **Per-tenant dataplane topics.** `DataPlaneServiceImpl` topic names become `t.<tenant>.<class>.event`;
   remove `global.event` (and the sharded `global.event.N`) from `broker_shared_destinations` — the shared
   carve-out ([TenantPolicy.java:62](../code/controller/src/main/java/io/cresco/agent/controller/communication/TenantPolicy.java#L62))
   should shrink to genuinely tenant-agnostic control only.
3. **Bridge policy.** New `BridgeTenantPolicy` + per-connector `setDynamicallyIncludedDestinations`; replace
   the `isNetworkConnection` blanket exemption
   ([CrescoAuthorizationBroker.java:99-111](../code/controller/src/main/java/io/cresco/agent/controller/communication/CrescoAuthorizationBroker.java#L99))
   with the allowed-tenant-set check.
4. **Advisory leak (subtle).** `ActiveMQ.Advisory.*` is allow-listed
   ([TenantPolicy.java:59-61](../code/controller/src/main/java/io/cresco/agent/controller/communication/TenantPolicy.java#L59));
   advisories can leak *destination existence* across tenants on a shared broker. Decide: scope advisories
   per tenant subtree, suppress cross-tenant advisory propagation on bridges, or accept + document.

---

## 6. Cross-global — the "global" half of glocal

A global controller must only accept/forward tenant namespaces it is *federated for*. Reuse the trust-bundle
gossip already designed for CA distribution
([distributed-identity-trust-design.md](distributed-identity-trust-design.md) §3.2): alongside the region-CA
bundle, a global advertises an **allowed-tenant set per federation edge**. A global↔global bridge then
applies L3 with that set, so a tenant present under G1 is only reachable under G2 if the G1↔G2 edge is
authorized for it. Tenant reachability becomes a policy artifact distributed the same way trust is — no new
distribution channel.

---

## 7. Migration (additive, flag-gated, staged — same discipline as the security fabric)

- **Flag `tenant_namespacing`**, default off → fabric unchanged.
- **Dual-namespace transition shim:** during rollout, brokers accept both flat and namespaced destinations;
  a translation layer maps legacy `region_agent` → `t.<tenant>.<region>.<agent>` using the connection's
  bound tenant, so un-migrated clients keep working. Flip enforcement on per region once both ends speak
  namespaced.
- **Default tenant:** existing single-tenant deployments map to `t.default.…` — nothing breaks.
- **`vm://` stays exempt** for local rights, but the controller must now stamp the correct tenant on
  outbound so its own messages land in the right namespace.
- Stage region-by-region; keep the full bring-up/recovery + `tenant_isolation_mesh_test.sh` +
  `regional_ca_mtls_test.sh` suites green with the flag both off and on.

---

## 8. Decisions to make (recommendations first)

1. **Namespace scheme.** *Recommend* `t.<tenant>.<class>.<region>.<agent>`, `.`-hierarchical, class before
   region. Confirm delimiter and that IDs never contain it.
2. **Tenant ID form.** *Recommend an opaque stable ID* (UUID/hash), not the human `O=tenant` string, so
   tenant names don't leak in destination strings visible to shared-broker operators; controllers hold the
   map. (Trade: harder debugging.)
3. **Bridge model.** *Recommend* shared prefix-filtered (O(E)) default + dedicated per-tenant opt-in.
4. **Advisory handling.** *Recommend* scope/suppress cross-tenant advisories (close the existence leak).
5. **Encryption tier + key mgmt.** *Recommend* opt-in; per-tenant key via recipient leaf pubkey to avoid a
   new KMS dependency initially.
6. **Cross-global tenant policy.** Allowed-tenant-per-edge in the trust bundle (§6).

---

## 9. Relationship to the routing plan

L1 (namespace) + L3 (bridge scoping) **are** routing-plan W2 and are its prerequisite — the router forwards
per-tenant prefixes on chosen bridges, so isolation and routing share the same substrate. L5 = W6, L4 = W9.
**Build order:** L1 → L2 → L3 first (they close the actual leaks and unblock W2 routing), L5 alongside the
QoS/fairness work, L4 as an opt-in tier. Do L1–L3 before any redundant-path routing carries multi-tenant
traffic, or the new paths widen the very leak we're closing.

---

## 10. Configuration & operations reference (shipped)

All tenant-isolation behavior is additive and flag-gated; the defaults reproduce the pre-existing fabric
exactly. There are two independent switches — the **security fabric** (identity/mTLS/authz, shipped
earlier) and **namespacing** (this work). Namespacing is only meaningful with the security fabric on.

| Config (`-D` or config file) | Default | Effect |
|---|---|---|
| `tenant_namespacing` | `false` | Master switch for this work. When on: MsgEvent inbox queues and dataplane topics are qualified `T.<tenant>.<raw>`; producers stamp+qualify by message tenant; infra (vm://) consumes the `T.*.` wildcard inbox. Off ⇒ raw names, byte-for-byte unchanged. |
| `tenant_id` | `default` | This node's tenant (1-64 of `A-Z a-z 0-9 -`; no `.`, so no tenant's `T.<tenant>.` prefixes another's). Bound into the leaf cert DN (`O=<tenant>`) and used as the local tenant for stamping/qualification. In a single-tenant deployment leave as `default`. |
| `broker_security_enabled` | `false` | Installs `CrescoAuthorizationBroker` (the ACL). Required for isolation to be *enforced*; namespacing without it only *names* destinations, it does not deny. |
| `broker_require_client_auth` | = `broker_security_enabled` | Broker demands a client cert (mTLS) and binds the principal from the validated, registry-bound certificate. Defaults ON with broker security. When OFF the broker still requests a certificate (Cresco nodes present theirs); a network connection without one has NO identity and no role whatever username it types (never a tenant, never SUPERUSER): it is quarantined -- its subscriptions receive nothing, its sends are discarded, no durable or exclusive subscriptions (`broker_allow_unverified_bridges=true` keeps the old bridge exemption during a migration). |
| `broker_superuser_tenants` | `cresco-system` | Comma list of tenants whose **network** clients get the cross-tenant `SUPERUSER` role (god view). The local controller (`vm://`) and broker bridges are superuser-equivalent regardless. |
| `broker_shared_destinations` | `agent.event,region.event,global.event` | Tenant-agnostic destinations allowed for everyone. Under namespacing the real dataplane topics are `T.<tenant>.*.event`, so these flat names are unused; kept for back-compat. |
| `security_regional_ca` | `false` | Regional-CA enrollment (O(regions) trust). Recommended at scale so every node presents a CA-signed, identity-bearing leaf. |
| `broker_security_log_allow` | `false` | Verbose: log every ALLOW decision (debugging ACLs). DENY is always logged. |
| `peer_identity_mode` | `enforce` | Every outbound broker TLS connection (agent to its regional/global broker, each end of a broker bridge) must present a certificate naming the controller discovery returned for that address (and its pinned key, under per-node trust); `warn` logs and connects, `off` is chain-only as before. Under mTLS it also gates the broker-bridge exemption to discovered region/global controllers. See distributed-identity-trust-design §7. |
| `broker_stream_recipient_binding` | `true` | With `broker_security_enabled`: a dataplane message carrying `cresco_to=<region>_<agent>` (optionally `cresco_to_tenant`) is dispatched only to that agent's connection; a subscription whose selector asks for another identity's `cresco_to` is refused. Messages without `cresco_to` are unaffected. |
| `discovery_auth` | `v2` | Certificate trust is learned only over the mutually authenticated, replay-proof discovery v2 exchange; `compat` also accepts the legacy exchange during a rolling upgrade. See distributed-identity-trust-design §7.4. |
| `peer_identity_rebind` / `peer_bridge_revoke` / `peer_bridge_prune_days` | empty / empty / `30` | Operator controls over the persistent peer trust registry (one key per name, one CA per region, bridge peers). §7.3. |
| `node_identity_persist` / `peer_trust_persist` | `true` / `true` | The node's key, region CA and peer trust registry survive restarts (`<data>/security`). |
| `security_federated_tenants` | empty (any) | Tenants a regional-CA controller federates with. |

**Recommended enablement order (staged, per region, never blind):**
1. Turn on `security_regional_ca` fabric-wide; confirm every node enrolls and bridges under mTLS
   (`regional_ca_mtls_test`).
2. Turn on `broker_security_enabled` + `broker_require_client_auth`; confirm zero self-denial and that
   cross-tenant is denied (`tenant_isolation_mesh_test`).
3. Turn on `tenant_namespacing`; confirm the fabric still forms and control plane flows with zero
   self-denial (`tenant_namespacing_test`). Roll it region-by-region.
4. Provision `broker_superuser_tenants` only for genuine fabric-admin/god-view clients.

**Failure modes & what they look like:**
- A `DENY <READ|WRITE> '<dest>' for <identity> : <reason>` in a broker log is the ACL working. On a
  *legit* flow it means a namespacing mismatch — usually a producer stamping the wrong tenant or an infra
  node not using the `T.*.` wildcard inbox.
- Messages silently not delivered under namespacing (no DENY) ⇒ producer qualified with tenant X but the
  consumer subscribes tenant Y (a genuine cross-tenant send — correct isolation), *or* an infra node was
  mis-detected as a leaf and got a tenant-specific inbox instead of the wildcard.

---

## 11. End-to-end message-flow walkthrough (namespacing on)

Trace a request from **agent1 (tenantA, region-a)** to **agent2 (tenantA, region-c)**, relayed
region-a → global → region-c. Local tenant of each node in parentheses.

```
agent1 (tenantA)                 region-a ctrl (tenantA)      global ctrl (global)         region-c ctrl (tenantA)      agent2 (tenantA)
   |  produce, dst=region-c_agent2                                                                                          |
   |  AgentProducer: no cresco_tenant param -> stamp tenantA; qualify -> T.tenantA.region-c_agent2                          |
   |  send to its regional ctrl inbox (T.tenantA.region-a_ctrlA)                                                            |
   |------------------------------->|                                                                                       |
   |            (network client, mTLS tenantA; ACL: writing own T.tenantA.* -> ALLOW)                                       |
   |                                | MsgRouter relays; AgentProducer sees cresco_tenant=tenantA (preserved)                |
   |                                | qualify raw fwd dst with tenantA -> T.tenantA.<global>_<globalAgent>                   |
   |                                |=========bridge========>|                                                              |
   |                                |   (bridge exempt; demand-forwards T.tenantA.* on demand)                              |
   |                                |                        | global ctrl on vm:// consumes T.*.<global>_<globalAgent>   |
   |                                |                        | (wildcard infra inbox -> receives any tenant stamp)        |
   |                                |                        | relays; qualify with preserved tenantA                     |
   |                                |                        |=========bridge========>|                                     |
   |                                |                        |         (demand-forwards T.tenantA.*)                       |
   |                                |                        |                        | region-c ctrl vm:// T.*. inbox     |
   |                                |                        |                        | relays to leaf; qualify tenantA    |
   |                                |                        |                        |----------------------------------->|
   |                                |                        |                        |  agent2 network client, mTLS       |
   |                                |                        |                        |  consumes T.tenantA.region-c_agent2|
   |                                |                        |                        |  (ACL: own tenant -> ALLOW)        |
```

Key invariants this shows: **(a)** the tenant is stamped once at origin and *preserved* across every relay,
so every hop qualifies to the identical queue name; **(b)** infra controllers (vm://) use the `T.*.`
**wildcard** inbox so they relay any tenant regardless of the stamp; **(c)** the leaf's own regional
controller is always same-tenant, so the last hop qualifies correctly; **(d)** a cross-tenant send would
qualify to `T.tenantB.…`, which agent2 (tenantA) never consumes *and* which agent1's own ACL would deny —
isolation is enforced twice over.

### Tenants' regions under a global of a superuser tenant (2026-11-11)

The trace above is one tenant talking to itself through the global. When the global is of a superuser tenant
(`broker_superuser_tenants`) and manages several tenants' regions, three more things hold; without them such a
fabric did not form (`docs/tests/fabric_matrix.py --topology tenants`):

- **Which namespace a message travels in is decided where its origin stamps it** (`ControllerEngine.stampAs` →
  `MsgRouter.travelTenant`), before it is signed. A tenant's caller: its own tenant, always — an answer is the
  request's message sent back and carried the request's `cresco_tenant`, so a tenant's answer to the global stayed in
  `T.cresco-system.` and the tenant's own router dropped it (`route: refused … travels in tenant cresco-system's
  namespace`). A superuser tenant's caller: answering a tenant's caller, that caller's verified tenant (never one the
  request merely named); else the stamp the message carries (a superuser's request it answers, or the tenant an
  operator's client named through wsapi); else the tenant of the node it addresses; else its own.
- **The tenant rule holds where a message is dispatched, not only where it is relayed**
  (`ControllerEngine.admitArrival`, from the inbox and from `admitDeferred`): its namespace must be its verified
  caller's tenant's unless the caller is of a superuser tenant, and an RPC request of another tenant's caller must
  name the node that verified as its sender (its own node, at its region or its former one; an API user's speaker) —
  the answer goes to the request's source, which nothing else binds. Refused: dropped, counted
  (`msg.dropped.cross_tenant`, `msg.dropped.sender_unbound`), logged, never answered.
- **The tenant of a region** is the tenant of that region's controller(s) that completed discovery with this node
  (`PeerTrustRegistry.tenantOfRegion`: the bridge peer's recorded tenant, else the tenant bound to its name); failing
  that, the tenant bound here to the very node addressed. A tenant's leaf consumes only `T.<its tenant>.<path>`, so
  this is what lets the global reach it. A region whose recorded controllers are of several tenants — a superuser
  tenant among them or not — is addressed in nobody's namespace (nothing is guessed: the second claimant would be
  handed the first one's messages). Then, and for a region nothing is recorded of, the message goes in the sender's
  own namespace, counted (`msg.tenant.unresolved`) and logged (`TENANT UNRESOLVED`, with the region and the tenants).
  A superuser node that does not hold the region's controller first hand (an operator's agent, a second global) does
  not resolve it: its client names the tenant (`cresco_tenant`, from `tenant_id` in `listagents`).
- **A node of a superuser tenant reads its inbox in every tenant's namespace** (`T.*.<its inbox>`,
  `MsgRouter.inboxQueue`), a leaf like a controller: a tenant answers it in the tenant's own namespace. A tenant's
  bridge therefore brings in nothing under a superuser tenant's name any more.
- **The global's registry listener for region exports is on every tenant's topic** (`T.*.agent.event`,
  `DataPlaneServiceImpl.addMessageListenerEveryTenant`) when the global is of a superuser tenant: a region exports its
  state in its own tenant's namespace, and that export is the region's registration. The namespace says nothing about
  the writer, so the writer is everything: an export is applied only when its verified writer is a controller (no API
  caller, whatever its role), the one that joined this global, reporting its own region
  (`RegistryBinding.dataplaneRefusal`); its agents' rows carry the writer's tenant. The registry keys an agent row and
  a plugin row by id alone, fabric-wide: an entry for a name that is another tenant's node here is refused (`REGISTRY
  NAME COLLISION`, the row untouched), and an export's omission removes only its writer's tenant's rows — so the same
  name in two tenants is listed once, for whoever registered it first. Nothing else changes: the node publishes, and
  every other listener subscribes, in its own tenant's namespace.

No tenant gains reach: a tenant's node sends in its own namespace only (the router's rule is unchanged), a tenant's
bridge still carries its own tenant's subtree, and only a superuser tenant's node — which the policy already lets
into every tenant — chooses another tenant's namespace.

---

## 12. Security properties & threat model

**Guaranteed (with `broker_security_enabled` + `broker_require_client_auth` + `tenant_namespacing`):**
- *Confidentiality (client-to-client):* a `TENANT` client cannot **subscribe** to any destination outside
  its own `T.<tenant>.` subtree at **any** broker in the mesh — enforced locally at every broker from the
  cert-bound tenant, so it holds across bridges and across globals.
- *Delivery integrity:* a `TENANT` client cannot **publish** into another tenant's subtree; the flat-name
  same-region write hole (tenant B injecting into tenant A's `region_agent` inbox) is closed because inboxes
  are now `T.<tenant>.`-scoped.
- *Non-spoofable identity:* under mTLS the tenant is derived from the validated client-cert DN and overrides
  any self-asserted username.

**Explicitly NOT guaranteed by L1+L2 (documented, tiered mitigations):**
- *A malicious/compromised BROKER* is trusted infrastructure (it holds a region-CA identity and relays all
  tenants); it can read/inject across tenants. Mitigation: L3 dedicated per-tenant bridges (blast radius)
  and L4 payload encryption (confidentiality from the broker itself). Out of scope for L1+L2.
- *Payload confidentiality at rest inside a transit broker.* mTLS encrypts each hop in flight; a payload
  sitting in a transit broker's store is readable by that broker. Mitigation: L4 envelope encryption (opt-in).
- *Advisory destination-existence leak.* `ActiveMQ.Advisory.*` is allow-listed; a tenant can learn other
  tenants' destination *names* (not contents) via advisories. Mitigation: scope/suppress advisories (future).
- *Metadata:* routing headers (tenant, src/dst region/agent) travel in clear so brokers can route; only
  payloads are candidates for L4 encryption.

**Trust boundary:** tenants trust the Cresco infrastructure (controllers/brokers) to relay faithfully;
they do **not** trust each other. That is the standard shared-fabric multi-tenant model and matches how
NATS accounts / Solace VPNs draw the line.

---

## 12a. Inside one tenant: a node reads its own inbox only (2026-11-11)

Sections 1 to 12 keep tenants apart. This one is about two nodes of the SAME tenant, and the threat is a compromised
node that holds a valid identity (a leaf's certificate).

**The defect.** The namespaced rule of `TenantPolicy.check` allowed every access, READ included, to every destination
of the principal's own `T.<tenant>.` subtree. A node's inbox is a queue there (`T.<tenant>.<region>_<agent>`), so any
verified node of a tenant could open a consumer -- on the broker it is attached to -- on its controller's inbox, a
sibling's, or on the pattern `T.<tenant>.>`, and receive (as a competing or an exclusive consumer: take away) the
control messages addressed to other nodes. They are signed, not encrypted. The un-namespaced rule always had it right
("own-queue"; "read of peer inbox in region denied"); the namespaced rule had lost it.

### Who reads what on a broker (the inventory, from the code)

Only NETWORK clients are judged by the rule. A controller's own connections to its own broker are in-JVM (`vm://`), a
bridge is a verified controller's (`isTrustedInfra`: broker-internal, in-JVM, or a verified bridge) and what a bridge
carries is judged per message by `bridgeRefusal`; a bridge's local side and broker-internal contexts are forwarders
(`isForwarder`, `bridgeCarrier`). None of them is touched by this section.

| Reader | Destination | Kind | Name |
|---|---|---|---|
| `AgentConsumer` (every node) | its inbox | queue | `T.<tenant>.<region>_<agent>` (`MsgRouter.inboxQueue`) |
| `AgentConsumer`, node with `region_alias` | the inbox of its former address | queue | `T.<tenant>.<former region>_<agent>` (`RegionAlias.aliasQueue`) |
| `AgentConsumer`, node of a superuser tenant | its inbox in every tenant's namespace | queue pattern | `T.*.<region>_<agent>` (role SUPERUSER) |
| `AgentConsumer`, controller on its own broker (in-JVM, exempt) | the same pattern; in the upgrade window also its raw inbox | queue | `T.*.<region>_<agent>`, `<region>_<agent>` |
| a node of an earlier build without namespacing (upgrade window) | its raw inbox | queue | `<region>_<agent>` (the un-namespaced rule: "own-queue") |
| `DataPlaneServiceImpl` (`getConsumer`, `shardConsumer`, `ownConsumer`) | the tenant's dataplane | topic | `T.<tenant>.agent.event`, `.region.event`, `.global.event`, each also `.<shard>` |
| `DataPlaneServiceImpl.addMessageListenerEveryTenant` (a global of a superuser tenant) | regions' exports | topic pattern | `T.*.agent.event` |
| the ActiveMQ client itself | temporary-destination advisories | topic | `ActiveMQ.Advisory.TempQueue,ActiveMQ.Advisory.TempTopic` |

Nothing else creates a consumer, a browser or a durable subscriber (`createConsumer|createBrowser|
createDurableSubscriber` over the controller; the library and the wsapi, filerepo, stunnel, executor, sysinfo, repo and
gfs plugins create none). A plugin reaches the broker only through `DataPlaneService`, where it chooses a topic TYPE
(`AGENT`, `REGION`, `GLOBAL`), a selector and a shard -- never a destination name, never a queue.

So **there is no shared queue**: no work queue, no queue two nodes read. Every queue a network client legitimately
reads in a tenant's namespace is its own inbox, in one of the two forms above. No name space for shared queues is
defined; one that is ever needed must be one that cannot be an inbox name, and must be added to the rule by name.

### The rule

A principal of role `TENANT`, on a destination of its own `T.<tenant>.` subtree:

- **WRITE**: any destination, as before (a node writes its controller's and its peers' inboxes).
- **READ of a topic**: any, as before (below).
- **READ of a QUEUE** (a consumer, a queue browser): only its own inbox -- `TenantPolicy.isOwnInbox`: the name is
  exactly `T.<tenant>.<region>_<agent>` of its certificate, or `T.<tenant>.<R>_<agent>` with the same agent name and
  `R` a plain region name (letters, digits, `-`: `Caller.validAlias`). Anything else is refused at the subscription
  (`DENY READ '<queue>' for <identity> : a node reads no queue of its tenant's namespace but its own inbox`): another
  node's inbox, a plugin-suffixed name, a queue that is no inbox, and every pattern -- an inbox name holds no wildcard
  (`*`, `>`) and no `,`, a pattern is never equal to one, and a composite was already refused to all but
  infrastructure.
- **Removing or creating a queue by name** (`DestinationInfo`) needs READ and WRITE as before, so with the same rule
  a node removes no queue but its own inbox (before: its controller's inbox and what waited in it, whenever no
  consumer was on it). A queue is still created by the first message written to it.

`SUPERUSER` and `INTERNAL` are not judged (a superuser tenant's node reads `T.*.<its inbox>` and any pattern); in-JVM
connections and bridges never reach the rule; another tenant's namespace is refused as before, a named cross-tenant
sink that says `read` excepted (the operator's explicit grant, unchanged). Callers that judge a NAME without its kind
keep the earlier answer: a bridge's reach (`bridgeRefusal`), an advisory about a destination (`advisoryAllowed`), the
creation of a destination by writing.

### What binds an alias inbox to the node

`AgentConsumer` opens the alias inbox when it connects, before the node has registered; the alias the region records
(`recordedRegionAlias`) is the node's own statement in that registration. So neither can gate the subscription, and
neither would bind anything: the node says what its former region is. What binds an inbox -- either form -- to the
node is **the agent name in its certificate**: the queue's name must end in `_<agent>`, whole (names hold no `_`), and
within a region a name is bound to one key. A node therefore never reads its controller's, a sibling's or a plugin's
queue through the alias form: they end in another agent's name.

What the alias form does reach is `T.<tenant>.<another region>_<the same agent name>`: the inbox of a node of the
same name, in the same tenant, in another region. That is the alias feature itself (a moved node answers at its
former address), and it is safe only under the requirement `region_alias` already states: **agent names are unique
across the fabric**. Nothing enforces that at enrolment today (section 15), and where a node joins under a name of
its own choosing (the standard profile: an agent's identity is its own claim, bound at first use) a compromised node
could join again under the agent name of a controller of another region and ask for that controller's inbox as its
"former address". So the broker
refuses the alias form for the address of **a controller it knows**: its own, its parent, and every controller that
joined it or bridges to it (`formerAddressOfAController`, asked when the consumer is added and when a queue is
removed, never per message): `DENY READ '<queue>' for <identity> : the inbox of a controller this broker knows is no
node's former address`. A controller that is there reads that inbox; it is nobody's former address.

What remains is the inbox of a LEAF of the same agent name in another region (or of a controller this broker has
never met): the broker a node is attached to cannot know every name of the fabric. Closing it is refusing, at
enrolment or registration, an agent name another region of the tenant already holds (section 15).

### Per message as well

The subscription rule is the gate; two things reach a node without passing it, so the broker also judges each
message (`isAllowedToConsume`, which every queue and topic subscription and every queue browser asks):

- **Advisories that carry a copy of a message.** A tenant's node may read advisory topics, and
  `ActiveMQ.Advisory.Expired.Queue.<queue>`, `...MessageDiscarded...`, `...MessageDLQd...` carry the message itself
  (its properties -- the caller stamp among them -- and its body where a destination's policy asks for it; none does
  today). The copy was judged by its destination's name: every node
  of the tenant was sent the copy of a message that expired in another node's inbox. It is now read as the queue it
  was on: a network node is sent the copy only for its own inbox. This is the one topic rule that changed, because
  such a topic holds a message addressed to a single node.
- **Whatever subscription a node holds.** A message on a queue of the connection's own tenant's namespace is given
  to a network node only when that queue is its own inbox (`notItsInbox`); otherwise it is withheld, counted
  (`withheldCount`) and logged (`WITHHELD message on '<queue>' from <identity> : a queue of its tenant's namespace
  that is not this node's own inbox`, at most every 10 s). It costs a few field reads and one comparison of the
  queue's name with the identity's; no lookup, no lock, nothing for topics.

Why the subscription rule alone is otherwise tight: every consumer and browser of a network client passes
`addConsumer` on the broker it attaches to; queue subscriptions do not outlive the broker (only durable topic
subscriptions are restored, and a rolling upgrade restarts the broker), and a connection's identity and role never
change while it is up, so no subscription predates the rule. `Consumer.*.VirtualTopic.>` queues are fed from TOPICS
named `VirtualTopic.>`, never from a queue, and lie outside every tenant's namespace; mirrored queues (a topic copy
`VirtualTopic.Mirror.<queue>` of every queue) are off (the ActiveMQ default; `ActiveBroker` does not turn them on) and
would lie outside every namespace too; the dead-letter queue is `ActiveMQ.DLQ`, which no tenant reads. A tenant must
not be named `VirtualTopic`, `Consumer` or `ActiveMQ` (the un-namespaced rule would hand it those names).

### Topics: unchanged, and what a node can still learn inside its tenant

A node of a tenant reads and writes every topic of `T.<tenant>.`, patterns included. Ranked by what it yields:

1. **The tenant's whole dataplane** on the broker it is attached to (and what its demand brings over bridges):
   `agent.event`, `region.event`, `global.event` -- telemetry, logs, state exports, application streams, the output
   of processes. Only a stream its sender bound to a recipient (`cresco_to`, `StreamRecipientPolicy`) reaches that
   recipient alone, and no subscription but the executor's sees a process's stdin (`StdinGuard`). Next: bind by
   default the streams that have one reader.
2. **Who is there.** Advisories about consumers, producers and destinations of its tenant: the names of the
   tenant's nodes on this broker (their inbox queues), when each connects and leaves, and the selectors of their
   dataplane subscriptions. No message. Next: give a node the advisories about its own inbox only (the same check).
3. **It can write** to any inbox and topic of its tenant: receivers judge a message by its verified caller and its
   signature, but nothing bounds the volume, and it can create queues without bound. Next: quotas per identity.
4. **Names outside `T.`**: the un-namespaced rule still lets a tenant read `<tenant>.` names, queues included. No
   Cresco code uses them with namespacing on; a raw inbox lies under such a prefix only when a region's name begins
   with `<tenant>.`, and raw inboxes exist only in the upgrade window.

### Tests

`TenantPolicyInboxReadTest` (the rule, name by name), `InboxReadBrokerTest` (a real secured broker, mutual TLS: no
consumer and no browser on another inbox or a pattern; all of what is sent to a controller's inbox reaches the
controller; a subscription put on the queue past the rule is given nothing; an expired message's advisory copy; no
removal of another node's inbox; no alias form of a known controller's address; a node of the released build --
inbox, alias inbox, raw inbox -- and a superuser tenant's node work as before). Against the sources before the
change 8 of the 10 broker tests fail (the two that pass are the last two: what must not change), and with the rule
alone switched off 4 of the 8 unit tests fail (the other four assert what is unchanged: writes, topics, superuser,
names outside the namespaced rule).

---

## 13. Verification (shipped tests)

| Test | Result | What it proves |
|---|---|---|
| `run/tests/tenant_namespacing_test.sh` (new) | **12/12** | Federated 1-global/2-tenant-region mesh forms under `tenant_namespacing=true`; **zero self-denial** on every node's control plane (⇒ MsgEvent `T.*.` wildcard inbox + producer tenant-qualification work end-to-end); `TENANT` confined to own `T.<tenant>.*`, denied cross-tenant and `T.*.` wildcard; `SUPERUSER` (`cresco-system`) gets cross-tenant god view. |
| `run/tests/tenant_isolation_mesh_test.sh` | 10/10 | **No regression** with namespacing off (default): legacy `<tenant>.stream` ACL isolation intact across the federated mesh. |
| `run/tests/security_foundation_test.sh` | 15/15 | Identity/sign/verify/chain-trust primitives intact. |
| `run/tests/regional_ca_mtls_test.sh` | 10/10 | Regional-CA enrollment + fabric-wide mutual-TLS federation intact. |

Run all four after any change to the auth or namespacing path; each must stay green with the flags both
**off** (default) and **on**.

---

## 14. Implementation map (where each layer lives)

| Layer | File(s) | Symbol |
|---|---|---|
| L1 namespace helper | `library/.../security/TenantNamespace.java` | `qualify` / `wildcard` / `tenantOf` / `isNamespaced` |
| L1 MsgEvent producer | `controller/.../communication/AgentProducer.java` | `sendMessage` — stamp `cresco_tenant` + qualify `dstPath` |
| L1 MsgEvent inbox | `controller/.../statemachine/ControllerSMHandler.java` | `initIOChannels` — `T.*.` wildcard (vm://) vs `T.<tenant>.` (network) |
| L1 dataplane | `controller/.../data/DataPlaneServiceImpl.java` | `getTopicName` — qualify topics |
| L2 policy | `controller/.../communication/TenantPolicy.java` | `Role` enum + namespaced-prefix rule in `check`; `isOwnInbox` (a queue is read by its node only, section 12a) |
| L2 role resolution | `controller/.../communication/CrescoAuthorizationBroker.java` | `roleOf` from `broker_superuser_tenants` |

---

## 15. Limitations & next work

- **L3 — dedicated per-tenant bridges** (blast-radius / physical isolation). Core isolation already holds
  via client-edge ACL; L3 is a high-assurance tier. The mechanism is a per-connector
  `setDynamicallyIncludedDestinations(T.<tenant>.>)` (the same lever `ActiveBroker.buildConnector` already
  uses for shard partitioning).
- **L4 — payload envelope encryption** (opt-in; protects against a compromised transit broker).
- **Advisory scoping** — close the destination-existence leak (§12), and inside a tenant give a node the advisories
  about its own inbox only (§12a).
- **Agent names unique across the fabric** — `region_alias` requires it and the alias inbox rule rests on it (§12a);
  nothing refuses, at enrolment, a name that another region of the tenant already holds.
- **Infra cross-tenant dataplane aggregation** — with topics qualified by local tenant, a global "god
  view" of all tenants' dataplane streams needs a `T.*.` wildcard *consumer* on infra (produce stays
  tenant-scoped). Built for the one stream the fabric itself needs — the regions' state exports at a global of a
  superuser tenant (above); not for application streams: per-tenant monitoring is arguably the more correct default
  under isolation.
- **App-destination enforcement** — application destinations that opt out of the `T.` prefix still fall to
  the legacy tenant rules; a strict mode could require every non-shared destination to be namespaced.
