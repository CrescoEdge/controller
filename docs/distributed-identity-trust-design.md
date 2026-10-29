# Cresco — Distributed Identity, Trust & Data-Rights Design

**Date:** 2026-07-02
**Status:** Phase 0 (identity/sign/trust primitives) + a working slice of Phase 2 (broker tenant
authorization) **SHIPPED and PROVEN LIVE** in a 2-tenant / 2-region / 1-global federated mesh
(`run/tests/tenant_isolation_mesh_test.sh` 10/10; `security_foundation_test.sh` 15/15). Remaining:
mutual-TLS *binding* of identity at the broker, regional-CA *issuance*, tenant dataplane namespacing
(§5). All enforcement is behind `broker_security_enabled` (default OFF → fabric unchanged).

### What shipped (this pass)
- **library `io.cresco.library.security`:** `CrescoIdentity` (identity⇄cert-DN), `MessageSigner`
  (sign/verify payloads), `CertTrust` (PKIX chain-verify to trusted region CAs). Pure JDK, additive.
- **controller:** `TenantPolicy` (pure ACL logic) + `CrescoAuthorizationBroker` (ActiveMQ 6.2.7
  `BrokerFilter`/`BrokerPlugin`), wired in `ActiveBroker` behind `broker_security_enabled`;
  `ActiveClient` asserts identity (`tenant|region|agent`) as the JMS username on network connections.
- **Proven live:** federation bridges form under security (0 self-denial); tenantB is denied tenantA's
  namespace at *every* broker in the mesh (cross-region cross-tenant); each tenant free in its own;
  RTT/scaling machinery intact. Local `vm://` controller + broker bridges are exempt by design.
- **Cryptographic identity binding — SHIPPED + PROVEN (single broker).** Leaf certificates are now
  identity-bearing (`CertificateManager` issues `CN=<agent>, OU=<region>, O=<tenant>`), the broker
  connector takes `needClientAuth` (`broker_require_client_auth`), and `CrescoAuthorizationBroker.
  addConnection` derives the principal from the validated client-cert DN — authoritative, overriding any
  self-asserted username. Proven live: a client with **no certificate is rejected** by the handshake; a
  client presenting a **tenantB cert but a spoofed `tenantA` username is treated as tenantB**
  (broker log: `mTLS identity bound from certificate: CrescoIdentity{tenant=tenantB…}` →
  `DENY READ 'tenantA.stream' … outside tenant 'tenantB'`). Anti-spoof, end to end.
- **Fabric-wide mutual TLS via regional CAs — SHIPPED + PROVEN (2026-07-03).** The trust-distribution
  boundary is closed. Regional-CA enrollment (`security_regional_ca`) rides the existing
  discovery-secret-authenticated cert exchange: the responder (a region/global acting as issuer) signs
  the joiner a leaf under its CA (`RegionCA` / `CertificateManager.issueEnrollment`, region stamps
  identity) and returns the chain; the joiner installs it (`installEnrollment`) and trusts the issuing
  CA that rides inside the chain — so trust is **O(regions)** (region-CA certs), not O(nodes²). With
  every node then presenting a CA-signed leaf, `broker_require_client_auth` validates any peer by chain.
  Proven: `run/tests/regional_ca_mtls_test.sh` **10/10** — a global establishes a region CA, two regions
  (tenantA, tenantB) enroll + install global-CA-signed identities and **bridge to the global under
  mutual TLS**, 0 TLS trust failures, healthy federation RTT. (Key fix: `ActiveClient.refreshTrust`
  rebuilds only *network* factories and preserves `vm://localhost` — closing it dropped the RPC reply
  consumer and the ping PONG never returned.)
**Supersedes:** the "B-7 broker-auth" item in `broken-and-untouched-report.md` (which framed this as
a single broker-auth switch). It is not one switch — it is a distributed security fabric.

## 1. Requirements (as stated)

1. **Tenant isolation** — *who can see what, who can do what.* Data-access rights enforced at the broker.
2. **Mutual identity** — an agent must be able to **identify itself** and **be identified by other agents**.
3. **Trust establishment** — nodes must establish trust with each other across the mesh.
4. **Data rights secured broker-to-broker** — protection must survive relaying/bridging across the network,
   not just the first TLS hop.
5. **Optional signing / encryption** of messages where needed.
6. **Scale** — a *huge mesh of interconnected regions* and *multiple globals*. **A central CA will not
   scale**; issuance must be **distributed, at the regional level**.

Mechanisms (cert exchange / signing / encryption) are open — the recommendation below is my judgment.

## 2. What exists today (code-grounded)

- **Certs:** every node self-generates a **3-tier chain** in `CertificateManager.generateCertChain()`
  (`controller/.../communication/CertificateManager.java:229`): `rootCA` (self-signed) → `intermedCA`
  (signed by rootCA) → `endUserCert` (leaf, signed by intermedCA), BouncyCastle RSA. The leaf CN is a
  throwaway UUID (`CN=endUserCert-message-<uuid>`) — **carries no identity.**
- **Trust:** peer-to-peer — each node writes its public leaf cert to a shared dir as `<region>_<agent>.cer`
  and imports everyone else's (`CertificateManager` export/import, ~lines 149-197). Identity lives in the
  **filename**, not the cert. **O(agents²)** trust material; no CA relationship.
- **Transport:** ActiveMQ `nio+ssl` with the node's chain as key/trust material. **Server-auth only** —
  `needClientAuth` is set **nowhere**; clients don't present certs.
- **Connections are anonymous:** `createConnection()` is called with **no credentials**
  (`ActiveClient.java:114,189`). `broker.setUseAuthenticatedPrincipalForJMSXUserID(true)` is already set
  (`ActiveBroker.java:283`) — waiting for an authenticated principal that never arrives.
- **Topology:** each region runs its **own broker**; agents are broker **clients**; region↔global and
  global↔global federate via broker-to-broker **bridges** (`ActiveBroker.AddNetworkConnector`).
- **Destinations:** per-agent **queue `<region>_<agent>`** is the one identity-scoped name (the strong
  isolation point); dataplane **topics** `agent.event`/`region.event`/`global.event` are flat (not scoped).
- **Enrollment gate already exists:** discovery carries `discovery_secret_region` / `_global` — a
  pre-shared join secret proving a node is allowed into a region/global.

The 3-tier chain, the region-broker/global-bridge topology, the join secret, and the
`useAuthenticatedPrincipalForJMSXUserID` flag are **exactly the primitives this design needs.** This is a
restructuring, not a greenfield build.

## 3. Recommended architecture — Regional CAs + federated trust bundles

Three layers, defense-in-depth. Each is independently valuable and separately switch-able.

### 3.1 Identity — make the certificate self-describing
Bind identity **into** the leaf cert DN instead of a sidecar filename:
```
Subject:  CN=<agent>, OU=<region>, O=<tenant>, UID=<stable-node-uuid>
```
Now every cert cryptographically asserts *"I am agent A, in region R, tenant T"*, verifiable by anyone who
trusts the issuer. This is the cornerstone — every option below depends on it, and it is Phase-0 additive
(subject content doesn't affect the current whole-cert-import trust, so it can't break today's handshake).

### 3.2 Trust — the region **is** the CA; globals distribute trust, they do not sign
The scaling insight: **separate issuance from trust distribution.**

- **Issuance is regional.** The regional controller holds a **persistent region CA** (its `intermedCA`
  tier, promoted from throwaway to a durable signing authority). Agents enroll by submitting a CSR during
  the *existing* discovery/registration handshake; the join secret (`discovery_secret_region`) is the
  enrollment gate. The region CA signs the agent's leaf (identity in DN) and returns leaf + region-CA cert
  + the region's trust bundle. **No round-trip to any central authority** → issuance scales horizontally,
  works under partition, and has no single point of compromise.

- **Trust anchors are region CA certs — not leaf certs.** A node trusts *"any leaf that chains to a region
  CA in my bundle."* Trust material collapses from **O(agents)** to **O(regions you federate with)** — one
  CA cert per region instead of one leaf per agent. This is the key scale win.

- **Globals are trust-list *introducers/aggregators*, not signers.** When region R joins global G, R
  publishes its (self-signed, identity-bound) **region CA cert** to G. G maintains the **regional trust
  bundle** for its domain and distributes it to member regions. This replaces the fragile per-agent
  filename exchange with **per-region CA distribution.**

- **Cross-global mesh = bundle gossip.** Globals exchange their regional bundles with peer globals (subject
  to tenant/trust policy). An agent under G1 can verify an agent under G2 because G1 imported G2's bundle.
  Federation is **bundle-level** (a handful of CA certs per global), so it scales with the number of
  *federating domains*, not nodes.

- **Optional fabric root (opt-in).** Deployments wanting a single cryptographic anchor can have region CAs
  **cross-signed by an offline fabric root**. Then trust = *chain-to-fabric-root* and bundle distribution
  becomes an optimization rather than a requirement. The root signs region CAs **at region birth only**
  (not per agent), so it is never a runtime bottleneck. Kept optional so the default stays fully distributed.

**Why not the alternatives:**
- *Central CA / global-signs-everything* — the user's rejected case: per-agent enrollment round-trips to
  global, global compromise = total compromise, global is an enrollment SPOF. ✗
- *Pure per-agent web-of-trust* (today's model) — O(agents²) cert exchange; does not scale to a huge mesh. ✗
- *Regional CAs + federated bundles* (this) — regional issuance + O(regions) trust + optional root. ✓

### 3.3 Data rights — enforce at every broker, and end-to-end when needed

- **Transport (default): mutual TLS.** Turn on `needClientAuth` so the broker requires a client cert,
  validates it chains to a trusted region CA, and extracts the DN → **authenticated principal**. With
  `useAuthenticatedPrincipalForJMSXUserID(true)` (already on), every message is stamped with a *verified*
  origin identity the broker trusts.

- **Authorization: `CrescoAuthorizationBroker`** — an ActiveMQ `BrokerFilter` plugin that checks the
  authenticated principal (tenant/region/agent parsed from the DN) against destination ACLs on
  `addConsumer`/`addProducer`/`send`:
  - an agent may pub/sub **only** its own queue `<region>_<agent>` and its region/tenant-scoped topics;
  - cross-tenant destinations → **deny**;
  - `vm://localhost` (the controller's own in-JVM connection) → full local rights (bounds blast radius:
    the local agent keeps working; only *network* clients are constrained).

- **Tenant isolation on shared topics (Phase 3): namespace destinations by tenant** — `T<tenant>.region.event`,
  `T<tenant>.<region>_<agent>`. ACLs and bridges become simple **prefix rules**; a bridge only forwards its
  tenant namespace, so even a shared global broker cannot leak across tenants.

- **End-to-end across bridges (the "broker-to-broker" requirement): selective message signing.** Transport
  mTLS protects one hop; a control/data message that transits region→global→region crosses multiple broker
  trust domains. For those, the **sender signs the payload digest with its leaf private key**; any receiver
  verifies with the sender's leaf cert (chain-validated to a trusted region CA). The signature proves origin
  identity **and** tamper-evidence independent of how many brokers relayed it. **Selective** (per-message
  flag) because signing every message is costly — mTLS is the default, signing is the elevated tier for
  cross-domain / sensitive traffic.

- **Confidentiality beyond transport (optional): payload encryption** to the recipient's public key (from
  their leaf cert) for messages that must stay opaque to an intermediate broker. Expensive; use selectively.

## 4. Lifecycle at scale

- **Enrollment:** agent generates keypair → CSR(identity) → submits to region CA over the existing
  discovery handshake, gated by the join secret → receives signed leaf + region CA cert + trust bundle.
- **Revocation by expiry:** short-lived leaves (hours/days) + auto re-enrollment. A compromised node simply
  isn't renewed; access lapses fast — no mesh-wide CRL/OCSP to scale. A small region-published revocation
  delta rides along in the bundle for immediate kills.
- **Rotation:** region CA rollover overlaps old+new CA certs in the bundle; leaf rotation is just
  re-enrollment. Fabric-root (if used) signs region CAs rarely and offline.

## 5. Phasing (nothing below is flipped on a live mesh blind — each is flag-gated, staged region-by-region)

| Phase | Deliverable | Risk | Flag |
|-------|-------------|------|------|
| **0 ✅ DONE** | Identity-bearing leaf DN; `io.cresco.library.security` identity/chain-verify + **sign/verify** primitive | additive, none | n/a |
| **2 ✅ SHIPPED** | `CrescoAuthorizationBroker` tenant ACLs (proven in federated mesh) + broker mutual-TLS `needClientAuth` cert-DN binding (proven single-broker) | high (fabric) | `broker_security_enabled`, `broker_require_client_auth` |
| **1 ✅ SHIPPED** | Region-CA issuance at enrollment (over the discovery-secret exchange) + chain-based trust — fabric-wide mutual TLS proven (`regional_ca_mtls_test.sh` 10/10) | medium | `security_regional_ca` |
| **3 ← NEXT** | Tenant destination namespacing + bridge tenant filtering (multi-tenant dataplane isolation); cross-region region-CA bundle distribution (global→regions, global↔global) | high | `tenant_namespacing` |
| 4 | Revocation/rotation automation; selective payload encryption | medium | per-feature |

**Phase 2 is the fabric-breaking one** — mТLS + ACLs mean a misconfigured principal/ACL denies real
traffic. It ships only after Phase 1 gives every node an identity-bound cert chaining to a distributed
region CA, and it is enabled per-region behind `broker_security_enabled` with the local `vm://` path always
allowed, so a bad rule can never lock a controller out of its own broker.

## 6. Test matrix (per phase, before enable-by-default)
- Identity: leaf DN parses to correct tenant/region/agent; peer cert chain-verifies to region CA; **fails**
  for a cert from an untrusted region.
- Signing: verify passes for untampered payload, **fails** on a flipped bit or wrong signer.
- Enrollment: agent with valid join secret gets a chaining cert; wrong/missing secret is refused.
- Authz: agent reads/writes its own queue; **denied** on another agent's queue and a cross-tenant topic;
  local `vm://` retains full rights; region bridge still federates.
- Scale/federation: region joins a global and its CA propagates; two globals exchange bundles; an agent
  under G1 verifies an agent under G2; revoked/expired leaf is rejected fabric-wide within the TTL window.
- Regression: the full existing bring-up/recovery suite stays green with each flag **off** (default) and,
  separately, **on**.

---

## 7. Peer identity binding and stream recipient binding (OUT-28, 2026-10-28; reviewed twice)

### 7.1 What was verified before
`CertificateManager.getTrustManagers()` is the JDK PKIX `TrustManagerFactory` over the node's truststore, and every
broker client URI carried `verifyHostName=false`. A TLS connection was accepted when the peer's chain reached ANY
anchor in the truststore -- which holds every peer that ever did a discovery exchange (per-node trust: each peer's own
self-made root CA; regional CA: every region CA) -- so any holder of a fabric certificate could answer for any
controller or mint any name. Any client could claim to be an authorization-exempt bridge (a `BrokerInfo`), identity
came from the certificate name alone or from a typed username, discovery was authenticated by a constant replayable
token, and the discovery port deserialized arbitrary Java classes (fixed separately: `fix/discovery-deserialization`).

### 7.2 Names
One grammar for tenant, region, agent and uid (`CrescoNames`): 1-64 of `A-Z a-z 0-9 . -`, starting and ending with a
letter or digit; no spaces, `#`, DN specials, control characters or `_` (the separator in `<region>_<agent>`).
Certificates are built from attribute values and read strictly; a regional CA re-runs its guards on the identity it
issued; `cresco_to` is exactly two valid names. Certificates are backdated an hour (peers with slow clocks).

### 7.3 Who may hold a name (`PeerTrustRegistry`)
**The discovery secret proves fabric membership, not a name.** Every agent holds the agent secret, so an agent exchange
cannot prove which agent is speaking. The rules are therefore scoped:
- **A responder** (a controller others join) binds an AGENT joiner first come, only inside its own region and tenant,
  never to a controller name (its own -- reserved before it answers -- or one bound as a controller) and never in a
  superuser tenant (`broker_superuser_tenants`; such identities are provisioned, not claimed). A REGION/GLOBAL joiner is
  bound first come as a controller. Under the regional CA the same rules apply to what the CA issues, and a region CA
  vouches only for its own region and tenant (one CA per region).
- **A client** (the node that joined) binds the controller it joined -- which must be the controller of the region and
  tenant it joined -- and FOLLOWS its key (and CA) through authenticated discovery: a re-installed controller is accepted
  and logged. A joiner cannot tell a re-install from anything else; refusing would cut every agent off until each was
  reconfigured. An agent can therefore impersonate a controller to OTHER joiners if it can answer their discovery (it
  holds the same secret): only per-name credentials (operator enrolment, or a CA whose key agents do not hold) close
  that, which is what the regional CA with persistent region CAs gives the controllers.
- **Re-admitting a re-installed node** at a responder is ONE operator action at that responder: add the name (or region
  for a CA) to `<data>/security/peer-trust.json.rebind` (re-read without a restart) or `peer_identity_rebind`. Each entry
  is one-shot (consumed and recorded); a later rebind of the same name needs a new tag (`rgA/a1#2`). The refusal log line
  says exactly this. A wiped global or region controller needs nothing at its clients (they follow it); a wiped region
  needs one rebind at the global; a wiped agent needs one rebind at its region.

A certificate's identity counts only when its key is bound to its name or it was issued by the CA recorded for that
name's region and tenant: outbound (`PeerIdentity`, every broker connection and bridge, bound to the identity discovery
returned for the address) and inbound (`CrescoAuthorizationBroker`).

**Store.** An append-only log of JSON lines with a chained HMAC-SHA-384 (`<data>/security/peer-trust.json`, key in
`.key`). Appends are group-committed (one fsync for all waiting binds, outside the lock) and acknowledged once durable;
reads are lock-free; a known binding's refresh writes nothing; last-seen times persist in the background at most daily;
the log compacts when it grows. Bindings unseen for `peer_binding_prune_days` (90) and bridge peers unseen for
`peer_bridge_prune_days` (30) are pruned; bridge peers are revocable (`peer_bridge_revoke`). Files and directory are
owner-only from creation and written atomically with file and directory fsync (also the node's `node-identity.p12`,
`region-ca.p12` and their `.pass` files). Measured on macOS/APFS: 10k bindings from 16 concurrent joiners 12 s, one
sequential new binding ~12 ms, reload 59 ms, bridge check ~230 ns.

**Failure.** A torn last line (a crash mid-append, never acknowledged) is dropped. Any other line that does not verify,
or the log missing while its key says it was initialised, SEALS the registry: what verified is still served
(established peers keep working), nothing new is bound, and the seal persists across restarts. Recovery is explicit:
write `accept` (keep every record that parses, re-sign it; use after inspecting) or `reset` (start empty: every name,
CA scope and bridge peer is learned again, first come -- the cost: any key may claim any name once) into
`peer-trust.json.recover`; it applies on the next refused bind or restart. The MAC key sits beside the log: it detects
corruption and edits by anyone who cannot read the data directory, NOT tampering by someone who can.

### 7.4 Discovery v2 (`DiscoveryAuth`)
```
client  HELLO     type, Nc, client region/agent, client chain
server  CHALLENGE Ns, server region/agent, server chain, broker port, MAC(K, "S1", T)
client  PROOF     MAC(K, "C", T)          -- server records/issues nothing before this verifies
server  RESULT    ok, reason, result chain, MAC(K, "S2", T, H(result))   -- client installs after this verifies
```
T covers the label, type, both nonces, both identities, both leaf-key SHA-384 fingerprints and the broker port.
K = PBKDF2-HMAC-SHA-384(secret, per-purpose salt, 210,000 iterations), cached (bounded, 64 keys); HMAC-SHA-384 in
constant time; 32-byte nonces. Locator validators are AES-256-GCM with a fresh IV. The discovery decoders deserialize
only `String`.

**Default and migration.** `discovery_auth=v2` (default). `compat` lets a node also use the legacy single-message
exchange -- for AGENT membership only: a legacy exchange never makes a bridge peer and controllers never fall back to it
(its token is the constant DISCOVER token anyone on the LAN may hold). Rolling upgrade: upgrade globals and regions
first (together), then agents, with `compat` on upgraded controllers while old agents remain; then remove it.

### 7.5 Stream recipients and broker identities
A message with `cresco_to=<region>_<agent>` (optionally `cresco_to_tenant`) is dispatched only to that agent's PROVEN
identity (a bound client certificate, or this broker's own controller over vm://), to verified bridges and to bridges'
local sides; any other subscription receives nothing, and one whose selector asks for another identity's stream is
refused. An advisory carrying a copy of a message (MessageDiscarded, ...) is judged as that message (its recipient and
the consumer's right to its destination's tenant). A username typed by a client without a certificate is never an
identity for `cresco_to` and never earns the bridge exemption; `broker_require_client_auth` therefore defaults to
`broker_security_enabled` (migration: an existing secured fabric without client certificates logs a loud warning;
`broker_allow_unverified_bridges=true` keeps the old bridge exemption until client certificates are on). The
`cresco-local|...` vm:// identity is honoured only for this broker's own controller. Refusals reach clients: the
dataplane throws `SecurityException`, a listener refused on reconnect is told (JMS `ExceptionListener`), wsapi answers
status 40 or closes the stream with 1008, and pycrescolib (`DataplaneSubscriptionRefused`, `refused_callback`) and
clientlib (`SubscriptionRefusedException`, `onSubscriptionRefused`, `isReceiving()`) report every refusal, reconnects
included.

**Senders (GaiaKeep GKT):** set `cresco_to` to the recipient agent's path (the agent whose dataplane connection the
receiving plugin uses), and `cresco_to_tenant` under tenant namespacing; only after the brokers and agents involved are
upgraded, with client certificates on.

### 7.6 Settings and modes
| Setting | Default | |
|---|---|---|
| `peer_identity_mode` | `enforce` | `warn`: log and accept, outbound and inbound (an unbound certificate is accepted by name); `off`: the old connection and broker behaviour exactly. Names, CA uniqueness/scope, discovery v2 and the registry still apply under `off`. |
| `broker_require_client_auth` | = `broker_security_enabled` | client certificates; without them identities are unverified |
| `broker_allow_unverified_bridges` | `false` | migration only |
| `discovery_auth` | `v2` | `compat`: legacy AGENT membership during an upgrade |
| `peer_identity_rebind` / `<data>/security/peer-trust.json.rebind` | empty | one-shot rebind tokens |
| `<data>/security/peer-trust.json.recover` | -- | `accept` or `reset` a sealed registry |
| `peer_bridge_revoke`, `peer_bridge_prune_days`, `peer_binding_prune_days` | empty, 30, 90 | |
| `peer_trust_persist`, `node_identity_persist`, `node_identity_dir` | true, true, `<data>/security` | |
| `security_federated_tenants` | any | tenants a regional-CA controller federates with |
| `broker_stream_recipient_binding` | true | with `broker_security_enabled` |

### 7.7 Tests
Unit and real-TLS/real-broker: `PeerIdentityPolicyTest`, `StreamRecipientPolicyTest`, `PeerImpersonationTest`,
`PeerIdentityBindingTest`, `StreamRecipientBindingTest`, `CertificateIdentityTest`, `PeerTrustPersistenceTest`,
`PeerTrustStoreTest`, `DiscoveryPeerLearningTest`, `DiscoveryWireTest`, `DataPlaneRefusalTest`, `BrokeredAgentRefreshTest`,
and the two independent reviews' `AdversarialPeerIdentityReviewTest` and `SecondReview*Test`. Live:
`docs/tests/peer_identity_live_test.sh` (per-node trust, `REGIONAL_CA=1`, `AGENT_REGIONS=1`).
