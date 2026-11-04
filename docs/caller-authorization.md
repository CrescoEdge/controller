# Verified caller and action authorization

Cresco 1.3 hardening. Owner rulings this follows: every audit decision is mandatory, defaults fail closed
(2026-08-30), and no action that runs code, changes the fabric or destroys data runs for a caller the
transport has not identified.

## 1. The caller

An incoming `MsgEvent` carries three reserved parameters. Infrastructure sets them on the receiving side;
a sender can never set them.

| Parameter | Meaning |
|---|---|
| `cresco_caller` | `controller:<tenant>/<region>/<agent>`, `agent:<tenant>/<region>/<agent>`, `plugin:<tenant>/<region>/<agent>/<pluginid>`, or `api:<tenant>/<user>` |
| `cresco_caller_tenant` | The caller's tenant |
| `cresco_caller_roles` | A comma list drawn from `infra`, `admin`, `plugin`, `user` |

Any of these three names present in a message as it arrives off the wire is removed before stamping. A
value written by the sender is never read.

## 2. Who stamps

- **Broker path.** The broker stamps JMS string properties of the same names from the producer
  connection's authenticated identity (the peer-identity TLS client certificate). It overwrites anything
  already there. On a broker-to-broker bridge hop from a verified broker identity, the stamp set by the first
  broker is kept. The consumer (`AgentConsumer`) strips the body values and copies the properties in.
  Controllers (agent, region, global) get `infra`.
- **In-JVM path.** A controller delivering to its own plugins, or a plugin to its own controller, does not
  pass the broker. The controller stamps it: `infra` for controller-originated messages, and
  `plugin:<…>/<pluginid>` with `plugin` for plugin-originated ones.
- **On behalf of an API user.** wsapi authenticates its own clients and submits their messages through a
  library call, `PluginBuilder.msgOutOnBehalfOf(MsgEvent, Caller)`. The controller honours an on-behalf
  stamp only from bundles listed in `on_behalf_bundles` (default `io.cresco.wsapi`). The caller is
  `api:<tenant>/<user>` with `admin` or `user`, as wsapi's credential store says.

## 3. Enforcement

`PluginBuilder.msgIn` checks every message before it calls the executor. This is the one dispatch point
for every plugin and for the controller itself.

- `@CrescoAction(roles = {...})` names the roles allowed to call an action.
- An action with no `roles` is open to any caller whose tenant is this agent's tenant, and to `infra`.
  `admin` counts only when it belongs to the superuser tenant.
- A message with no caller, or a caller without a required role, is refused with `status_code=403`,
  `status_desc=unauthorized: <action> needs <roles>`. The refusal is logged, counted in
  `cresco.authz.denied{action}`, and the executor never runs.
- These actions require `infra` or `admin`, and nothing else:
  - plugin lifecycle: `pluginadd`, `pluginupload`, `pluginremove`;
  - controller and JVM: `controllerupdate`, `killjvm`, `restartframework`, `restartcontroller`;
  - agent state: `agent_disable`, `agent_enable`;
  - remote file access: `getfiledata`, `getfileinfo`;
  - executor: `config_process`, `run_process`;
  - filerepo: `clearrepo`, `putjar`, `removefile`, `putfiles`;
  - repo: `putjar`.

  Each plugin's owner may add actions to this list. None may be removed from it.

## 4. Defaults

- **Secure by default.** `broker_security_enabled`, `broker_require_client_auth` and `tenant_namespacing`
  all default to true.
- **Refuse to start** with security on but client auth or namespacing off.
- **Insecure development mode** exists only when `broker_security_enabled=false` is set explicitly. Then
  every boot logs a WARNING that security is disabled, and the caller is `anonymous` with every role.
- **Remote file reads** (`getfiledata` and `getfileinfo`) are denied unless `file_access_roots` is set.
  Paths are resolved to real paths, so symlinks cannot escape a root.

## 5. Code arriving over the network

Jars arriving through `putjar`, `pluginupload`, or `pluginadd` from a repository must carry a valid JAR
signature that chains to `plugin_signing_truststore`. The receiver computes the hash itself; a hash the
caller supplies is advisory only. Jars embedded in the agent jar are trusted as part of it.
`plugin_signing_required=false` is a development-only escape and is logged as a WARNING on every boot.

## 6. Broker and registry hardening (round-4 review, phase 1)

These change nothing for a well-behaved node; they close what a misbehaving or broken one could do.

- **Destination administration is authorized.** Removing a destination (`removeDestination`, a `DestinationInfo` REMOVE),
  creating one, and removing a durable subscription are checked like consumers and producers: a connection may remove only
  a destination it can both read and write, never a fabric-shared or advisory one, and a temporary destination only when it
  owns it; a durable subscription only with its own client id. Infrastructure (this controller, verified bridges) is exempt
  as before.
- **A certificate that passes TLS but carries no verified identity** (unbound, rebound away, foreign CA) is quarantined like
  a connection without a certificate (it receives nothing, its sends are discarded) and is still refused outright on
  everything but advisory and fabric-shared destinations.
- **A key that was rebound away is refused at TLS at once** (the trust manager asks the registry), and its truststore anchors
  are removed and the broker's trust refreshed.
- **Quarantined connections are bounded**: at most `broker_quarantine_max_connections` (64) at once, each with at most
  `broker_quarantine_max_consumers` (4) consumers and `broker_quarantine_max_producers` (4) producers, no composite
  destinations other than the client's own advisory consumer, and at most `broker_quarantine_max_destinations` (256)
  destinations created by such connections over the broker's lifetime.
- **Live peers are not pruned**: every verified send and subscription refreshes the peer's last-seen, and a sweep every
  `peer_seen_refresh_ms` (1 h) refreshes every verified connection that is up, idle or not.
- **`global.event` is not fabric-shared when tenants are namespaced** (audit D2a): with `tenant_namespacing` on,
  `broker_shared_destinations` defaults to `agent.event,region.event` -- a tenant's dataplane is `T.<tenant>.global.event`, and
  the un-namespaced fabric-wide topic would carry data across tenants. With namespacing off the dataplane IS `global.event`
  (it has no tenant boundary to keep), so it stays shared there until namespacing becomes the default with security.
- **Same-region writes stay in the writer's tenant** (audit D2b): with namespacing off, a write into another inbox of the
  same region is allowed only when that inbox's name is bound to the writer's tenant.
- **The peer trust registry never serves what is not durable.** Each change is written and fsynced under the write lock before
  memory changes or the change is acknowledged; a failed write or fsync is truncated away at once (later appends never land
  behind a broken line); a bind and the rebind token it consumes are one write; a crash between a compaction and its new
  head anchor is recognised (the compacted log names the replaced log's last MACs) instead of sealing the registry. One
  writer per store: opening a store fences the instance this JVM had open, an exclusive file lock (`peer-trust.json.lock`)
  keeps a second process out, and re-created components share the open instance.
- **A region declares its global lost** when the global stops answering its pings: by default the region judges its global
  link by pong age (`health_link_region_ping`, now default true; threshold `health_link_region_stale_ms`, default three ping
  intervals and at least 30 s), not only by its own bridge, which a region whose global is reached over another region's
  route never had.
- **Dataplane refusals are a declared type**: `DataPlaneService.addMessageListener` throws
  `io.cresco.library.data.SubscriptionRefusedException` (a `SecurityException`) when the broker refuses a subscription by
  policy, and returns null for a transient failure.
- **RPC replies are bound to their callee**: a reply whose verified caller is not the callee the call was addressed to is
  dropped (the call keeps waiting). Until callers are stamped (phase 3) a reply without a stamp is accepted as before.
- **A stopping plugin never waits for the AgentService** (OUT-111): `PluginBuilder.getAgentService()` returns at once while
  its bundle or the framework is stopping, so a JVM stop no longer ends in SIGKILL.
- The unused c3p0 0.9.1.1 dependency is gone from the library bundle.

### Operator-visible changes so far

| Setting | Before | Now |
|---|---|---|
| `broker_shared_destinations` default | `agent.event,region.event,global.event` | `agent.event,region.event` when `tenant_namespacing` is on |
| `health_link_region_ping` default | false | true (`health_link_region_stale_ms` added) |
| `broker_quarantine_max_{connections,consumers,producers,destinations}` | (none) | 64 / 4 / 4 / 256 |
| `peer_seen_refresh_ms` | (none) | 3600000 |
| `peer-trust.json.lock` | (none) | created next to the registry; two processes cannot share a registry |

## 7. Discovery, trust anchors and identity lifecycle (phase 2)

These change code that only this branch runs (discovery v2, the peer trust registry, the regional CA).

- **Discovery v2 binds every certificate of both chains** (round-4 H1). MAC "S1" covers the responder's whole presented
  chain; the client's PROOF ("C") and the RESULT MAC ("S2") also cover the client's whole chain, exactly as each message
  carries it. A man in the middle that re-issues either side's leaf (same key, same subject) under a CA of its own fails the
  exchange before anything is recorded, so it can no longer plant a trust anchor or replace a joining region's CA.
- **Responders are ordered only by authenticated facts** (round-4 M5): arrival order, except that an address this node
  already completed an authenticated exchange with is tried first. A self-reported agent count or a claimed name orders
  nothing (round 3's test that preferred them is rewritten to this rule).
- **Joiners pin the controller they joined** (item 7). An agent pins its region controller's key (and, under the regional
  CA, the region CA) at the first join; a region pins its global the same way. A different key or CA for that name later is
  refused: holding the shared agent secret no longer lets anyone pose as a controller. A re-installed controller is
  accepted again when its `security/` directory is restored (same key), when its new leaf is vouched for (§8), or after the
  operator authorises one rebind of it on each joiner (`peer_identity_rebind=<region>/<agent>=<key fingerprint>`, or the line
  in `peer-trust.json.rebind`, no restart). Controller pins never lapse (§8).
  `controller_trust_anchors` (SHA-256 of a controller's public key or of a certificate in its chain, comma separated)
  closes the first join as well: only a listed controller is joined.
- **Public-key directory** (audit D6): `public_key_directory` (default `<agent data dir>/public-keys`, absolute) still
  receives this node's exported certificate, but a certificate placed there becomes a trust anchor only when its SHA-256
  fingerprint is listed in `public_key_trusted_fingerprints`, the directory is absolute and neither it nor the file is
  writable by group or others (or owned by another user), and the file is not a symlink.
- **Renewal** (round-4 M4, corrected in §8): the persisted node chain and region CA are renewed with the SAME keys when a
  third of their own lifetime is left (or `identity_renew_before_days`, 30, if that is more -- but never more than half the
  lifetime) -- at start and every `identity_renew_check_minutes` (60). Bindings and recorded CA scopes follow keys, so peers
  notice nothing. Connections that reconnect present the current chain.
- **Short-lived region-issued leaves**: a region CA issues leaves valid for `identity_leaf_validity_hours` (168 = 7 days).
  A node re-enrolls with the controller it joined (one authenticated exchange, same key) at half-life; see §8 for what
  happens when that keeps failing (no self-made fallback).
- **Revocation**: `peer_identity_revoke` (names `region/agent` or key fingerprints) and the operator file
  `peer-trust.json.revoke` (re-read every 10 s) revoke a key for every name: the binding is dropped, TLS, the broker and the
  CA path refuse it, live connections holding it are closed at once, and it stays revoked (persisted, permanent; a revoked
  node needs a new key). Each controller signs its revocation list with its identity key and it travels on the parent/child
  pings as deltas (§8): a region's list is accepted at the global only from a controller bound there, for the region's own
  names, and the global's (and a region's) list applies to everything below it. Agents' lists are not accepted.
- **Expired identities**: the broker refuses an expired certificate at authorization (both the bound-key and the CA path)
  and closes, within a minute, a connection whose certificate expires or whose key is revoked while it is up.
- **The broker's identity check is the authoritative gate.** A client that reconnects by resuming a TLS 1.3 session (a
  stateless session ticket) is not re-checked by the TLS trust manager; the broker re-verifies the presented certificate
  on every connection, resumed or not, so a revoked or expired key is quarantined there (no identity, no rights). Cached
  server sessions of a revoked key are invalidated as well.

| Setting | Before | Now |
|---|---|---|
| a joiner and a re-keyed controller | followed | refused until restored identity or operator rebind |
| `controller_trust_anchors` | (none) | optional first-join anchors |
| `public_key_directory` default | `cresco-data/public-keys` (relative) | `<agent data dir>/public-keys` |
| `public_key_trusted_fingerprints` | (every `.cer` imported) | required for any import |
| `identity_leaf_validity_hours` | 3 years | 168 |
| `identity_renew_before_days`, `identity_renew_check_minutes` | (no renewal) | 30, 60 |
| `peer_identity_revoke`, `peer-trust.json.revoke` | (none) | revocation |

## 8. Revocation, renewal and controller pins (identity review, 2026-11-04)

An independent adversarial review of phases 0-2 found that revocation could be submitted by any agent, reached across
regions, could be undone by re-claiming a name, did not cover CA keys or the bridges a node opened itself, failed open on a
write failure and did not scale; that renewal churned (a 7-day leaf was "due" at birth: hourly re-issue, truststore growth,
re-enrollment dropping every connection, a silent 3-year self-rooted fallback); and that a joiner's controller pin lapsed
after 90 days. Its tests (`ReviewIdentityRevocationTest`) pass unchanged; `IdentityRevocationFixTest` covers the rest.

- **Who may send a revocation list.** A list is accepted only when signed with the key BOUND at the receiver as a
  CONTROLLER to the signer's name at the global, from a region controller that joined it (never the global's own region,
  never its own parent); at a child, only from the controller it joined (whose name comes from the child's own state),
  verified by its bound key or, under the regional CA, by its region's recorded CA (which issues a controller's name to that
  controller only), never by a key bound there as an agent's. The issuer
  is the identity the signing certificate verifies as, never `src_region`/`src_agent`; a broker-stamped `cresco_caller`, when
  present, must be that same controller. An agent's list is refused everywhere, whatever the ping says.
- **Positive scope.** A region's entry is accepted at the global only when its key is bound there to a name of that region
  (and no other), or is bound to no name there and the entry names an identity of that region; it is then recorded as
  `fingerprint@region` and applies to that region's names ONLY -- at the global and, carried down in the global's list, at
  every region. A CA key, a key bound outside the region, or an entry naming no identity of the region is refused.
- **A revoked name is tombstoned.** Revoking a key drops the bindings that hold it and reserves their names: no key (old or
  new) is accepted for the name -- at TLS, at the broker, on the CA path, or by first-come binding -- until the operator
  rebinds it. A revoked controller name also loses its bridge-peer record. The revoked key itself stays revoked after a rebind.
- **CA keys can be revoked.** Revoking a region CA's key fingerprint stops it vouching: no leaf it issued verifies, it is
  never re-added as a trust anchor at start (`recordedCas`), no other CA is recorded for that region (even by a client
  following its region) until the operator rebinds the region, and a chain carrying a revoked CA key anywhere is refused. A
  federating controller whose binding is refused leaves nothing recorded (its CA is restored).
- **Region-CA names need CA-issued leaves.** Where a region CA is recorded, a name of that region verifies only with a leaf
  issued by that CA and within its validity -- also when its key is bound (the bound-key path no longer accepts a chain the
  key holder made itself). An identity its region stops re-enrolling (a node gone, refused, or a stolen key) lapses with its
  last leaf. Under per-node trust (no region CA) leaves are self-rooted by design: a bound key stays valid until revoked.
- **Revocation tears down what this node opened.** The trust manager of every outbound connection records it; a
  revocation closes each one whose peer is now refused (bridges and client connections, after invalidating the TLS session
  so a reconnect cannot resume it unchecked), removes the bridge groups to tombstoned peers, and the broker closes inbound
  connections refused for any revocation reason (key, name, region CA), not only by key.
- **Fail closed.** A revocation is in force in memory before it is recorded. When it cannot be recorded (full disk, a sealed
  or fenced registry) it stays in force, the write is retried (1 s, doubling to 30 s) and the `identity` health check is
  CRITICAL until it is recorded. The operator file counts as applied only once its entries are recorded (else it is read
  again in 10 s). A node never revokes its own key or own region CA, by name or by fingerprint.
- **Versioned lists, O(1) pings.** Each registry has an epoch (random, made with it, kept across restarts and compaction;
  new after a `reset`). Entries have positions; a ping says `<epoch>.<position>` held of the parent's list and the pong
  carries only what follows (at most `revocation_sync_max_entries`, 4096, per pong) -- nothing when unchanged. A region
  sends its own entries after what its parent acknowledged; the pong always names the parent's epoch, and a child that sees
  it change (the parent re-created or reset its registry) sends its list again. Signed deltas are cached; only durable
  entries are served (a position is never reused). Merging is in force at once and recorded off the ping path in ONE
  durable write (1,000 entries: ~30 ms instead of 8-17 s), so a bulk merge never delays a pong.
- **Expiry.** A revocation of a key this region's CA enrolled expires once every leaf it could have issued for it has
  (`identity_leaf_validity_hours` + twice the backdate + a day); the name stays tombstoned. Other revocations are permanent.
- **Controller pins never lapse.** `peer_binding_prune_days` prunes AGENT bindings only; a controller this node joined (its
  pin), one that joined it, and this node's own identity are kept until an operator rebinds or revokes them.
- **Renewal without churn.** Renewal is at a fraction of the certificate's own lifetime (above). This node's own chain,
  region CA, issuing CA and a peer's rediscovered chain REPLACE their truststore entries. A re-enrollment with the same key
  and CA keeps every live connection (the key managers present the new leaf at the next handshake); an agent enrollment no
  longer rewrites the region broker's TLS context. Every join path re-enrolls -- multicast and static
  `regional_controller_host` / `global_controller_host` alike.
- **No silent fallback.** A region-issued leaf is never replaced by a self-made chain (at renewal or at start). When
  re-enrollment keeps failing the node keeps serving its last valid leaf: WARN, then CRITICAL in its last tenth (retrying
  every `identity_reenroll_retry_minutes`, 5). At expiry the leaf lapses -- every verifier refuses it and the region's broker
  closes the connection -- and the node keeps trying; the region re-enrolls a bound, unrevoked key whatever its last leaf's
  validity, so it returns by itself as soon as its region's discovery port answers. Trade-off: connectivity is kept as long
  as the identity is valid and never extended past what the region vouched for; a node that cannot reach its region's
  discovery port for half a leaf's lifetime (3.5 days by default) loses its broker connection until it can (raise
  `identity_leaf_validity_hours` where links are that unreliable).
- **Re-installed controllers (one action).** A rebind token may name the key it allows: `<region>/<agent>=<fingerprint>`
  (`#tag` as before); a token without one rebinds to the first key presented and is logged as such. A joiner follows its
  pinned controller to a new key WITHOUT a token when the new leaf is vouched for: issued by the CA it pinned for that
  region (a region controller re-installed with its `region-ca.p12` restored), or carrying a certificate or key listed in
  `controller_trust_anchors` -- never for a revoked (tombstoned) name or key.
- **Clocks.** Certificates are backdated `identity_cert_backdate_hours` (24; was a fixed hour), so a clock lagging less than
  that accepts a certificate issued "now". Pongs (`remote_ts`) and pings (the child's clock) are compared: a skew of half the
  backdate is WARN, the full backdate CRITICAL (`identity` health); an enrollment refused as not yet valid says the clock is
  behind.
- **A registry that cannot lock its store** (another process, or a file system without locks such as some NFS mounts) is
  fenced LOUDLY: an ERROR naming the cause, the `identity` health check CRITICAL, and every refused binding says
  `FENCED: <why>`; revocations are still enforced in memory.

| Setting / signal | Before | Now |
|---|---|---|
| renewal point | when max(30 d, life/3) is left (a 7-day leaf: at birth) | max(life/3, `identity_renew_before_days`), at most life/2 (7-day leaf: half-life) |
| region-issued leaf, re-enrollment failing | self-rooted 3-year chain in the last tenth | last valid leaf kept; WARN, CRITICAL in the last tenth; lapses at expiry |
| `identity_reenroll_retry_minutes` | (none) | 5 (in a leaf's last tenth) |
| `identity_cert_backdate_hours` | fixed 1 h | 24 |
| `revocation_sync_max_entries` | (whole list on every changed ping) | 4096 entries per pong, deltas |
| revocation list wire | `rev_have`/`rev_ack` = digest of the whole list; v1 lists | `<epoch>.<position>`; `rev_epoch`, `rev_ts`; v2 signed deltas (only this branch spoke v1) |
| `peer_binding_prune_days` | every binding but this node's own | AGENT bindings only |
| rebind token | `<region>/<agent>[#tag]` | also `<region>/<agent>[#tag]=<key fingerprint>` (recommended) |
| a revoked name | free again at once | tombstoned until an operator rebind |
| health check `identity` | (none) | CRITICAL: registry fenced/sealed, revocations not recorded, own leaf expired or in its last tenth with re-enrollment failing, clock skew > backdate |
