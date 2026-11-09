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
for every plugin and for the controller's agent tier; the controller's regional and global executors are checked
the same way at `RegionalExecutor.dispatch`, each against its own table (`ControllerActionRoles`).

Terms: `C` is the caller's tenant (stamped), `D` the destination agent's own tenant (`tenant_id` of the node
running the action), `S` the superuser tenants (`broker_superuser_tenants`, default `cresco-system`). The key
of an action is (MsgEvent type, `action`).

- No caller, or a malformed stamp: refused, whatever the action.
- `anonymous` (only with `broker_security_enabled=false`, set explicitly): allowed; nothing is refused for its caller.
- An **open** action (no roles) admits a caller when `C == D`; or when it holds `infra` or `admin` and `C` is in `S`;
  or when it holds `infra` and the action admits every tenant's infrastructure (fabric mechanics on the controller
  tiers only: liveness `ping`, registration `agent_enable`/`agent_disable`/`region_enable`/`region_disable`/
  `regionalimport`, WATCHDOG and KPI keep-alives, `getcryptopolicy` at the global). `setinodestatus` is a scheduling
  action (`infra` or `admin` of the global's tenant, or a superuser tenant), not a fabric mechanic.
- A **role** action (`@CrescoAction(roles = {...})`, the controller tables, or the privileged floor) admits a caller
  holding one of the roles in scope: `C == D`; or the role is `infra` or `admin` and `C` is in `S`; or the role is
  `infra` and the action admits every tenant's infrastructure.
- So an admin of tenant T acts only on agents of T unless T is a superuser tenant; `user` and `plugin` never cross a
  tenant; `infra` crosses one only for the fabric mechanics above. A controller that manages other tenants' agents (a
  global or region hosting several tenants) must be in a superuser tenant.
- A refused RPC request is answered `status_code=403`, `status_desc=unauthorized: <action> needs <roles> (<why>)`.
  The refusal is logged (at most 20 lines per 10 s), counted in `cresco.authz.denied{action}`, and the executor never
  runs. A refused one-way message is dropped.
- These actions require `infra` or `admin`, and nothing else (the privileged floor):
  - plugin lifecycle: `pluginadd`, `pluginupload`, `pluginremove`, `pluginrepopull`;
  - controller and JVM: `controllerupdate`, `killjvm`, `restartframework`, `restartcontroller`, `stopcontroller`;
  - agent state: `agent_disable`, `agent_enable`;
  - remote file access: `getfiledata`, `getfileinfo`;
  - logging that exposes other callers' traffic: `setloglevel`, `setlogdp`;
  - executor: `config_process`, `run_process`, `start_process`, `end_process`, `write_stdin`;
  - filerepo: `clearrepo`, `putjar`, `removefile`, `putfiles`;
  - repo: `putjar`.

  Each plugin's owner may add actions to this list. None may be removed from it; an annotation can only narrow an
  action on it (to `infra`).
- The controller fails closed: an action its tables do not list needs `infra` or `admin`. A drift test checks that
  every `@CrescoAction` of the three executors is listed.
- Registry updates are bound to their maker (`RegistryBinding`): an agent registers, unregisters and keeps alive only
  itself in its region, a region only itself and its agents at the global -- by message (`agent_enable`,
  `agent_disable`, `region_enable`, `region_disable`, `regionalimport`, WATCHDOG) and on the dataplane
  (`update_mode` AGENT/REGION state updates, by their verified writer). At the global only the controller of a region --
  the one that joined the global (a bridge peer or a controller bound in its peer trust registry) -- speaks for it; an
  ordinary agent's stamp names its region too. Every delete names its records: an agent unregisters itself, never its
  region; a region unregisters only its own agents; a plugin record is removed only with its agent named (the registry
  never deletes a bare plugin id). An in-scope `admin` is not bound to one node, but names a plugin's agent too.

## 4. Defaults

- **Secure by default.** `broker_security_enabled`, `broker_require_client_auth` and `tenant_namespacing`
  all default to true.
- **Refuse to start** (exit 78, every problem named) with security on but client auth or namespacing off, with a tenant
  name that cannot be an identity, or with a setting the `security_profile` forbids (section 11).
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
  accepted again when its `security/` directory is restored (same key), when its new leaf is vouched for (§9), or after the
  operator authorises one rebind of it on each joiner (`peer_identity_rebind=<region>/<agent>=<key fingerprint>`, or the line
  in `peer-trust.json.rebind`, no restart). Controller pins never lapse (§9).
  `controller_trust_anchors` (SHA-256 of a controller's public key or of a certificate in its chain, comma separated)
  closes the first join as well: only a listed controller is joined.
- **Public-key directory** (audit D6): `public_key_directory` (default `<agent data dir>/public-keys`, absolute) still
  receives this node's exported certificate, but a certificate placed there becomes a trust anchor only when its SHA-256
  fingerprint is listed in `public_key_trusted_fingerprints`, the directory is absolute and neither it nor the file is
  writable by group or others (or owned by another user), and the file is not a symlink.
- **Renewal** (round-4 M4, corrected in §9): the persisted node chain and region CA are renewed with the SAME keys when a
  third of their own lifetime is left (or `identity_renew_before_days`, 30, if that is more -- but never more than half the
  lifetime) -- at start and every `identity_renew_check_minutes` (60). Bindings and recorded CA scopes follow keys, so peers
  notice nothing. Connections that reconnect present the current chain.
- **Short-lived region-issued leaves**: a region CA issues leaves valid for `identity_leaf_validity_hours` (168 = 7 days).
  A node re-enrolls with the controller it joined (one authenticated exchange, same key) at half-life; see §9 for what
  happens when that keeps failing (no self-made fallback).
- **Revocation**: `peer_identity_revoke` (names `region/agent` or key fingerprints) and the operator file
  `peer-trust.json.revoke` (re-read every 10 s) revoke a key for every name: the binding is dropped, TLS, the broker and the
  CA path refuse it, live connections holding it are closed at once, and it stays revoked (persisted, permanent; a revoked
  node needs a new key). Each controller signs its revocation list with its identity key and it travels on the parent/child
  pings as deltas (§9): a region's list is accepted at the global only from a controller bound there, for the region's own
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

## 8. Verified callers on every path (phase 3)

Nothing here applies while `broker_security_enabled=false` is set explicitly: every caller is `anonymous` and nothing
is refused for its caller.

- **In-JVM.** Each bundle gets its own `AgentService` (a service factory), so the controller knows the calling
  bundle. A message a plugin sends goes out as `plugin:<tenant>/<region>/<agent>/<pluginid>` only for a plugin id that
  bundle runs (a send as another bundle's plugin is dropped and logged); the controller's own messages as
  `controller:<tenant>/<region>/<agent>`. Whatever a sender wrote into the three reserved parameters is replaced.
- **On behalf of an API user.** `msgOutOnBehalfOf`/`sendRPCOnBehalfOf` are honoured only from a bundle in
  `on_behalf_bundles` (default `io.cresco.wsapi`), for a plugin it runs, for an `api:` caller of the node's own tenant
  -- of any tenant only when the node's own tenant is a superuser tenant (the broker applies the same rule, so a
  tenant's node never mints another tenant's or a superuser tenant's users). A file list on such a message is dropped
  (a user names no file on the agent's disk).
- **On the wire.** A sender copies its stamp into the JMS properties `cresco_caller`, `cresco_caller_tenant`,
  `cresco_caller_roles` (an unstamped message declares an empty caller). The broker keeps what this controller's own
  in-JVM connection carries, and what a verified bridge may carry (below); from a verified node it keeps what that node may declare -- itself
  (`infra`), its own plugins (`plugin`), its tenant's users (`admin`/`user`) -- and strips anything else (counted,
  logged); a node that declares nothing at all (no stamping code) is stamped `agent:<tenant>/<region>/<agent>` with
  `user`. The receiver strips the body's values and takes the properties. A file list in a message body is dropped;
  every part of a file transfer must come from the caller that registered it.
- **Dataplane.** Every dataplane message carries its verified writer in the same properties: the plugin (per bundle)
  or the controller, checked by the broker like any message.
- **Relays.** With tenant namespacing, a relay never carries a message into a tenant namespace (`cresco_tenant`) its
  verified caller does not belong to, unless the caller is of a superuser tenant.
- **RPC replies** must come from the callee (phase 1); now that every delivery is stamped, an unstamped reply is
  refused. An error reply a controller on the route generates (`route_unavailable`, `route_refused`) is no answer: it
  only FAILS the call, and the caller gets the marker, `error` and the controller's stamp (`status_code=503`) -- every
  other parameter it carried is dropped. A node moved to another region (`region_alias`, GaiaKeep gfs#26) answers a call
  made at its former address: its stamp carries `cresco_caller_alias`, which only infrastructure sets -- the broker the
  node is attached to, from the alias its region recorded of the node's own verified registration (its `agent_enable`
  configuration) -- and a sender's value is stripped on arrival.
- **Stdin.** No dataplane subscription but the executor's sees a process's stdin (`type` `input`/`input-*`). Every
  selector is narrowed, unconditionally, to `(<selector>) AND NO_STDIN`, after the selector parses on its own and the
  narrowed form parses as exactly that conjunction (no `OR`, parenthesis or quote reaches past it). A selector that does
  not parse, that evaluates bodies (XPATH/XQUERY), or that asks for stdin (judged on the parsed selector: a comparison
  of `type` reached without a negation that holds for a stdin type and not for an ordinary one) is refused. The
  controller's dataplane applies it per bundle (the executor is exempt only when this controller installed it); the
  broker to every subscription that is not this controller's own or a bridge's.
- **Remote files (D3a).** `getfiledata`/`getfileinfo` read only regular files below `file_access_roots`, both resolved
  to real paths; unset, both are refused. A plugin's dataplane offers no `downloadRemoteFile`.
- **Signed installs.** Every jar that did not come embedded in the agent jar -- `pluginupload`, `pluginadd` from a
  repository or an absolute path, `getjar` fetches, `plugindownload`, `controllerupdate`, the repository cache,
  `savetorepo` at the global -- is installed only from an owner-only private copy that verifies against
  `plugin_signing_truststore` (`JarSignatures.verify`). The cache names a jar by its own hash; a hash the caller
  supplies is advisory. `plugindownload` fetches only http(s) URLs into a plain file name.
- **Logging.** A plugin sets the log level of its own loggers (`<pluginid>:...`) only.
- **Dataplane loss is never silent.** The broker still drops the oldest messages a slow topic subscriber has not taken
  (its pending limit protects the other subscribers and the broker's memory), and counts every drop
  (`dataplane.broker.discarded`, logged). Every message of a stream (one carrying `stream_name`) is numbered by its
  writer's controller per (writer, `stream_name`, `type`) in `cresco_seq`; the subscriber's controller checks the
  numbers, counts what is missing in `dataplane.stream.gap`, and marks the first message after a gap with `cresco_gap`
  = how many messages of the stream that subscriber did not get. A subscriber joining mid-stream, and a writer that
  restarts (back to 1), are not gaps. A consumer that needs completeness reads `cresco_gap` (or `cresco_seq`) and
  recovers or reports; nothing in the stream path blocks a writer for a slow reader.
- **Plugin data is kept.** A plugin's data directory (`plugin-data/<pluginid>`) is never deleted when the plugin stops
  (every graceful agent shutdown stops every plugin) nor when it is removed: `pluginremove` deletes it only with
  `purge_data=true` from an `admin` caller, and logs where the kept data is otherwise. Data of plugins no longer
  configured on the agent is deleted only by the opt-in sweep `plugin_data_retention_days` (default 0: never), once a
  day, after that many days without a change.
- **Agent listings carry the tenant.** `listagents` (regional and global) gives each agent's `tenant_id`: the tenant its
  controller was verified in when it registered and kept alive (its region writes it; an agent's own claim is replaced),
  so a client resolves a destination's tenant.
- **Dataplane user writers.** A bundle in `on_behalf_bundles` (wsapi) names the API user it writes for in
  `cresco_writer`/`cresco_writer_roles`; that is kept only from such a bundle and for a user of the node's tenant (any
  tenant's from a superuser tenant), and at the broker only from a node that may send for that user. Anyone else's is
  removed.
- **Signed jars and time.** A jar signature carrying a trusted RFC 3161 timestamp (the TSA's certificate has the
  time-stamping extended key usage and chains, valid at the stamped time, to `plugin_signing_truststore`) is judged at
  the stamped time, so a stored jar stays installable after its signing certificate expires. Without one, re-sign
  jars before the signing certificate expires.
- **Review findings fixed here.** A composite destination is for infrastructure only (beyond a client's own advisory
  consumer): judged by its whole name it let a tenant read another tenant's dataplane and consume a peer's inbox. Only
  the broker writes advisory topics. No tenant name claims the broker's own destinations (`ActiveMQ.*`, e.g.
  `ActiveMQ.DLQ`).

- **What a bridge carries** (review F2). A stamp arriving over a broker bridge is kept only when the controller at the
  other end may carry it: a principal of its own tenant (any tenant's when that controller is in a superuser tenant),
  with the roles of its kind, and -- for a node principal -- within its subtree: its own region's nodes, or, for this
  node's parent (a region's global), any node below it. Anything else is stripped (counted in `outOfScopeStampCount`,
  logged as a forged caller): the message travels without a caller and is refused wherever one is required. With
  `net_source_routing` on, a message relayed through another region's controller loses its stamp at that hop until
  messages are signed end to end (D4).
- **Plugin ids** (review F6) are one plain path segment: 1-64 of `A-Z a-z 0-9 . _ -`, starting with a letter or digit,
  no `..`, checked where they enter (`pluginadd` `inode_id`, `pluginremove` `pluginid`, the global's `addplugin`). A purge
  deletes only a real directory whose real parent is `plugin-data`, and never follows a link inside it.

### The in-JVM trust boundary (review F5)

In-process code is trusted code. Java 21 has no SecurityManager: a plugin can reach any object it can name (reflection,
another bundle's `BundleContext` through `Bundle.getBundleContext()`, the classes of another bundle). Only signed plugins
are installed (section 5), and that signature is the boundary. Inside it the controller adds what is structurally cheap:

- The AgentService factory never hands out the controller's own instance, not even to the controller bundle (whose
  context any bundle can borrow): the controller uses its own reference, never the service registry. The controller
  bundle's registry instance is an ordinary per-bundle one with no plugins, so whatever is sent through it is dropped.
- A capability bound to a bundle's symbolic name -- sending on behalf of an API user (`on_behalf_bundles`), naming a
  dataplane writer user, the executor's stdin -- holds only for a bundle this controller installed itself (from the agent
  jar, or from a copy whose signature verified). A bundle a plugin installs through its own context holds none of them,
  whatever name it chose.
- A send on behalf of an API user must be made by that bundle's own code: the first stack frame below the Cresco library,
  the controller and the JDK must belong to the bound bundle, so borrowing wsapi's context is not enough by itself.
  This does not stop code that runs wsapi's own classes (reflection): that is the boundary above.

### Operator-visible changes (phase 3)

| Setting | Before | Now |
|---|---|---|
| `on_behalf_bundles` | (none) | `io.cresco.wsapi` |
| `plugin_signing_required` | (none) | defaults to `broker_security_enabled` |
| `plugin_signing_truststore`, `plugin_signing_truststore_password` | (none) | trust anchors for jar signatures; unset, every jar arriving over the network is refused while signing is required |
| `file_access_roots` | unset = open | unset = `getfiledata`/`getfileinfo` refused |
| controller actions | open to every caller | the tables above; undeclared actions need `infra` or `admin` |
| `plugindownload` | any URL, any file name | http(s), plain jar name, signed |
| composite destinations (non-infra) | judged by the whole name | refused (except a client's own advisory consumer) |
| advisory topics | any tenant could write | read only, for tenants |
| plugin data | deleted on every stop and remove of a runtime plugin | kept; `purge_data=true` (admin) on `pluginremove`; `plugin_data_retention_days` sweep (default off) |
| `listagents` | no tenant | `tenant_id` per agent (verified) |
| jar signatures | judged now | judged at a trusted RFC 3161 timestamp when present |
| stream messages | lost silently to a slow subscriber | `cresco_seq` on every stream message, `cresco_gap` after a loss, `dataplane.stream.gap` |
| AgentService | one shared instance | one per bundle (service factory); never the controller's own instance |
| `setinodestatus` (global) | any tenant's infra | infra/admin of the global's tenant or a superuser tenant |
| `agent_disable` naming only a region, or a plugin without its agent | deleted region / plugin rows | refused |
| registry updates at the global | any controller stamp of the region | only the region's own (joined) controller |
| bridged caller stamps | kept from any verified bridge | kept only within the bridge peer's tenant and subtree |
| route error replies (`route_unavailable`/`route_refused`) | delivered whole | fail the call with marker and error only |
| `region_alias` nodes on a secured fabric | replies dropped | accepted via `cresco_caller_alias` (from the node's registration) |
| plugin ids | any string | `[A-Za-z0-9][A-Za-z0-9._-]{0,63}`, no `..` |
| dataplane selectors | refused by text match, narrowed unless containing `NO_STDIN` | parsed; always narrowed; malformed/XPATH refused |
| named capabilities (`on_behalf_bundles`, executor stdin) | by symbolic name | by a bundle the controller installed (and, on behalf, its own code) |

## 9. Revocation, renewal and controller pins (identity review, 2026-11-04)

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
  longer rewrites the region broker's TLS context. A connection keeps the leaf it was opened with: when THAT leaf expires
  (by default about 3.5 days after the renewal) the broker closes it, as it closes any connection whose certificate expires,
  and the client reconnects at once with its current leaf -- one reconnect per leaf lifetime, by design (an expired
  certificate is never honoured). The re-enrollment runs outside the certificate manager's lock (holding it deadlocked the
  renewal against the discovery handler, found by the live renewal soak). Every join path re-enrolls -- multicast and static
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
  pinned controller to a new key WITHOUT a token when the new leaf is vouched for -- never for a revoked (tombstoned) name
  or key:
  - by an **operator-signed rebind statement** the controller serves in discovery: the operator signs "`<region>/<agent>`
    now holds key `<fingerprint>`" (at most 30 days) with an operator key and puts it in the re-installed controller's
    `<data>/security/controller-rebind.json`; every joiner that lists that operator key's SHA-256 (of its
    SubjectPublicKeyInfo) in `controller_rebind_signers` follows. One action at the controller instead of one rebind per
    joiner. Made with the JDK alone: `java -cp controller-1.3-SNAPSHOT.jar
    io.cresco.agent.controller.communication.RebindStatement <operator-key.pem> <operator-pub.pem> <region>/<agent>
    <new key fingerprint> [days]` (PKCS#8 / SubjectPublicKeyInfo PEM, e.g. `openssl genpkey -algorithm EC -pkeyopt
    ec_paramgen_curve:P-384`); a node logs its key fingerprint at start ("This node's identity key: ...");
  - by the CA it pinned for that region (a region controller re-installed with its `region-ca.p12` restored);
  - by a certificate or key listed in `controller_trust_anchors`.
  Under the regional CA, back up `region-ca.p12` (and its `.pass`): restoring it on a re-installed region controller is
  the one action (its agents verify the new controller leaf by that CA). Its private key is encrypted at rest under
  `region_ca_password_file` (an owner-only file the operator keeps elsewhere -- a mounted secret, another volume; its first
  line, 16 characters at least): a store made before is re-encrypted under it and the `.pass` beside it removed; without the
  setting the key is protected only by that `.pass` and the directory's permissions, and a WARNING says so.
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
| `controller_rebind_signers`, `<data>/security/controller-rebind.json` | (none) | operator keys whose signed rebind statements joiners follow; the statement a re-installed controller serves |
| a revoked name | free again at once | tombstoned until an operator rebind |
| health check `identity` | (none) | CRITICAL: registry fenced/sealed, revocations not recorded, own leaf expired or in its last tenth with re-enrollment failing, clock skew > backdate, region CA unavailable |
| `region_ca_allow_new` | (an unreadable region CA was silently replaced) | false: a region CA that existed and cannot be loaded is never replaced; true for one start makes a new one deliberately |
| `<data>/peer-trust.json.hwm` | (none) | the revocation high-water mark, kept OUTSIDE `security/` (a restored `security/` starts a new revocation epoch) |
| `identity_keystore`, `identity_keystore_password_file`, `trust_anchors`, `revocation_file` | (none) | a PRE-PROVISIONED identity from the operator's PKI (`security_profile=production`): nothing generated, enrolled or bound on first use -- see `pre-provisioned-identity.md` and `tools/cresco-pki.sh` |

### Round-2 re-review (2026-11-04)

- **Trust anchors are a validated path.** `controller_trust_anchors` is satisfied only by a path built from the presented
  LEAF up through the presented certificates -- each issuer's subject matching, a CA (basic constraints), its signature
  verifying, every certificate valid now -- to a listed certificate or key; certificates merely carried along are ignored.
  The same path vouches for a re-keyed controller. Under the regional CA the anchors are checked on the responder's own
  chain, and the identity it issues the joiner (or the CA it returns) must come from a CA on that path: a copied public
  chain cannot enroll anyone under another CA.
- **Revocation expiry outlives every leaf.** A revocation of a key a region CA enrolled expires only after the longest
  validity any such leaf could carry (three years, the pre-upgrade leaf lifetime, or the configured one if longer, plus
  twice the backdate and a day), and the name it names is tombstoned wherever the revocation applies (not only at the
  revoking region), so nothing is readmitted when it expires. (A remote tombstone refuses that name only for connections
  made directly to that node, which an agent does not make: it connects to its own region, where its operator rebinds it.)
- **Bridges are recorded at every handshake.** A bridge's TLS context records the connection whenever a handshake
  completes -- a resumed one (a TLS 1.3 session ticket, which never calls the trust manager) included -- and closes it at
  once if its peer is revoked (key, name, or a CA anywhere in its chain); bridge groups to a peer whose name is tombstoned or
  whose region's CA is revoked are removed.
- **Signed lists are signed by a valid certificate.** The signed-list cache is keyed by the signing certificate as well as
  the positions; a renewed leaf signs anew, and a list is never signed or served with a certificate that is not valid now.
- **A restored registry starts a new epoch.** The revocation high-water mark (epoch and furthest durable position) is kept
  next to `security/`, outside it: a registry restored from an older copy (same epoch, fewer positions) or of another epoch
  starts a new epoch at open, so its parent and children exchange their lists again from the start, both directions. A
  verified peer that reports positions this node never made (a whole data directory restored) also starts one (at most once
  an hour).
- **Short leaves renew at a fraction of their intended life.** The renewal window counts a certificate's lifetime without
  the backdate it carries (a leaf of a day or less was due at issue): a 1-hour leaf renews at half an hour left, a 12-hour
  one at six hours.
- **Clock skew is taken only from verified peers.** A child's clock counts only when the ping's broker-stamped caller is a
  node bound here (never the names the message carries); a report ages out after 15 minutes without that peer.
- **A region CA that existed is never silently replaced.** An unreadable, missing (while the registry records it) or
  wrong-scope `region-ca.p12` leaves the region without a CA -- no identities issued, CRITICAL health, an ERROR with the
  recovery steps (restore it, picked up within a minute; or `region_ca_allow_new=true` for one start, after which every peer
  must rebind the region) -- instead of generating a new CA that every agent and peer refuses.
- **The global serves its list only to its child controllers.** The global's ping handler sends its revocation list
  (fingerprints and names) only to a pinger verified as a child controller (a verified signed list, or a broker-stamped
  controller caller bound there as a controller), and in the explicit insecure mode; a region serves its own list to its
  agents.

## 10. End-to-end message signatures (D4)

A broker vouches for one hop. Across several brokers -- relays, source routes, the bridges of other regions -- the
receiver also needs the origin's word, so every MsgEvent a node originates is signed with that node's identity key, and a
receiver checks it before it dispatches the message locally or completes an RPC with it. Nothing changes while
`broker_security_enabled=false` is set explicitly.

- **What is signed.** The type, the source header and every parameter except what infrastructure rewrites in transit:
  `routepath-*`, `linkcost-*`, `srcroute*`, `ttl`, the broker's caller stamp (`cresco_caller*`, including the alias) and
  the signature itself. The destination header is not signed (`region_alias`, source routes and the controller tiers
  rewrite it), but the **recipient** is (`cresco_sig_to`): the node and plugin the message is for, as the origin's router
  decided it -- after the controller tiers, the last waypoint of a source route -- as `<region>/<agent>/<plugin>` (plugin
  empty for the node itself), or `^<region>/<agent>` for a function of the global that an agent hands its region controller
  to carry on ("the global above that region controller": accepted only by a global whose child region it is, from a
  signer of that region). A receiver refuses a signed or MAC'd message that does not name it, or its former address
  (`region_alias`): a message signed for one node is never accepted by another, whoever re-sends it. The
  origin's caller is signed as a copy (`cresco_sig_caller`, `cresco_sig_caller_tenant`, `cresco_sig_caller_roles`) next
  to the signer (`cresco_sig_by`, `<tenant>/<region>/<agent>`), its credential (`cresco_sig_cred`) and the signing time
  (`cresco_sig_ts`); `cresco_tenant` is set when the message is stamped, so it is signed too. A message is signed as it
  leaves its origin node; an in-JVM delivery is never signed, and a relay never signs what it carries.
- **Keys.** Node keys are RSA-3072, signed with SHA-384 (CrescoCrypto's policy). A region or global controller presents
  its certificate as its credential; an agent an attestation from its region controller (tenant, region, name, public key;
  valid `msg_signing_attestation_hours`, default 24, renewed on its pings when a third is left), and its certificate until
  it holds one. A receiver accepts a certificate that verifies here as its name (a peer bound here, or under a recorded
  region CA) or whose key the trust bundle lists for that controller, and an attestation signed by the controller the trust
  bundle lists for the attested region (a superuser-tenant region attests any tenant's agents, any other its own tenant's
  only). The trust bundle is the list of region and global controller keys: each region presents its certificate on its
  pings to the global, which lists it when it verifies there as a joined controller, signs the list and sends it down on its
  pongs when the region holds another; each region re-signs what it verified for its agents (as revocation lists travel).
  A controller its global no longer lists is kept `msg_signing_bundle_grace_ms` (15 min). An attestation and a bundle
  entry carry, by reference, the certificates the key stands on (each one's key and certificate digests, issuer and
  serial, validity): a credential is verified once and cached, but on EVERY message the signer -- and an attestation's
  issuer -- is checked against this node's revocations as they stand: a key revoked here (any scope), a tombstoned name,
  a certificate out of its validity, and with a PKI the operator's `revocation_file` (a listed key or certificate, a CRL
  serial, a CRL past its `nextUpdate`: fail closed). A revocation coming in force (a reload of `revocation_file`, a revoked
  key, a merged list) also re-verifies every cached credential, drops the sessions of what it now refuses and, at a
  global, re-verifies its regions' certificates and drops those refused from the bundle it sends down.
- **The receiver** refuses an unsigned message, a bad signature or credential, a message that does not name it, a
  signature older or newer than `msg_signature_max_age_ms` (5 min) and one seen before (a replay); an RPC request is
  answered `401 unauthenticated`. What a node only carries on (a source route's waypoint, a region controller carrying an
  agent's global function to its global, a message not addressed to it) is not checked on arrival but noted, and checked
  wherever it is then dispatched on that node -- whatever re-addressed it on the way (the controller tiers turn a global
  function addressed elsewhere into one for the controller they pass): nothing is dispatched unverified. The
  signer must be the node of the signed caller -- or, for an API user, a node of the user's tenant (any tenant's from a
  superuser tenant) -- and the broker's stamp, when there is one, must be that caller. When a bridge stripped a stamp it
  could not vouch for (section 8), the origin's signed caller stands.
- **Direct links (no relay, no signature).** A signature carries the origin's word past relays. A message between a node
  and its own parent or child -- an agent and its region (or global) controller, a region and its global -- crosses no
  relay: the broker its receiver consumes from took it from that very node, over the node's verified connection or the
  verified bridge of that very controller. That broker says so in `cresco_direct` (`<tenant>/<region>/<agent>`), which only
  it writes (whatever a sender put there is replaced at every broker), and only when the sender declared itself the origin
  (`cresco_origin`) and its stamp was accepted for that connection. Such a message travels unsigned
  (`msg_signing_direct`, default true). The receiver alone decides: it accepts an unsigned message only when the broker
  it consumes from attests it first hand from the node whose word the stamp is (that node itself, its plugins, or an API
  user of that node's tenant). A controller trusts its own broker's attestation; an agent trusts its parent's broker only
  for the parent's own messages -- everything else that broker delivers came through a relay and must be signed. An origin
  that misjudges a route is refused (`MESSAGE SIGNATURE: REFUSED an unsigned message (... not delivered first hand)`),
  never trusted. Each side offers direct links on its pings and pongs (`sig_direct_ok`): a node sends unsigned only to a
  parent or child that offered them within the last minute, so a node of an earlier hardened build is always sent
  signatures.
- **Batch signatures.** The messages a node signs at the same moment share one private-key operation: each is a leaf
  (`SHA-384(0x00 || canonical form)`) of a Merkle tree (`SHA-384(0x01 || left || right)`), the node signs
  `"cresco-msg-batch-v1" || 0 || count || root` and each message carries the signature and its own path to the root
  (`cresco_sig_tree`: `<index>/<count>:<sibling>,...`, not signed: it is what leads to the signature). Each message still
  verifies on its own -- its receiver recomputes the root from the message, checks the root's signature once per batch
  (then cached) and keeps signature and leaf in its replay memory, so a replayed, altered or re-pathed message is refused
  like any other. A message alone in its batch -- whenever the node is not busy -- gets an ordinary signature, so no
  latency is added; up to 256 share one under load (`msg_signing_threads` workers, default half the cores, 2 to 8). Every
  signed message says its signer reads batch signatures (`cresco_sig_v=2`, signed), and a node sends them only to a node
  it verified such a message from, so a node of an earlier build is always sent single signatures
  (`msg_signing_batch=false` turns them off). Every signed message says what its signer reads (`cresco_sig_caps`:
  `batch`, `session`; signed).
- **Session keys (end to end, past the relays).** Two nodes that talk across relays agree once on a key only they hold,
  then authenticate each message with an HMAC-SHA-384 under it (`cresco_sig_sid`, `cresco_sig_mac`; about 15 us to
  check) instead of a signature (2 ms to make), and the credential (2.2 KB) no longer travels. The agreement rides on
  messages already authenticated: the first node to send to a peer that reads session keys attaches an ephemeral P-384 key
  to its signed message (`cresco_sig_kx`), the peer answers with its own on its next message (`cresco_sig_kxa`), and each
  derives the key from the ECDH secret (SP 800-56A ephemeral-ephemeral; SP 800-56C two-step HMAC-SHA-384 key derivation
  over both names and both public keys). A relay never sees the key, so it still cannot forge, alter or replay what it
  carries; the receiver checks the signing time, the replay memory, the signed caller, the broker's stamp, and the
  signer's credential and revocation on every message exactly as for a signature (a revoked key's session is dropped).
  What a session does not give is a signature a third node could check: a MAC convinces its one receiver only, which is all
  a MsgEvent needs. The message that carries an offer or an answer is always signed (never MAC'd), and a receiver acts on
  an offer or answer only from a signed message: an agreement costs its sender a signature, and every renewal refreshes
  both credentials. While a session is usable an offer or answer rides at most one message a second; the others stay
  MAC'd. An offer is accepted from a peer at most once every 10 s, under that peer's own lock (a P-384 key pair and an ECDH
  each): no peer can make a node spend more, or hold up the others. A renewal never interrupts: an offer from a peer that
  still holds the confirmed session goes to a pending slot, and both keep sending under the confirmed session until the
  peer uses (or names) the new one. A node sends under a session only while its peer said within the last 30 s that it
  holds it (every message to a peer names the session the sender holds with it, `cresco_sig_held`; a message from the peer
  that names another one, or none, stops this node's MACs to it at once) and names its sender's incarnation
  (`cresco_sig_inc`, random at each start): a peer that restarted names a new one, and its next message makes this node
  return to signatures and agree anew. A message sent to a restarted node under the session it lost is refused like any
  unauthenticated message (an RPC is answered `401`). Sessions last `msg_signing_session_seconds` (one hour) and are
  renewed ahead of their end on the messages they carry; a node of an earlier build, or one with
  `msg_signing_sessions=false`, never reads them and is always sent signatures. A provider that refuses any of it (a FIPS
  module raising an error) turns session keys off on that node, logged once: it stays with signatures.
- **Cost.** Between two nodes that talk both ways, one ECDH agreement per pair and hour, then an HMAC per message (about
  15 us to check, 0.7 KB on the wire); otherwise one signature per message a node originates to anything but its parent
  or children (RSA-3072 private key: about 2 ms of one core, shared by a batch under load) and one signature check per
  message dispatched (about 0.08 ms), the signed message carrying its signer's credential (about 2.2 KB). Credentials are
  verified once and cached. Direct-link messages cost no cryptography beyond TLS: the bulk of the control plane
  (keepalives, registrations, pings, the replies of regions and the global) is direct. Nothing in it grows with the size
  of the fleet (every table is bounded, O(1) per message); the trust bundle grows with the number of regions and
  travels only when it changes: a version number bumped on each change makes a child's ping O(1) at its parent, with no
  lock (it was rebuilt and hashed whole on every ping: 260 us at 1,000 regions, 1.1 ms at 5,000), and it is signed and
  compressed once per change. Bounded memory, sized for 100,000+ agents with no cliff: the replay memory keeps each
  signature in the bucket of its signed time for one max-age (8 bytes of digest an entry in primitive tables, about 17
  bytes with their slack; `msg_replay_memory_max`, 4,000,000; full, it drops its oldest bucket and refuses what was signed
  in it, never accepting a replay), the check and the record one step under one of 64 locks; credentials are a bounded LRU
  (`msg_signing_credentials_max`, 100,000: past it the least recently used goes, never all at once); sessions are one
  entry per peer (`msg_signing_sessions_max`, 200,000, about 1 KB each) holding who the peer authenticated as, not its
  2 KB credential -- a full table gives a new peer no session (it stays signed), never evicting one in use. `docs/tests/control_plane_bench.sh` measures it live (`ControlPlaneCostBench`,
  `-Dcresco.bench=true`, in one JVM). Measured on one host (2026-11-09, RPCs from a client at the global to an agent of
  another region, across its region's bridge; before hardening: p50 1.1 ms, 3948/s at 8 callers): per-message signatures
  p50 5.9 ms, 1371/s; with session keys p50 1.4 ms, 3410/s (production, FIPS: 7.1 ms and 1094/s, now 1.8 ms and 3228/s).

| Setting | Default | Meaning |
|---|---|---|
| `msg_signing_required` | `broker_security_enabled` | false: no signing or checking (development only; a WARNING every boot) |
| `msg_signature_max_age_ms` | 300000 | the signing time's tolerance either way (clock skew included) |
| `msg_signing_attestation_hours` | 24 | an agent attestation's lifetime |
| `msg_signing_bundle_grace_ms` | 900000 | how long a controller dropped from the global's bundle is still trusted |
| `msg_signing_direct` | true | messages between a node and its parent or children travel unsigned, attested first hand by the receiving broker |
| `msg_signing_batch` | true | messages signed at the same moment share one signature (a Merkle root), to nodes that read them |
| `msg_signing_threads` | cores / 2 (2 to 8) | signing workers; each signs a batch at a time |
| `msg_signing_sessions` | true | end-to-end session keys (HMAC under an ECDH-agreed key) with the nodes that read them |
| `msg_signing_session_seconds` | 3600 | a session key's lifetime (renewed ahead of its end) |
| `msg_signing_sessions_max` | 200000 | peers a node holds sessions with (past it, a new peer stays signed) |
| `msg_signing_credentials_max` | 100000 | credentials held verified (an LRU) |
| `msg_replay_memory_max` | 4000000 | signatures and MACs the replay memory holds (past it, the oldest signing times are refused as too old) |

### Final review (2026-11-09)

An independent review of the message authentication above found, and `FinalReviewProofTest` first proved and now guards:

- **F1** a signed message was accepted by every node, not just its addressee (the destination was not signed, and the
  replay memory is per receiver): an agent could re-send an admin's `stopcontroller` meant for itself to every node of its
  tenant, and the global accepted its own infra message reflected back. Now the recipient is signed and checked (above);
  session offers, batch signatures and session MACs are bound to it the same way (they cover the same canonical form), so
  an offer re-sent to another node is refused there and can no longer replace that pair's session (F1b).
- **F2** in production the operator's `revocation_file` did not stop message authentication: a revoked agent kept acting
  through its region's attestation, a revoked region controller through the global's trust bundle and the credential
  cache. Now every message's signer and issuer are checked against the revocations as they stand (above).
- **F3** the upgrade window is decided at start (`upgrade-1.3-secure-defaults.md`); **F4** the ping cost is O(1) (above).
- Also: verified wherever dispatched (a function re-addressed to this node was dispatched unverified); offers only on
  signed messages, rate-limited per peer, no node-wide lock; bounded memory without a thrash cliff; the replay check
  atomic (a copy could pass twice across a rotation); `security_profile=production` checks FIPS approved-only mode itself
  (`production-deployment.md` section 6).
- Cost of the fixes, `control_plane_bench.sh` on one host, cross-region RPC at 8 callers (before -> after, two runs of
  the build before): standard p50 1.65-1.71 ms, 3423-3446/s -> 1.70 ms, 3442/s; production (FIPS) 2.02-2.12 ms,
  3207-3222/s -> 2.09 ms, 3222/s: within the run-to-run spread.

## 11. Secure by default, security profiles and the upgrade (phase 4)

- **Defaults.** `broker_security_enabled`, `broker_require_client_auth` and `tenant_namespacing` default to true: a node
  with no security setting runs secured. Insecure debug mode is only `broker_security_enabled=false`, set explicitly.
- **Profiles** (`security_profile`): `standard` (default) keeps the enrollment conveniences (discovery-secret joins,
  self-made and region-issued certificates, pins on first join, the upgrade window); `production` requires
  pre-provisioned identities (`identity_keystore`, `identity_keystore_password_file`, `trust_anchors`) and plugin signing
  anchors, makes every security setting mandatory, and refuses enrollment, trust on first use, the upgrade window and
  passwords -- see `production-deployment.md`. Plugins read the profile through `AgentService.getSecurityProfile()`.
- **The upgrade window (M6a)**: `discovery_auth=compat` takes effect only with `discovery_compat_until` (at most 90 days
  ahead). Inside it, a node without stamping (`agent:<tenant>/<region>/<agent>`, role `user`, unsigned) keeps its own
  membership at its region (register, unregister, keep alive, ping) and nothing more; after it, it is refused. Never in
  production.
- **Dotted tenants (M6b)**: a `tenant_id` with a `.` runs only with `tenant_id_dot_replacement`.
- **Order (M6c)**: global, then regions, then agents, with the window open on every upgraded node until the last agent is
  upgraded -- `upgrade-1.3-secure-defaults.md`.
- **Namespaced fabric.** A region judges its global reachable by the queue its messages go to (`T.<tenant>.<path>`) and
  counts the global's wildcard inbox (`T.*.<path>`) and its bridge demand.

| Setting | Before | Now |
|---|---|---|
| `broker_security_enabled` | false | true |
| `tenant_namespacing` | false | follows `broker_security_enabled` (true) |
| `security_profile` | (none) | `standard`; `production` available |
| `discovery_auth=compat` | open-ended | only inside `discovery_compat_until` |
| `tenant_id` with `.` | accepted | `tenant_id_dot_replacement` required |
| `identity_keystore`, `identity_keystore_password_file`, `trust_anchors`, `revocation_file` | (none) | production identity settings |
| `plugin_signing_truststore` | PKCS12/JKS with a password | also a PEM bundle (no password) |

