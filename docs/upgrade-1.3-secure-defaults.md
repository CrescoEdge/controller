# Upgrading a fabric to the secure 1.3 defaults

Cresco 1.3 hardening turns security on by default. A node with no security setting at all now runs with broker
authorization, mutual TLS (client certificates), tenant-namespaced destinations, verified callers, per-action roles and
signed messages. This guide is for an operator who runs a fabric on an earlier 1.3 build (security off, or on without
namespacing) and upgrades it without a flag day. The contract behind it is `caller-authorization.md`; production
deployments with operator-issued certificates follow `production-deployment.md` instead.

## 1. What changes when a node is upgraded

| Setting | Before | After the upgrade |
|---|---|---|
| `broker_security_enabled` | false | **true** (false only when set explicitly: insecure debug mode, a WARNING every boot) |
| `broker_require_client_auth` | follows security | follows security (so true) |
| `tenant_namespacing` | false | **true** (follows security) |
| `security_profile` | (none) | `standard` (or `production`, see the production guide) |
| `discovery_auth=compat` | open-ended | in effect only with `discovery_compat_until` (the upgrade window, at most 90 days) |
| `tenant_id` with a `.` | accepted | refused at start unless `tenant_id_dot_replacement` is set |
| messages | unsigned | signed by their origin node, checked before dispatch (`msg_signing_required`) |

A node also **refuses to start** (exit code 78, one `REFUSING TO START` line per problem) when security is on but
`broker_require_client_auth=false` or `tenant_namespacing=false` is set, when its tenant name cannot be an identity, or
when its `security_profile` forbids a setting. Remove the offending setting, or set `broker_security_enabled=false`
explicitly for an insecure debug node.

## 2. Before you start

1. **Tenant names.** A `tenant_id` with a `.` (legal before) cannot be an identity: tenant `t1`'s namespace `T.t1.` would
   be a prefix of `t1.x`'s. Set `tenant_id_dot_replacement=-` (or a letter or digit) on **every node of that tenant**; the
   tenant then runs as, e.g., `acme-prod` for every security use. Make sure no other tenant already has that name.
2. **Plugins installed over the network** (`pluginadd` from a repository, `pluginupload`, `putjar`) must be signed:
   set `plugin_signing_truststore` (and its password) on every node that installs them. Jars embedded in the agent jar
   are trusted as part of it.
3. **Remote file reads** (`getfiledata`, `getfileinfo`) need `file_access_roots`.
4. **Callers that are not controllers.** API users authenticate to wsapi; a plugin acts only as itself. Tools that sent
   MsgEvents with a forged `cresco_caller`, or set `cresco_to` on a dataplane stream from an API client, are refused.
5. **Multi-tenant regions.** A region or global that manages agents of several tenants must be in a superuser tenant
   (`broker_superuser_tenants`, default `cresco-system`).

## 3. The upgrade window

Agents that are not upgraded yet (an earlier 1.3 build) stamp nothing, sign nothing and run without tenant namespacing
(they write their controller's raw inbox `<region>_<agent>` and listen on their own). A secured broker stamps such a node
`agent:<tenant>/<region>/<agent>` with role `user` only. During an explicit, time-bounded window those nodes keep their
membership -- they join over the legacy discovery exchange, register, unregister, keep alive and ping for their own node at
their region -- and nothing more: everything else they send meets the action tables as a `user`, and their dataplane use
(e.g. the route state they publish) is refused by the broker (`DENY ... global.event` lines for them are expected).
Outside the window they are refused.

What an upgraded controller does for them while the window is open:

- it also consumes its raw inbox (`AgentConsumer also consumes the raw inbox [<region>_<agent>] during the upgrade window`
  at boot), and a node of its region may write that inbox whatever its tenant (an earlier build wrote there);
- a message there stamped `agent:` marks its sender as an earlier build, and what the controller sends to it -- the reply to
  its registration, health pings -- goes to its raw inbox, where it listens; once the node speaks as an upgraded node the
  mark is gone (bounded: 100,000 nodes, 15 minutes after a node was last heard from);
- it answers a discovery locator in the format it was asked in (an upgraded node always asks in the authenticated format,
  so an upgraded agent finds an upgraded controller with or without the window settings);
- an earlier build keeps no identity key across restarts, so its upgraded build presents a NEW key: the binding the legacy
  exchange made for that agent name is replaced, once, by the key the upgraded agent proves over the authenticated
  exchange (`Discovery: <region>/<agent> UPGRADED: the binding its earlier build made over the legacy exchange is
  replaced ...`; the old key stays refused). A binding made by the authenticated exchange is never replaced this way, and
  outside the window a legacy binding is like any other (an operator rebind).

Open the window on every **upgraded** node they talk to (global, regions, upgraded agents):

```
discovery_auth=compat
discovery_compat_until=2026-12-15      # an ISO date (end of that day, UTC) or instant, at most 90 days ahead
```

Every boot logs `WARNING: UPGRADE WINDOW OPEN until ...`. Without `discovery_compat_until` (or with one more than 90 days
ahead) compat is ignored, and an ERROR says so. When the date passes the window closes by itself (no restart): legacy
exchanges, unsigned messages and `agent:` registrations are refused from then on, and every boot logs that the window has
closed until the two settings are removed. The window is not available under `security_profile=production`.

## 4. Order (M6c)

Controllers first, from the top: a parent must understand everything its children send.

1. **Global.** Upgrade it with the window settings (section 3), and the same security settings every node will use.
   Verify: it starts (no `REFUSING TO START`), logs `UPGRADE WINDOW OPEN`, `AgentConsumer also consumes the raw inbox`,
   and `Discovery: accepting the LEGACY unauthenticated certificate exchange` once its earlier-build agents rejoin; they
   register again (`registered with Region` in their logs).
2. **Regions**, one at a time, right after the global, with the same window settings. Each region rejoins its global over
   the authenticated discovery exchange (both must be upgraded: controllers never use the legacy exchange, so a region of
   the earlier build is not federated with an upgraded global until it is upgraded; its agents keep working with it) and
   pins it. Verify per region: `Broker bridge from verified controller ...` at the global, and that its not-yet-upgraded
   agents re-register and appear in the global's registry (`listagents`).
3. **Agents**, in any order, one at a time or in batches. The window settings are not needed on an agent (they are
   harmless while the window lasts). An upgraded agent joins over the authenticated exchange (`discovery v2:
   authenticated AGENT exchange` in its log, `<region>/<agent> UPGRADED` at its controller), registers, and its messages
   are signed from then on (`message signatures ON`).
4. **Close the window**: once every node is upgraded, remove `discovery_auth=compat` and `discovery_compat_until` from
   every node at its next restart, or let the date pass: the window then closes by itself, without a restart -- an
   earlier-build node still running is refused from that moment (`MESSAGE SIGNATURE: REFUSED an unsigned message (from
   agent:...)` at its controller, which stops answering it), and one that starts later is refused at discovery
   (`Discovery: REFUSED a node of an earlier build`). A node still configured with the expired settings logs `the upgrade
   window has CLOSED` at every boot until they are removed.

A node that cannot be upgraded before the window ends is cut off when it does: upgrade it, or take it out of the fabric.

## 5. Checking a node

- `REFUSING TO START: ...` lines name every configuration problem; the process exits 78.
- `WARNING: ...` lines at every boot name every temporary or insecure setting in effect.
- `message signatures ON: this node signs what it originates` and `... checks what it dispatches` once each.
- `MESSAGE SIGNATURE: REFUSED ...` (at most every 10 s, with a running count) names unsigned, stale, replayed or
  mis-vouched messages: during the window, a legacy node's messages other than its membership; after it, any of them.
- The peer-identity live harness (`docs/tests/peer_identity_live_test.sh`, `DEFAULTS=1`) runs a global, a region and two
  agents with no security setting at all and checks the secured fabric end to end.
- The upgrade live harness (`docs/tests/upgrade_window_live_test.sh <upgraded agent.jar> <earlier agent.jar> <dir>
  <python>`) proves this guide on one host: earlier-build agents join upgraded controllers in the window, are upgraded one
  by one (with and without the window settings), the window closes by its date while an earlier-build agent still runs,
  and the controllers restart without it.

## 6. Rolling back

Rolling a node back to the earlier build is possible while the window is open on the nodes around it (it is a legacy
node again). Rolling the whole fabric back to insecure operation needs `broker_security_enabled=false` on every node --
never on a network you do not control.
