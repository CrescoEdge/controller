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

Agents that are not upgraded yet (an earlier 1.3 build) stamp nothing and sign nothing. A secured broker stamps such a
node `agent:<tenant>/<region>/<agent>` with role `user` only. During an explicit, time-bounded window those nodes keep
their membership -- they join over the legacy discovery exchange, register, unregister, keep alive and ping for their own
node at their region -- and nothing more: everything else they send meets the action tables as a `user`. Outside the
window they are refused.

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
   Verify: it starts (no `REFUSING TO START`), logs `message signatures ON`, and `UPGRADE WINDOW OPEN`.
2. **Regions**, one at a time, with the same window settings. Each region rejoins its global over the authenticated
   discovery exchange (both must be upgraded: controllers never use the legacy exchange) and pins it. Verify per region:
   `Broker bridge from verified controller ...` at the global, `registered with Global`, and that its not-yet-upgraded
   agents re-register (`agent:` stamps in the region's log are expected during the window).
3. **Agents**, in any order, with the window settings while the rollout lasts. An upgraded agent re-enrolls over the
   authenticated exchange and its messages are signed from then on.
4. **Close the window**: once every node is upgraded, remove `discovery_auth=compat` and `discovery_compat_until` from
   every node (or let the date pass and remove them at the next restart).

A node that cannot be upgraded before the window ends is cut off when it does: upgrade it, or take it out of the fabric.

## 5. Checking a node

- `REFUSING TO START: ...` lines name every configuration problem; the process exits 78.
- `WARNING: ...` lines at every boot name every temporary or insecure setting in effect.
- `message signatures ON: this node signs what it originates` and `... checks what it dispatches` once each.
- `MESSAGE SIGNATURE: REFUSED ...` (at most every 10 s, with a running count) names unsigned, stale, replayed or
  mis-vouched messages: during the window, a legacy node's messages other than its membership; after it, any of them.
- The peer-identity live harness (`docs/tests/peer_identity_live_test.sh`, `DEFAULTS=1`) runs a global, a region and two
  agents with no security setting at all and checks the secured fabric end to end.

## 6. Rolling back

Rolling a node back to the earlier build is possible while the window is open on the nodes around it (it is a legacy
node again). Rolling the whole fabric back to insecure operation needs `broker_security_enabled=false` on every node --
never on a network you do not control.
