# Pre-provisioned identities (`security_profile=production`)

In a secure environment every certificate is computed ahead of time and assigned by the operator's PKI. A node then
never generates, enrolls or learns an identity: it loads the one it was given, trusts the fabric's anchors, and judges
every peer by that PKI. This mode is on when `security_profile=production` is set (A4's profile, which requires it) or
whenever `identity_keystore` is set.

## Settings

| Setting | Meaning |
|---|---|
| `identity_keystore` | PKCS12 keystore with ONE private key entry: the node's key and its chain (leaf first, then its issuing CAs) |
| `identity_keystore_password_file` | the keystore's password (first line); owner-only |
| `trust_anchors` | PEM bundle of the fabric's ROOT certificate(s) only (region CAs are intermediates each node presents in its chain) |
| `revocation_file` | optional: the operator's revocations -- PEM CRLs issued by the PKI and/or a deny list (below) |
| `identity_keystore_reload_seconds` | 30: how often the files are checked for replacement |
| `regionname`, `agentname`, `tenant_id` | must match the certificate (unset names are taken from it) |

Every one of these files must be a regular file (not a symbolic link) owned by the node's user (or root), not writable by
group or others (the keystore and its password file not readable by them either), in a directory -- the parent and every
directory above it -- owned by the node's user or root and not writable by group or others (a root-owned sticky directory
such as `/tmp` is allowed above the parent): otherwise another user could rename a file of their own into its place. Anything
else and the node **refuses to start**; a replacement that fails these checks is refused; and the checks run again at every
reload tick (a directory opened to others later raises a CRITICAL `identity` alarm, and no replacement is loaded).

## Identity names

What the broker and the peer trust registry use is the leaf's subject, read strictly:

- **node**: `CN=<agent>, OU=<region>, O=<tenant>` (Cresco name grammar: 1-64 of `A-Z a-z 0-9 . -`; a tenant has no `.`),
  with the SAN URI `cresco:node:<tenant>/<region>/<agent>` (when present it must name the same identity);
- **controller** (a region or global controller: it answers discovery, bridges, sends revocation lists): also the SAN URI
  `cresco:role:controller`; a node configured as `is_region`/`is_global` refuses to start without it, and a joiner refuses a
  responder without it;
- **the global** controller: also the SAN URI `cresco:role:global` (issued by the root, or a CA whose roles include
  `global`). A node configured `is_global` refuses to start without it; ONLY a node holding it answers GLOBAL discovery, and a
  region accepts as its global (its parent) only a responder whose certificate carries it. Every region holds
  `discovery_secret_global` (it needs it to join), so the secret alone never says which controller is the global;
- **API user** (wsapi): SAN URIs `cresco:api:<tenant>/<user>` and `cresco:role:<admin|user>`, subject `CN=<user>, O=<tenant>`
  (no `OU`, so it can never name a node);
- **CA scope (fail closed)**: every CA on a path -- the anchor included -- must allow the leaf's tenant, region and roles.
  A CA's scope is EXPLICIT: exactly one SAN URI `cresco:ca:<tenant>/<region>` (`<tenant>/*`: every region of the tenant;
  `*`: any) and exactly one `cresco:ca-roles:<roles>` (a comma list of `node`, `controller`, `global`). Only a self-signed
  anchor without scope URIs (the fabric root) may issue anything. Any other CA without an explicit, well-formed scope -- a
  tenant CA naming only `O=`, an enterprise CA, a region CA of the earlier tool -- issues nothing a node accepts. A region CA
  made by the tool carries `cresco:ca:<tenant>/<region>` and `cresco:ca-roles:node,controller` (never `global`).

## At start

The chain is validated (PKIX: signatures, validity, CA basic constraints and path length) from the leaf to an anchor, the
CA scopes are checked, the key must match the certificate, and then EVERY certificate on the path, the anchor included, must
be within its validity (PKIX never checks an anchor's own dates) and not listed by the operator's revocations (a revoked
region CA stops every leaf it issued, whether the leaf presents it or not). Peers are judged the same way at every
handshake, and the broker's sweep of live connections (every minute and at every revocation change) re-judges the whole
path each connection presented: a revoked or expired issuer or anchor closes it. The node refuses
to start -- the controller does not come up, the log says `REFUSING TO START: ...` -- when any check fails, the
certificate names another identity than the node is configured as, or `security_profile=production` is set without a
keystore. There is no fallback.

## What changes

- **No self-made certificates, no enrollment.** No node identity or region CA is generated (`security_regional_ca` is
  ignored), nothing is issued at a discovery join, and nothing a peer returns is installed.
- **No trust on first use.** Discovery still finds the controller, but nothing is bound or pinned: the responder must be a
  controller of the PKI naming itself as it answered, a joining agent must hold a PKI certificate of the responder's region
  and tenant, a joining controller must carry the controller role. Every TLS handshake and every broker connection is judged
  the same way (PKIX to the anchors, scope, revocations), and the truststore holds the anchors only.
- **Revocation** is enforced from both sources: the operator's `revocation_file` (re-read when it changes; a listed key,
  certificate, CRL serial or name is refused at TLS, at the broker and in discovery, and the broker closes live connections
  that hold it) and the signed revocation lists controllers exchange (`caller-authorization.md` section 9). Revoking a name
  (`peer_identity_revoke=<region>/<agent>`, the operator file) reserves it: a rebind token (`<region>/<agent>=<new key
  fingerprint>`) lifts that once the PKI issued a new certificate.
- **Renewal** is the operator's: replace the keystore file (atomically, e.g. write and rename -- the tool does). The node
  reloads it within `identity_keystore_reload_seconds` WITHOUT dropping a connection: the next handshake presents the new
  chain and key. A replacement that does not validate is refused (CRITICAL) and the node keeps its identity. Trust anchors
  and the revocation file are reloaded the same way.
- **Expiry is alarmed** by the `identity` health check: WARN within the certificate's renewal window (a third of its
  lifetime, or `identity_renew_before_days` if longer, at most half), CRITICAL in its last tenth or last week, and when
  expired.

## The offline PKI tool: `tools/cresco-pki.sh`

Requires OpenSSL 3. Run it on an offline, owner-only machine: the PKI directory holds the CA keys. Everything it writes is
owner-only (`umask 077`, files `600`, directories `700`).

```
tools/cresco-pki.sh init   <pki> [--name "Cresco Fabric Root"] [--days 3650] [--key rsa|ec]
tools/cresco-pki.sh region <pki> <tenant> <region> [--days 1825]
tools/cresco-pki.sh node   <pki> <tenant> <region> <agent> [--controller|--global] [--issuer root|region] [--days 365]
tools/cresco-pki.sh api    <pki> <tenant> <user> <admin|user> [--days 365]
tools/cresco-pki.sh revoke <pki> <certificate.pem>
tools/cresco-pki.sh crl    <pki> [--days 30]
```

- `init` makes the fabric root (RSA 3072 by default, `--key ec` for P-384).
- `region` makes a region CA, scoped explicitly (`cresco:ca:<tenant>/<region>`, `cresco:ca-roles:node,controller`).
  `<pki>/trust-anchors.pem` holds the ROOT only; a region CA reaches nodes as an intermediate in each node's keystore chain.
- `node` issues a node certificate (by its region's CA when one exists, else the root; `--global`: THE global controller,
  always by the root, carrying `cresco:role:controller` and `cresco:role:global`) and writes
  `<pki>/nodes/<region>_<agent>/{node.p12, node.p12.pass, trust-anchors.pem, node.pem}`; the key exists only inside the
  keystore. Issuing the same node again makes a NEW key and replaces `node.p12` atomically, keeping its password file: that
  is a renewal.
- `api` issues an API user's client certificate (`<pki>/api/<tenant>_<user>/api.p12`).
- `revoke` records a certificate (a node's, or a region CA's) in its issuer's CRL database and its key in
  `<pki>/revocations.txt`; `crl` writes `<pki>/revocations.pem` (the region CA certificates, every CA's CRL, then the deny
  list) -- the file to give every node as `revocation_file`. Revoking a region CA stops all of its leaves.

A node is then started with

```
-Dsecurity_profile=production -Didentity_keystore=<dir>/node.p12 -Didentity_keystore_password_file=<dir>/node.p12.pass \
-Dtrust_anchors=<dir>/trust-anchors.pem -Drevocation_file=<shared>/revocations.pem -Dregionname=<region> -Dagentname=<agent>
```

Deny-list format (`revocation_file`, alongside or instead of CRLs): one entry per line, `#` comments:
`key:<SHA-256 of a SubjectPublicKeyInfo, hex>`, `cert:<SHA-256 of a certificate, hex>`, `name:<region>/<agent>`. A CRL in
the file must be signed by a trust anchor or by a CA certificate the file also carries (PEM) that chains to one.

## Migration (PKIs made by `cresco-pki.sh` before 2026-11-04)

The earlier tool put every region CA into `trust-anchors.pem` and gave region CAs no explicit scope. Such a PKI is refused
(fail closed): `trust_anchors` with a non-root CA that has no explicit scope stops the node (`REFUSING TO START ...
trust_anchors ... is neither a root ... nor a CA with an explicit Cresco scope`), and a leaf issued by an unscoped region CA
is not accepted. Re-make the PKI with the current tool (`init`, `region`, `node` -- the global with `--global`, `crl`) and
give every node its new `node.p12`, `node.p12.pass`, `trust-anchors.pem` (the root only) and the new `revocations.pem`.
An operator PKI made by other means must scope every intermediate explicitly (the SAN URIs above) and list only roots (or
explicitly scoped CAs) in `trust_anchors`.

## Tests

`ProvisionedIdentityTest` (start, refusals, verification by the PKI, discovery without enrollment or pins, keystore
replacement, expiry alarm, revocation-file reload), `PkiToolTest` (the tool's output loads; anchors are the root only; its
CRL and deny list refuse a revoked node, and a revoked region CA all of its leaves; skipped without OpenSSL 3),
`IdentityRound3FixTest` / `IdentityRound3DiscoveryTest` and `ReviewIdentity3Test` / `ReviewIdentity3DiscoveryTest` (whole-path
validity and revocation, explicit CA scopes, the global role, the directory checks), and the live proof
`docs/tests/provisioned_live_test.sh`.
