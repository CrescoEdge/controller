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
| `trust_anchors` | PEM bundle of the fabric's CA certificates (the root, and the region CAs) |
| `revocation_file` | optional: the operator's revocations -- PEM CRLs issued by the PKI and/or a deny list (below) |
| `identity_keystore_reload_seconds` | 30: how often the files are checked for replacement |
| `regionname`, `agentname`, `tenant_id` | must match the certificate (unset names are taken from it) |

The keystore and its password file must be regular files (not symbolic links), writable and readable by the owner only;
the anchors and revocation files must not be writable by group or others. Anything else and the node **refuses to start**.

## Identity names

What the broker and the peer trust registry use is the leaf's subject, read strictly:

- **node**: `CN=<agent>, OU=<region>, O=<tenant>` (Cresco name grammar: 1-64 of `A-Z a-z 0-9 . -`; a tenant has no `.`),
  with the SAN URI `cresco:node:<tenant>/<region>/<agent>` (when present it must name the same identity);
- **controller** (a region or global controller: it answers discovery, bridges, sends revocation lists): also the SAN URI
  `cresco:role:controller`; a node configured as `is_region`/`is_global` refuses to start without it, and a joiner refuses a
  responder without it;
- **API user** (wsapi): SAN URIs `cresco:api:<tenant>/<user>` and `cresco:role:<admin|user>`, subject `CN=<user>, O=<tenant>`
  (no `OU`, so it can never name a node);
- **CA scope**: a CA whose subject names a region and tenant (`OU`, `O`) may issue only that region's and tenant's names; the
  root (no `OU`) may issue any.

## At start

The chain is validated (PKIX: signatures, validity, CA basic constraints and path length) from the leaf to an anchor, the
CA scope is checked, the key must match the certificate, and the operator's revocations must not list it. The node refuses
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
tools/cresco-pki.sh node   <pki> <tenant> <region> <agent> [--controller] [--issuer root|region] [--days 365]
tools/cresco-pki.sh api    <pki> <tenant> <user> <admin|user> [--days 365]
tools/cresco-pki.sh revoke <pki> <certificate.pem>
tools/cresco-pki.sh crl    <pki> [--days 30]
```

- `init` makes the fabric root (RSA 3072 by default, `--key ec` for P-384).
- `region` makes a region CA (its scope is that region and tenant) and refreshes `<pki>/trust-anchors.pem`.
- `node` issues a node certificate (by its region's CA when one exists, else the root) and writes
  `<pki>/nodes/<region>_<agent>/{node.p12, node.p12.pass, trust-anchors.pem, node.pem}`; the key exists only inside the
  keystore. Issuing the same node again makes a NEW key and replaces `node.p12` atomically, keeping its password file: that
  is a renewal.
- `api` issues an API user's client certificate (`<pki>/api/<tenant>_<user>/api.p12`).
- `revoke` records a certificate in its issuer's CRL database and its key in `<pki>/revocations.txt`; `crl` writes
  `<pki>/revocations.pem` (every CA's CRL, then the deny list) -- the file to give every node as `revocation_file`.

A node is then started with

```
-Dsecurity_profile=production -Didentity_keystore=<dir>/node.p12 -Didentity_keystore_password_file=<dir>/node.p12.pass \
-Dtrust_anchors=<dir>/trust-anchors.pem -Drevocation_file=<shared>/revocations.pem -Dregionname=<region> -Dagentname=<agent>
```

Deny-list format (`revocation_file`, alongside or instead of CRLs): one entry per line, `#` comments:
`key:<SHA-256 of a SubjectPublicKeyInfo, hex>`, `cert:<SHA-256 of a certificate, hex>`, `name:<region>/<agent>`. A CRL in
the file must be signed by a CA of the trust anchors.

## Tests

`ProvisionedIdentityTest` (start, refusals, verification by the PKI, discovery without enrollment or pins, keystore
replacement, expiry alarm, revocation-file reload), `PkiToolTest` (the tool's output loads; its CRL and deny list refuse a
revoked node; skipped without OpenSSL 3), and the live proof `docs/tests/provisioned_live_test.sh`.
