# Production deployment: pre-provisioned identities (`security_profile=production`)

In a secure environment every certificate is computed ahead of time and assigned by the operator's PKI. Nothing in the
fabric mints, enrolls or trusts on first use, and nothing accepts a password. `security_profile=production` is that model;
`standard` (the default) keeps the enrollment conveniences; insecure debug mode (`broker_security_enabled=false`) is
never production. The upgrade path from an earlier build is `upgrade-1.3-secure-defaults.md`; the authorization
contract is `caller-authorization.md`.

The provisioned identity is loaded by the certificate manager (`pre-provisioned-identity.md`: chain validated to the
anchors, no generation, enrollment or trust on first use, hot reload of a replaced keystore); this guide and the start-up
interlocks use the same setting names.

## 1. What production requires and refuses

A controller with `security_profile=production` **refuses to start** (exit 78) and names every problem, one
`REFUSING TO START` line each, when anything below is missing or anything disallowed is set.

| Required | |
|---|---|
| `identity_keystore` | this node's private key and its certificate chain, issued by the operator's PKI: a **BCFKS** keystore (section 6: production runs FIPS approved-only, which reads no PKCS#12; convert the PKCS#12 the PKI tool writes) |
| `identity_keystore_password_file` | owner-only file holding that keystore's password |
| `trust_anchors` | PEM bundle: the fabric's root CA and region CA certificates |
| `plugin_signing_truststore` | the anchors plugin jars are signed to (a PEM bundle needs no password) |
| (mandatory) | `broker_security_enabled`, `broker_require_client_auth`, `tenant_namespacing`, `plugin_signing_required`, `msg_signing_required` all true; `peer_identity_mode=enforce` |

| Optional | |
|---|---|
| `revocation_file` | an operator CRL or deny list (must be readable when set) |
| `controller_trust_anchors` | restrict which controllers a node joins (fingerprints) |

| Refused in production | why |
|---|---|
| `broker_security_enabled=false`, `broker_require_client_auth=false`, `tenant_namespacing=false`, `plugin_signing_required=false`, `msg_signing_required=false`, `peer_identity_mode` other than `enforce`, `broker_allow_unverified_bridges=true` | security is mandatory |
| `discovery_auth=compat`, `discovery_compat_until` | no upgrade window |
| `security_regional_ca=true` | no certificates issued to nodes that join with a shared secret |
| `peer_identity_rebind`, `public_key_trusted_fingerprints` | trust comes from `trust_anchors` only; a re-keyed node is re-provisioned, not rebound |
| `cresco_service_key`, `broker_security_secret`, `keystorepwd`, `truststorepwd`, `plugin_signing_truststore_password` (and their `_file` forms) | passwords are for debug and testing only |
| `discovery_secret_agent`, `discovery_secret_region`, `discovery_secret_global` (inline, `_file` or `CRESCO_` form) | discovery is the PKI's: no shared secret is used or accepted |
| `-Ddb_password` on the command line | secrets come from `<name>_file` (owner-only) or `CRESCO_<NAME>` |

Plugins read the profile through `PluginBuilder.getSecurityProfile()` (the controller's answer, from
`AgentService.getSecurityProfile()`; `PRODUCTION` when the controller cannot be asked) and enforce their side (wsapi: no
password logins). A plugin never reads `security_profile` from its own configuration.

### Discovery in production

Discovery is authenticated by the operator's PKI alone; **no shared discovery secret is configured, used or accepted**
(one set is refused at start: a leftover is named, not silently ignored). The exchange has the same four messages as the
standard profile's discovery v2 -- HELLO, CHALLENGE, PROOF, RESULT, over fresh nonces from both sides -- but each side
*signs* the transcript with its provisioned identity key (the key its leaf carries) instead of keying a MAC with a secret:

- the joiner's whole chain must verify to `trust_anchors` as what it claims (name, role, validity, revocation, the
  region's CA scope) **before** the responder signs anything for it;
- the responder's chain must verify the same way as the controller it answered as (a GLOBAL answer only from the
  certificate carrying `cresco:role:global`), and its signature (over both nonces, both identities, both chains and its
  broker port) must verify under its leaf, before the joiner answers;
- the joiner's signature must verify under the leaf it presented: a copied (public) chain without its key gets nothing,
  and a replayed or altered exchange fails (the transcript covers the other side's fresh nonce);
- nothing is issued, enrolled or pinned on first use: the PKI decides at every exchange and every TLS handshake.

Locators (the UDP broadcast and the first TCP probe, which only find a controller) carry no secret either: a production
controller answers a PKI locator with its region, name and broker port (what its TLS certificate shows anyway) and
nothing else, and does not answer a secret-keyed locator or a legacy exchange (`Discovery: REFUSED ... authenticated by the
operator's PKI only` once per address). A production node and a standard-profile node never complete a discovery
exchange with each other. The live proof is `docs/tests/provisioned_live_test.sh` (a production mesh, no secret).

## 2. Issuing a fabric's certificates offline

Keep the root CA offline (an air-gapped machine or an HSM). Region CAs are optional intermediates. Every node certificate
names its identity in the subject -- `CN=<agent>, OU=<region>, O=<tenant>` -- with names from the Cresco grammar
(1-64 of `A-Z a-z 0-9 . -`; a tenant without `.`; never `_`). Nodes are TLS servers and clients: the leaf carries both
`serverAuth` and `clientAuth`. Keys are RSA-3072 (or EC P-384), signed with SHA-384 (CNSA). Use the offline issuance tool
`tools/cresco-pki.sh` (documented in `pre-provisioned-identity.md`: it also writes the `cresco:node:` and
`cresco:role:controller` SANs the provisioned identity reads); the OpenSSL commands below show what it produces.

```sh
# 1. fabric root CA (offline, long-lived)
openssl req -x509 -newkey rsa:3072 -sha384 -days 3650 -nodes -keyout root.key -out root.crt \
  -subj "/O=cresco-system/OU=fabric/CN=fabric-root" \
  -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign"

# 2. (optional) a region CA, issued by the root
openssl req -newkey rsa:3072 -sha384 -nodes -keyout rgA-ca.key -out rgA-ca.csr -subj "/O=t1/OU=rgA/CN=rgA-ca"
openssl x509 -req -in rgA-ca.csr -CA root.crt -CAkey root.key -CAcreateserial -sha384 -days 1825 -out rgA-ca.crt \
  -extfile <(printf "basicConstraints=critical,CA:TRUE,pathlen:0\nkeyUsage=critical,keyCertSign,cRLSign\n")

# 3. one leaf per node (here agent a1 of region rgA, tenant t1), issued by its region CA (or the root)
openssl req -newkey rsa:3072 -sha384 -nodes -keyout a1.key -out a1.csr -subj "/O=t1/OU=rgA/CN=a1"
openssl x509 -req -in a1.csr -CA rgA-ca.crt -CAkey rgA-ca.key -CAcreateserial -sha384 -days 90 -out a1.crt \
  -extfile <(printf "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth,clientAuth\nsubjectAltName=URI:cresco:node:t1/rgA/a1\n")
# a region or global controller's leaf also carries URI:cresco:role:controller in its subjectAltName

# 4. the node's keystore and its password file (owner-only), the fabric's trust anchors; then as BCFKS (section 6)
openssl rand -hex 32 > a1.p12.pass && chmod 600 a1.p12.pass
openssl pkcs12 -export -inkey a1.key -in a1.crt -certfile <(cat rgA-ca.crt root.crt) -name node \
  -keypbe AES-256-CBC -certpbe AES-256-CBC -macalg SHA384 -passout file:a1.p12.pass -out a1.p12
cat root.crt rgA-ca.crt rgB-ca.crt > trust-anchors.pem

# 5. plugin signing: a separate code-signing CA; sign every plugin jar that is installed over the network
jarsigner -keystore codesign.p12 -storetype PKCS12 -digestalg SHA-384 -sigalg SHA384withRSA \
  -tsa https://<your RFC 3161 TSA> plugin.jar codesign      # a timestamp keeps it installable after the signer expires
```

Ship each node only its own `.p12` and password file, plus `trust-anchors.pem` and the code-signing anchors. Delete the
leaf private keys from the issuing machine once the keystores are written.

## 3. Configuration

Every node of the fabric:

```
security_profile=production
tenant_id=t1
identity_keystore=/etc/cresco/node.p12
identity_keystore_password_file=/etc/cresco/node.p12.pass
trust_anchors=/etc/cresco/trust-anchors.pem
plugin_signing_truststore=/etc/cresco/codesign-anchors.pem
revocation_file=/etc/cresco/revoked.crl                  # optional
file_access_roots=/srv/cresco/shared                      # only where getfiledata/getfileinfo are wanted
```

- **Global**: `is_global=true`, its region and agent names exactly as in its certificate (`OU`, `CN`). A global that
  manages several tenants' regions is in a superuser tenant (`broker_superuser_tenants`, default `cresco-system`).
- **Region**: `is_region=true`, `global_controller_host` (or discovery), names as in its certificate.
- **Agent**: `is_agent=true`, `regional_controller_host` (or discovery), names as in its certificate.

A node's identity is its certificate's subject: the names it is configured with must match it, and a peer is trusted
because its chain verifies to `trust_anchors` -- never because it answered first.

## 4. Revocation

- Put revoked keys or certificates in `revocation_file` (the operator's CRL or deny list) on the controllers above them;
  a controller's revocations also travel down its pings as signed lists (`caller-authorization.md` section 9), and
  `peer_identity_revoke` revokes a key or a name at once on a running node.
- A revoked key is refused at TLS, at the broker (live connections holding it are closed at once), for message
  signatures, and as a signer of trust lists.
- Keep leaf lifetimes short (days to weeks): an expired certificate is never honoured, and a connection is closed when the
  certificate it opened with expires.

## 5. Re-provisioning a node

1. Issue a new key and certificate offline (section 2, step 3-4); keep the node's names.
2. Replace `identity_keystore` and its password file on the node and restart it. Its new chain verifies to the same
   anchors, so its peers accept it without any rebind step.
3. Revoke the old key (section 4) if it may have been exposed.

Re-issuing a CA: add the new CA to `trust_anchors` on every node first, re-provision the nodes under it, then remove the
old CA and revoke it.

## 6. FIPS 140-3 approved-only mode

`security_profile=production` runs the JVM in FIPS approved-only mode (the agent's `CryptoBoot`, see the agent's README):
every JCA provider that is not a FIPS 140-3 validated module is removed, Bouncy Castle FIPS (BC-FJA 2.1, CMVP #4943)
runs approved-only, and on Linux x86_64/aarch64 ACCP-FIPS (AWS-LC FIPS 3.0, CMVP #4816) serves its approved algorithms
first. TLS (the broker, wsapi) is TLS 1.3 over those modules. A non-approved algorithm (MD5, DES, RC4, ChaCha20, X25519 ...)
is refused, and a node does not start unless it is. `crypto_fips_mode=false` with this profile is refused.

Keystores are BCFKS, the FIPS module's own format: a PKCS#12 file's integrity MAC derives its key with the PKCS#12 KDF,
which FIPS does not approve, so approved-only mode cannot check one, and refuses it, saying how to convert it. Convert each
node's keystore once, offline, with the same password (the agent jar carries the module; nothing else is needed):

```sh
java -jar agent.jar --crypto convert-keystore node.p12 node.p12.pass node.bcfks
# identity_keystore=/etc/cresco/node.bcfks   identity_keystore_password_file=/etc/cresco/node.p12.pass
```

The code calls stay `KeyStore.getInstance("PKCS12")`: in FIPS mode that name is served by BCFKS (it loads BCFKS files and
stores BCFKS). `trust_anchors`, `revocation_file` and a PEM `plugin_signing_truststore` are PEM and need nothing.
`java -jar agent.jar --crypto status --fips` shows what a node will run with; `scripts/bench-crypto.sh` measures it.

## 7. Checking a production node

- Start-up: no `REFUSING TO START` line; `security profile production` is reported by `AgentService.getSecurityProfile()`;
  `crypto: FIPS 140-3 approved-only mode: ... MD5 refused` (section 6).
- `discovery (PKI): authenticated <TYPE> exchange with <region>/<agent>` for each join, `message signatures ON` lines,
  `Broker mutual-TLS ENABLED`, and `Peer identity verified` for each bridge and broker connection.
- A plugin jar that is not signed to `plugin_signing_truststore` is refused (`REFUSED to install`).
