#!/usr/bin/env bash
# Cresco offline PKI: issues every identity of a pre-provisioned fabric (security_profile=production) ahead of time.
# Requires OpenSSL 3. Keep the PKI directory OFFLINE and owner-only: it holds the CA keys.
#
#   cresco-pki.sh init   <pki> [--name "Cresco Fabric Root"] [--days 3650] [--key rsa|ec]
#   cresco-pki.sh region <pki> <tenant> <region> [--days 1825]
#   cresco-pki.sh node   <pki> <tenant> <region> <agent> [--controller] [--issuer root|region] [--days 365]
#   cresco-pki.sh api    <pki> <tenant> <user> <admin|user> [--days 365]
#   cresco-pki.sh revoke <pki> <certificate.pem>
#   cresco-pki.sh crl    <pki> [--days 30]
#
# Names (what the broker and the registry use): a node's subject is CN=<agent>, OU=<region>, O=<tenant>, with the SAN URI
# cresco:node:<tenant>/<region>/<agent>; a controller (a region or global controller) also carries cresco:role:controller.
# A region CA's subject names its region and tenant (OU, O): it can issue only that region's names. An API user's
# certificate carries cresco:api:<tenant>/<user> and cresco:role:<admin|user> (wsapi's mapping) and names no node.
#
# Output, per node: <pki>/nodes/<region>_<agent>/{node.p12, node.p12.pass, trust-anchors.pem, node.pem} -- give the node
#   identity_keystore=node.p12  identity_keystore_password_file=node.p12.pass  trust_anchors=trust-anchors.pem
# and, fabric-wide, revocation_file=<pki>/revocations.pem (written by `crl`). Everything is created owner-only.
set -euo pipefail
umask 077
die() { echo "cresco-pki: $*" >&2; exit 2; }
command -v openssl >/dev/null || die "openssl (3.x) is required"
openssl version | grep -q '^OpenSSL 3' || die "OpenSSL 3.x is required ($(openssl version))"

name_ok() { [[ "$1" =~ ^[A-Za-z0-9]([A-Za-z0-9.-]{0,62}[A-Za-z0-9])?$ ]] || die "invalid name '$1' (1-64 of A-Z a-z 0-9 . -, starting and ending with a letter or digit)"; }
tenant_ok() { name_ok "$1"; [[ "$1" != *.* ]] || die "a tenant name has no '.' ('$1')"; }
opt() { local key=$1 def=$2; shift 2; while [ $# -gt 0 ]; do [ "$1" = "$key" ] && { echo "$2"; return; }; shift; done; echo "$def"; }
flag() { local key=$1; shift; for a in "$@"; do [ "$a" = "$key" ] && { echo 1; return; }; done; echo 0; }
newkey() { # file alg
  if [ "$2" = ec ]; then openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-384 -out "$1" 2>/dev/null
  else openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:3072 -out "$1" 2>/dev/null; fi
  chmod 600 "$1"
}
randpass() { openssl rand -hex 32; }
spkifp() { openssl x509 -in "$1" -pubkey -noout | openssl pkey -pubin -outform DER | openssl dgst -sha256 -r | cut -d' ' -f1; }
certfp() { openssl x509 -in "$1" -outform DER | openssl dgst -sha256 -r | cut -d' ' -f1; }

# a CA directory: openssl ca's database and config
cadir() { # dir subject
  mkdir -p "$1/issued"; chmod 700 "$1" "$1/issued"; : > "$1/index.txt"; echo 1000 > "$1/crlnumber"
  openssl rand -hex 16 > "$1/serial"
  cat > "$1/openssl.cnf" <<EOF
[ ca ]
default_ca = cresco
[ cresco ]
dir = $1
database = \$dir/index.txt
new_certs_dir = \$dir/issued
certificate = \$dir/ca.pem
private_key = \$dir/ca.key
serial = \$dir/serial
crlnumber = \$dir/crlnumber
default_md = sha384
policy = anything
unique_subject = no
copy_extensions = none
default_crl_days = 30
[ anything ]
countryName = optional
organizationName = optional
organizationalUnitName = optional
commonName = supplied
[ req ]
distinguished_name = dn
prompt = no
[ dn ]
CN = unused
EOF
}

anchors() { # rewrite <pki>/trust-anchors.pem: the root and every region CA
  local pki=$1
  cat "$pki/root/ca.pem" > "$pki/trust-anchors.pem"
  for d in "$pki"/regions/*/; do [ -f "$d/ca.pem" ] && cat "$d/ca.pem" >> "$pki/trust-anchors.pem"; done
  chmod 600 "$pki/trust-anchors.pem"
}

sign() { # issuer-dir csr out days extfile
  openssl ca -batch -config "$1/openssl.cnf" -in "$2" -out "$3" -days "$4" -extfile "$5" -extensions ext -notext >/dev/null 2>&1 \
    || { openssl ca -batch -config "$1/openssl.cnf" -in "$2" -out "$3" -days "$4" -extfile "$5" -extensions ext -notext; die "signing failed"; }
}

cmd=${1:-}; shift || true
case "$cmd" in
init)
  pki=${1:?pki directory}; shift
  [ -e "$pki/root/ca.pem" ] && die "$pki already holds a root CA"
  mkdir -p "$pki/root" "$pki/regions" "$pki/nodes" "$pki/api"; chmod 700 "$pki" "$pki/root" "$pki/regions" "$pki/nodes" "$pki/api"
  cadir "$pki/root" x
  alg=$(opt --key rsa "$@"); echo "$alg" > "$pki/keyalg"
  newkey "$pki/root/ca.key" "$alg"
  printf '[ req ]\ndistinguished_name = dn\nprompt = no\n[ dn ]\nCN = unused\n[ ext ]\nbasicConstraints = critical, CA:true\nkeyUsage = critical, keyCertSign, cRLSign\nsubjectKeyIdentifier = hash\n' > "$pki/root/req.cnf"
  openssl req -x509 -new -key "$pki/root/ca.key" -subj "/O=Cresco/CN=$(opt --name 'Cresco Fabric Root' "$@")" -days "$(opt --days 3650 "$@")" \
    -sha384 -config "$pki/root/req.cnf" -extensions ext -out "$pki/root/ca.pem"
  chmod 600 "$pki/root/ca.pem"
  : > "$pki/revocations.txt"; chmod 600 "$pki/revocations.txt"
  anchors "$pki"
  echo "root CA: $pki/root/ca.pem (key fingerprint $(spkifp "$pki/root/ca.pem"))"
  ;;
region)
  pki=${1:?pki}; tenant=${2:?tenant}; region=${3:?region}; shift 3
  tenant_ok "$tenant"; name_ok "$region"
  d="$pki/regions/${tenant}_${region}"; [ -e "$d/ca.pem" ] && die "region CA $tenant/$region exists"
  cadir "$d" x
  newkey "$d/ca.key" "$(cat "$pki/keyalg")"
  openssl req -new -key "$d/ca.key" -subj "/O=$tenant/OU=$region/CN=$region CA" -config "$d/openssl.cnf" -out "$d/ca.csr" 2>/dev/null
  printf '[ ext ]\nbasicConstraints = critical, CA:true, pathlen:0\nkeyUsage = critical, keyCertSign, cRLSign\nsubjectKeyIdentifier = hash\nauthorityKeyIdentifier = keyid\n' > "$d/ext.cnf"
  sign "$pki/root" "$d/ca.csr" "$d/ca.pem" "$(opt --days 1825 "$@")" "$d/ext.cnf"
  cat "$d/ca.pem" "$pki/root/ca.pem" > "$d/chain.pem"; chmod 600 "$d/ca.pem" "$d/chain.pem"; rm -f "$d/ca.csr"
  anchors "$pki"
  echo "region CA $tenant/$region: $d/ca.pem (it issues only names of $tenant/$region)"
  ;;
node|api)
  pki=${1:?pki}; shift
  if [ "$cmd" = node ]; then
    tenant=${1:?tenant}; region=${2:?region}; agent=${3:?agent}; shift 3
    tenant_ok "$tenant"; name_ok "$region"; name_ok "$agent"
    out="$pki/nodes/${region}_${agent}"; subj="/O=$tenant/OU=$region/CN=$agent"; base=node
    san="URI.1 = cresco:node:$tenant/$region/$agent"
    [ "$(flag --controller "$@")" = 1 ] && san="$san
URI.2 = cresco:role:controller"
    eku="serverAuth, clientAuth"
    issuer="$pki/root"
    if [ "$(opt --issuer region "$@")" = region ] && [ -f "$pki/regions/${tenant}_${region}/ca.pem" ]; then issuer="$pki/regions/${tenant}_${region}"; fi
  else
    tenant=${1:?tenant}; user=${2:?user}; role=${3:?admin|user}; shift 3
    tenant_ok "$tenant"; name_ok "$user"; [ "$role" = admin ] || [ "$role" = user ] || die "role must be admin or user"
    out="$pki/api/${tenant}_${user}"; subj="/O=$tenant/CN=$user"; base=api
    san="URI.1 = cresco:api:$tenant/$user
URI.2 = cresco:role:$role"
    eku="clientAuth"; issuer="$pki/root"
  fi
  mkdir -p "$out"; chmod 700 "$out"
  newkey "$out/$base.key" "$(cat "$pki/keyalg")"
  openssl req -new -key "$out/$base.key" -subj "$subj" -config "$issuer/openssl.cnf" -out "$out/$base.csr" 2>/dev/null
  cat > "$out/ext.cnf" <<EOF
[ ext ]
basicConstraints = critical, CA:false
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = $eku
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid
subjectAltName = @san
[ san ]
$san
EOF
  sign "$issuer" "$out/$base.csr" "$out/$base.pem" "$(opt --days 365 "$@")" "$out/ext.cnf"
  if [ "$issuer" = "$pki/root" ]; then cp "$pki/root/ca.pem" "$out/issuer-chain.pem"; else cp "$issuer/chain.pem" "$out/issuer-chain.pem"; fi
  # a re-issued identity keeps its password file: a running node then sees only its keystore replaced (atomically)
  [ -s "$out/$base.p12.pass" ] || { printf '%s\n' "$(randpass)" > "$out/$base.p12.pass"; chmod 600 "$out/$base.p12.pass"; }
  openssl pkcs12 -export -inkey "$out/$base.key" -in "$out/$base.pem" -certfile "$out/issuer-chain.pem" -name "$base" \
    -passout "file:$out/$base.p12.pass" -out "$out/$base.p12.new"
  chmod 600 "$out/$base.p12.new"; mv -f "$out/$base.p12.new" "$out/$base.p12"     # a replaced keystore is reloaded by a running node
  cp "$pki/trust-anchors.pem" "$out/trust-anchors.pem"
  chmod 600 "$out/$base.pem" "$out/trust-anchors.pem" "$out/issuer-chain.pem"
  rm -f "$out/$base.key" "$out/$base.csr" "$out/ext.cnf"                          # the key lives only in the keystore
  echo "$cmd ${subj}: $out/$base.p12 (key fingerprint $(spkifp "$out/$base.pem"), serial $(openssl x509 -in "$out/$base.pem" -noout -serial | cut -d= -f2))"
  ;;
revoke)
  pki=${1:?pki}; cert=${2:?certificate.pem}
  iss=$(openssl x509 -in "$cert" -noout -issuer -nameopt RFC2253 | sed 's/^issuer=//')
  issuer=""
  for d in "$pki/root" "$pki"/regions/*; do
    [ -f "$d/ca.pem" ] || continue
    [ "$(openssl x509 -in "$d/ca.pem" -noout -subject -nameopt RFC2253 | sed 's/^subject=//')" = "$iss" ] && issuer="$d"
  done
  [ -n "$issuer" ] || die "no CA of $pki issued $cert ($iss)"
  openssl ca -config "$issuer/openssl.cnf" -revoke "$cert" >/dev/null 2>&1 || die "openssl ca -revoke failed"
  subj=$(openssl x509 -in "$cert" -noout -subject -nameopt RFC2253 | sed 's/^subject=//')
  printf 'key:%s  # %s, serial %s\n' "$(spkifp "$cert")" "$subj" "$(openssl x509 -in "$cert" -noout -serial | cut -d= -f2)" >> "$pki/revocations.txt"
  echo "revoked $subj; run: cresco-pki.sh crl $pki, then give every node the new $pki/revocations.pem"
  ;;
crl)
  pki=${1:?pki}; shift
  days=$(opt --days 30 "$@")
  tmp="$pki/revocations.pem.new"; : > "$tmp"
  for d in "$pki/root" "$pki"/regions/*; do
    [ -f "$d/ca.pem" ] || continue
    openssl ca -gencrl -config "$d/openssl.cnf" -crldays "$days" -out "$d/crl.pem" >/dev/null 2>&1 || die "CRL for $d failed"
    cat "$d/crl.pem" >> "$tmp"
  done
  { echo "# Cresco revocations: the CRL of every CA of this PKI, then the deny list (key:<SHA-256 of a SubjectPublicKeyInfo>)"; cat "$pki/revocations.txt"; } >> "$tmp"
  chmod 600 "$tmp"; mv -f "$tmp" "$pki/revocations.pem"
  echo "$pki/revocations.pem: $(grep -c 'BEGIN X509 CRL' "$pki/revocations.pem") CRL(s), $(grep -c '^key:' "$pki/revocations.txt" || true) key(s); valid $days days"
  ;;
*)
  sed -n '2,20p' "$0"; exit 2 ;;
esac
