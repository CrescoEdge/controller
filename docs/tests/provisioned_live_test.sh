#!/usr/bin/env bash
# Live proof of PRE-PROVISIONED identities (security_profile=production, docs/pre-provisioned-identity.md) on one host:
#
#   global glob/gc (THE global: --global, by the root)  agent glob/a1 (issued by the root)
#   region rgr/rc  (controller, issued by region CA rgr) agent rgr/b1  (issued by region CA rgr)
#
# every identity made beforehand by tools/cresco-pki.sh, and NO shared secret anywhere (no discovery secret, no service key:
# discovery runs on the PKI, API clients present certificates). Checks: the mesh comes up with NO enrollment, generation or
# first-use binding, over the PKI discovery exchange; messages are signed (direct links attested first hand); a node
# configured with a discovery secret refuses to start; a keystore replaced in place (a renewal, new key) is reloaded
# without dropping a connection and is presented at the next handshake; a node whose certificate comes from another PKI is
# refused; a node the operator revokes is closed and refused, and does not start again.
#
# Usage: provisioned_live_test.sh <agent.jar> <workdir> <python-with-pycrescolib>
#   PORT0 (default 33000) and WS0 (default 8482) move the port block. Starts and stops only its own JVMs.
set -uo pipefail
JAR="$1"; WORK="$2"; PY="$3"; shift 3
HERE="$(cd "$(dirname "$0")" && pwd)"; TOOL="$HERE/../../tools/cresco-pki.sh"
P0="${PORT0:-33000}"; WS0="${WS0:-8482}"
rm -rf "$WORK"; mkdir -p "$WORK/logs"
SEC=""          # production: no discovery secret, no service key
COMMON="-Djava.net.preferIPv4Stack=true -Xmx1024M -Droot_log_level=INFO -Denable_console=false -Denable_dashboard=false"
PKI="$WORK/pki"
echo "### the operator's PKI (offline): root, region CA rgr, two controllers, two agents"
bash "$TOOL" init "$PKI" --days 30 >/dev/null && bash "$TOOL" region "$PKI" t1 rgr --days 30 >/dev/null \
  && bash "$TOOL" node "$PKI" t1 glob gc --global --days 10 >/dev/null && bash "$TOOL" node "$PKI" t1 rgr rc --controller --days 10 >/dev/null \
  && bash "$TOOL" node "$PKI" t1 glob a1 --days 10 >/dev/null && bash "$TOOL" node "$PKI" t1 rgr b1 --days 10 >/dev/null \
  && bash "$TOOL" api "$PKI" t1 probe admin --days 10 >/dev/null \
  && bash "$TOOL" crl "$PKI" >/dev/null || { echo "FATAL: the PKI tool failed"; exit 2; }
# plugin signing is mandatory in production: the anchors network-installed jars are signed to (none is installed here)
openssl req -x509 -newkey rsa:3072 -sha384 -days 10 -nodes -keyout "$WORK/codesign.key" -out "$WORK/codesign.pem" \
  -subj "/O=t1/CN=test code signing (made at test time)" >/dev/null 2>&1 && chmod 600 "$WORK/codesign.key" "$WORK/codesign.pem"
# the probe is an API user of tenant t1: a client certificate from the PKI (wsapi in production takes nothing else)
export CRESCO_TLS_CLIENT_CERT="$PKI/api/t1_probe/api.p12"; CRESCO_TLS_CLIENT_KEY_PASSWORD="$(cat "$PKI/api/t1_probe/api.p12.pass")"
export CRESCO_TLS_CLIENT_KEY_PASSWORD
KEY=
PIDS="$WORK/pids"; : > "$PIDS"
launch() { # name region agent flags [append]
  local d="$WORK/nodes/$1" n="$PKI/nodes/$2_$3"; mkdir -p "$d"
  local id="-Dsecurity_profile=production -Dbroker_security_enabled=true -Dtenant_id=t1 -Dpeer_identity_mode=enforce -Dregionname=$2 -Dagentname=$3"
  id="$id -Didentity_keystore=$n/node.p12 -Didentity_keystore_password_file=$n/node.p12.pass -Dtrust_anchors=$n/trust-anchors.pem"
  id="$id -Drevocation_file=$PKI/revocations.pem -Didentity_keystore_reload_seconds=5"
  id="$id -Dplugin_signing_truststore=$WORK/codesign.pem -Dwsapi_client_ca=$n/trust-anchors.pem"
  echo "$2 $3 $4" > "$WORK/args.$1"
  if [ "${5:-}" = append ]; then ( cd "$d" && exec java $COMMON $SEC $id $4 -jar "$JAR" ) >> "$WORK/logs/$1.log" 2>&1 < /dev/null &
  else ( cd "$d" && exec java $COMMON $SEC $id $4 -jar "$JAR" ) > "$WORK/logs/$1.log" 2>&1 < /dev/null & fi
  echo $! >> "$PIDS"; echo $! > "$WORK/pid.$1"
}
stopall() { while read -r p; do kill "$p" 2>/dev/null; done < "$PIDS"; sleep 3; while read -r p; do kill -9 "$p" 2>/dev/null; done < "$PIDS"; }
trap stopall EXIT
waitport() { for _ in $(seq 1 90); do nc -z 127.0.0.1 "$1" 2>/dev/null && return 0; sleep 2; done; return 1; }
stopnode() { local p; p=$(cat "$WORK/pid.$1"); kill "$p" 2>/dev/null; for _ in $(seq 1 30); do kill -0 "$p" 2>/dev/null || break; sleep 1; done; kill -9 "$p" 2>/dev/null; }

echo "### global glob/gc, region rgr/rc, agents glob/a1 and rgr/b1 -- all pre-provisioned"
launch global glob gc "-Dis_global=true -Dnetdiscoveryport=$((P0+5)) -Dbroker_port=$((P0+10)) -Ddiscovery_port=$((P0+10)) -Denable_wsapi=true -Dwsapi_port=$WS0"
waitport "$WS0" || { echo "FATAL: global did not come up"; exit 2; }
launch region rgr rc "-Dis_region=true -Dglobal_controller_host=127.0.0.1 -Dglobal_controller_port=$((P0+5)) -Dnetdiscoveryport=$((P0+15)) -Dbroker_port=$((P0+20)) -Ddiscovery_port_remote=$((P0+10)) -Denable_wsapi=false"
waitport $((P0+20)) && waitport $((P0+15)) || { echo "FATAL: region did not come up"; exit 2; }
launch a1 glob a1 "-Dis_agent=true -Dregional_controller_host=127.0.0.1 -Dregional_controller_port=$((P0+5)) -Dnetdiscoveryport=$((P0+25)) -Denable_wsapi=true -Dwsapi_port=$((WS0+1))"
launch b1 rgr b1 "-Dis_agent=true -Dregional_controller_host=127.0.0.1 -Dregional_controller_port=$((P0+15)) -Dnetdiscoveryport=$((P0+35)) -Denable_wsapi=true -Dwsapi_port=$((WS0+2))"
waitport $((WS0+1)) && waitport $((WS0+2)) || { echo "FATAL: agents did not come up"; exit 2; }
for _ in $(seq 1 60); do grep -q "Broker bridge from verified controller CrescoIdentity{tenant=t1, region=rgr, agent=rc" "$WORK/logs/global.log" 2>/dev/null && break; sleep 2; done
sleep 15

P=0; F=0
ck() { if [ "$1" = "1" ]; then P=$((P+1)); echo "PASS  $2"; else F=$((F+1)); echo "FAIL  $2"; fi; }
has() { grep -qE "$2" "$WORK/logs/$1.log" 2>/dev/null && echo 1; }
none() { grep -qE "$2" "$WORK/logs/$1.log" 2>/dev/null || echo 1; }
cnt() { grep -cE "$2" "$WORK/logs/$1.log" 2>/dev/null || true; }
for n in global region a1 b1; do
  ck "$(has $n 'PRE-PROVISIONED identity CrescoIdentity')" "$n: runs on its pre-provisioned identity"
  ck "$(none $n 'Enrolled node|Region CA established|Issuing identity-bearing leaf|Installed region-CA-signed|Re-issued own certificate|Federated with|REFUSING TO START')" "$n: nothing enrolled, issued, generated or federated"
done
ck "$(has a1 'Peer identity verified: broker connection .* is region=glob, agent=gc')" "a1's broker connections are bound to the global glob/gc"
ck "$(has b1 'Peer identity verified: broker connection .* is region=rgr, agent=rc')" "b1's broker connections are bound to the region rgr/rc"
ck "$(has global 'Broker bridge from verified controller CrescoIdentity\{tenant=t1, region=rgr, agent=rc')" "the global admitted the region's bridge (a controller of the PKI)"
ck "$(has region 'mTLS identity bound from certificate: CrescoIdentity\{tenant=t1, region=rgr, agent=b1')" "the region identified b1 by its PKI certificate"
ck "$(has global 'mTLS identity bound from certificate: CrescoIdentity\{tenant=t1, region=glob, agent=a1')" "the global identified a1 by its PKI certificate"
ck "$(has global 'discovery \(PKI\): authenticated GLOBAL exchange with rgr/rc')" "the region joined the global over the PKI discovery exchange (no secret)"
ck "$(has b1 'discovery \(PKI\): authenticated AGENT exchange with rgr/rc')" "b1 joined its region over the PKI discovery exchange"
ck "$(has a1 'discovery \(PKI\): authenticated AGENT exchange with glob/gc')" "a1 joined the global over the PKI discovery exchange"
for n in global region a1 b1; do
  ck "$(none $n 'discovery v2: authenticated|LEGACY')" "$n: no secret-keyed or legacy discovery exchange"
  ck "$(has $n 'message signatures ON: this node signs')" "$n: signs the messages it originates past its direct links"
done
ck "$(has region 'direct links ON: this node accepts unsigned messages its broker attests')" "the region accepts its direct children's messages first hand (no signature)"
pin() { local f; f=$(find "$WORK/nodes/$1/cresco-data/plugin-data" -name ws.fingerprint 2>/dev/null | head -1); [ -n "$f" ] && head -1 "$f" | tr -d '[:space:]'; }
probe() {
  PYTHONPATH="${PYCRESCOLIB_PATH:-}${PYTHONPATH:+:$PYTHONPATH}" "$PY" "$HERE/peer_identity_dataplane.py" --tx-port "$WS0" --rx-port $((WS0+2)) \
    --spy-port $((WS0+1)) --spy2-port "$WS0" --rx-path rgr_b1 --key "$KEY" --messages 200 \
    --pin "$WS0=$(pin global)" --pin "$((WS0+1))=$(pin a1)" --pin "$((WS0+2))=$(pin b1)"
}
PROBE=$(probe); echo "$PROBE" | tail -1
ck "$(echo "$PROBE" | grep -q '"plain_received": 200' && echo 1)" "a stream crossed global -> bridge -> region -> b1 in full (200/200)"

echo "### renewal: the operator re-issues rgr/rc (NEW key) and its keystore is replaced in place"
B1C='mTLS identity bound from certificate: CrescoIdentity\{tenant=t1, region=rgr, agent=b1'
b1before=$(cnt region "$B1C"); rcb=$(cnt global 'Broker bridge from verified controller CrescoIdentity\{tenant=t1, region=rgr, agent=rc')
bash "$TOOL" node "$PKI" t1 rgr rc --controller --days 20 >/dev/null
for _ in $(seq 1 20); do grep -q "Pre-provisioned identity RELOADED" "$WORK/logs/region.log" && break; sleep 1; done
ck "$(has region 'Pre-provisioned identity RELOADED: .* with a NEW key')" "the region reloaded its replaced keystore (a new key) without a restart"
sleep 15
ck "$( [ "$(cnt region "$B1C")" = "$b1before" ] && [ "$(cnt global 'Broker bridge from verified controller CrescoIdentity\{tenant=t1, region=rgr, agent=rc')" = "$rcb" ] && echo 1)" "no connection was dropped by the reload (b1 and the region's bridges stayed up)"
echo "### b1 restarts: its new connection verifies the region's NEW certificate"
stopnode b1; v0=$(cnt b1 'Peer identity verified: broker connection .* is region=rgr, agent=rc'); a=( $(cat "$WORK/args.b1") ); launch b1 "${a[0]}" "${a[1]}" "${a[*]:2}" append
waitport $((WS0+2)); sleep 20
ck "$( [ "$(cnt b1 'Peer identity verified: broker connection .* is region=rgr, agent=rc')" -gt "$v0" ] && echo 1)" "b1 reconnected and verified the region's certificate"
want=$(openssl x509 -in "$PKI/nodes/rgr_rc/node.pem" -noout -serial | cut -d= -f2)
served=$(echo | openssl s_client -connect 127.0.0.1:$((P0+20)) 2>/dev/null | openssl x509 -noout -serial 2>/dev/null | cut -d= -f2)
echo "  the region's broker presents certificate serial ${served:-none}; the re-issued one is $want"
ck "$( [ -n "$served" ] && [ "$served" = "$want" ] && echo 1)" "the region's broker presents the RE-ISSUED certificate at a new handshake (no restart)"

echo "### a node with a certificate from ANOTHER PKI tries to join region rgr"
OTHER="$WORK/other-pki"; bash "$TOOL" init "$OTHER" --days 30 >/dev/null && bash "$TOOL" node "$OTHER" t1 rgr b9 --days 10 >/dev/null
d="$WORK/nodes/b9"; mkdir -p "$d"; n="$OTHER/nodes/rgr_b9"
( cd "$d" && exec java $COMMON $SEC -Dsecurity_profile=production -Dbroker_security_enabled=true -Dtenant_id=t1 -Dregionname=rgr -Dagentname=b9 \
  -Didentity_keystore=$n/node.p12 -Didentity_keystore_password_file=$n/node.p12.pass -Dtrust_anchors=$n/trust-anchors.pem \
  -Dplugin_signing_truststore=$WORK/codesign.pem \
  -Dis_agent=true -Dregional_controller_host=127.0.0.1 -Dregional_controller_port=$((P0+15)) -Dnetdiscoveryport=$((P0+45)) -Denable_wsapi=false -jar "$JAR" ) \
  > "$WORK/logs/b9.log" 2>&1 < /dev/null &
echo $! >> "$PIDS"; echo $! > "$WORK/pid.b9"
for _ in $(seq 1 45); do grep -qE "REFUSED (rgr/b9|the hello of rgr/b9)" "$WORK/logs/region.log" && break; sleep 2; done
ck "$(has region 'REFUSED (rgr/b9|the hello of rgr/b9 from [^:]*): .*(does not validate to the fabric.s trust anchors|chain)')" "the region refused rgr/b9: its certificate is not of the fabric's PKI"
ck "$(none region 'mTLS identity bound from certificate: CrescoIdentity\{tenant=t1, region=rgr, agent=b9')" "rgr/b9 was never identified"
stopnode b9

echo "### a production node configured with a discovery secret refuses to start"
d="$WORK/nodes/s1"; mkdir -p "$d"; n="$PKI/nodes/rgr_b1"
openssl rand -hex 16 > "$WORK/leftover.secret"; chmod 600 "$WORK/leftover.secret"     # a leftover from a standard fabric
( cd "$d" && exec java $COMMON -Dsecurity_profile=production -Dtenant_id=t1 -Dregionname=rgr -Dagentname=b1 -Ddiscovery_secret_agent_file=$WORK/leftover.secret \
  -Didentity_keystore=$n/node.p12 -Didentity_keystore_password_file=$n/node.p12.pass -Dtrust_anchors=$n/trust-anchors.pem \
  -Dplugin_signing_truststore=$WORK/codesign.pem -Dis_agent=true -Dnetdiscoveryport=$((P0+55)) -Denable_wsapi=false -jar "$JAR" ) \
  > "$WORK/logs/s1.log" 2>&1 < /dev/null &
sp=$!; echo $sp >> "$PIDS"; for _ in $(seq 1 30); do kill -0 $sp 2>/dev/null || break; sleep 1; done; kill $sp 2>/dev/null
ck "$(has s1 'REFUSING TO START: .*discovery_secret_agent is set: production discovery is authenticated by the operator.s PKI only')" "a production node with a discovery secret refuses to start, naming it"

echo "### the operator revokes rgr/b1 (tool revoke + crl; every node reads the shared revocation file)"
bash "$TOOL" revoke "$PKI" "$PKI/nodes/rgr_b1/node.pem" >/dev/null && bash "$TOOL" crl "$PKI" >/dev/null
for _ in $(seq 1 30); do grep -q "CLOSING the connection of CrescoIdentity{tenant=t1, region=rgr, agent=b1" "$WORK/logs/region.log" && break; sleep 2; done
ck "$(has region 'CLOSING the connection of CrescoIdentity\{tenant=t1, region=rgr, agent=b1.*revoked by the operator')" "the region closed b1's live connection: revoked by the operator"
sleep 15
mark=$(grep -n "CLOSING the connection of CrescoIdentity{tenant=t1, region=rgr, agent=b1" "$WORK/logs/region.log" | head -1 | cut -d: -f1)
ck "$( [ -z "$(tail -n +$((mark + 1)) "$WORK/logs/region.log" | grep -E "$B1C")" ] && echo 1)" "b1 was never identified again"
stopnode b1; a=( $(cat "$WORK/args.b1") ); launch b1 "${a[0]}" "${a[1]}" "${a[*]:2}" append; sleep 15
ck "$(has b1 'REFUSING TO START: the pre-provisioned identity cannot be used: .*REVOKED')" "the revoked b1 refuses to start"
echo "  PASS=$P  FAIL=$F  (logs: $WORK/logs)"
exit $F
