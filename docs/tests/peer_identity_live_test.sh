#!/usr/bin/env bash
# Live proof of OUT-28 on one host: peer identity binding (agent->broker and broker bridges) and dataplane stream
# recipient binding, with mutual TLS and tenant authorization on every broker.
#
#   global  glob/gc  (wsapi :WS0)        agent glob/a1 (wsapi :WS0+1)   -- a1 joins the global's region
#   region  rgr/rc   (bridged to global) agent rgr/b1  (wsapi :WS0+2)   -- b1 joins the region
#
# Agents start WITHOUT a configured region name, so they join under a region their start-up certificate does not name
# (exercises the leaf re-issue before the discovery certificate exchange). AGENT_REGIONS=1 configures them with the
# region they join instead (the only way a pre-fix controller forms this secured fabric).
#
# Usage: peer_identity_live_test.sh <agent.jar> <workdir> <python-with-pycrescolib> [extra -D flags for every node]
#   PORT0 (default 33000) and WS0 (default 8482) move the port block. Set REGIONAL_CA=1 to run under the regional CA.
# Starts only its own JVMs and stops only those (by PID). Secrets are generated per run.
set -uo pipefail
JAR="$1"; WORK="$2"; PY="$3"; shift 3; EXTRA="$*"
HERE="$(cd "$(dirname "$0")" && pwd)"
P0="${PORT0:-33000}"; WS0="${WS0:-8482}"
rm -rf "$WORK"; mkdir -p "$WORK/logs"
rnd() { "$PY" -c 'import secrets; print(secrets.token_hex(16))'; }
KEY="$(rnd)"
SEC="-Ddiscovery_secret_global=$(rnd) -Ddiscovery_secret_region=$(rnd) -Ddiscovery_secret_agent=$(rnd) -Dcresco_service_key=$KEY"
SECURITY="-Dbroker_security_enabled=true -Dbroker_require_client_auth=true -Dtenant_id=t1 -Dpeer_identity_mode=enforce"
[ "${REGIONAL_CA:-0}" = "1" ] && SECURITY="$SECURITY -Dsecurity_regional_ca=true"
COMMON="-Djava.net.preferIPv4Stack=true -Xmx1024M -Droot_log_level=INFO -Denable_console=false -Denable_dashboard=false"
PIDS="$WORK/pids"; : > "$PIDS"

launch() { # name flags
  local d="$WORK/nodes/$1"; mkdir -p "$d"
  ( cd "$d" && exec java $COMMON $SECURITY $SEC $2 $EXTRA -jar "$JAR" ) > "$WORK/logs/$1.log" 2>&1 < /dev/null &
  echo $! >> "$PIDS"
}
stopall() { while read -r p; do kill "$p" 2>/dev/null; done < "$PIDS"; sleep 3; while read -r p; do kill -9 "$p" 2>/dev/null; done < "$PIDS"; }
trap stopall EXIT
waitport() { for _ in $(seq 1 90); do nc -z 127.0.0.1 "$1" 2>/dev/null && return 0; sleep 2; done; return 1; }

echo "### global glob/gc"
launch global "-Dis_global=true -Dregionname=glob -Dagentname=gc -Dnetdiscoveryport=$((P0+5)) -Dbroker_port=$((P0+10)) -Ddiscovery_port=$((P0+10)) -Denable_wsapi=true -Dwsapi_port=$WS0"
waitport "$WS0" || { echo "FATAL: global wsapi did not come up"; exit 2; }
echo "### region rgr/rc -> global"
launch region "-Dis_region=true -Dregionname=rgr -Dagentname=rc -Dglobal_controller_host=127.0.0.1 -Dglobal_controller_port=$((P0+5)) -Dnetdiscoveryport=$((P0+15)) -Dbroker_port=$((P0+20)) -Ddiscovery_port_remote=$((P0+10)) -Denable_wsapi=false"
waitport $((P0+20)) && waitport $((P0+15)) || { echo "FATAL: region broker/discovery did not come up"; exit 2; }
echo "### agents glob/a1 and rgr/b1 (configured region: ${AGENT_REGIONS:-0})"
RA=""; RB=""; [ "${AGENT_REGIONS:-0}" = "1" ] && { RA="-Dregionname=glob"; RB="-Dregionname=rgr"; }
launch a1 "-Dis_agent=true $RA -Dagentname=a1 -Dregional_controller_host=127.0.0.1 -Dregional_controller_port=$((P0+5)) -Dnetdiscoveryport=$((P0+25)) -Denable_wsapi=true -Dwsapi_port=$((WS0+1))"
launch b1 "-Dis_agent=true $RB -Dagentname=b1 -Dregional_controller_host=127.0.0.1 -Dregional_controller_port=$((P0+15)) -Dnetdiscoveryport=$((P0+35)) -Denable_wsapi=true -Dwsapi_port=$((WS0+2))"
waitport $((WS0+1)) && waitport $((WS0+2)) || { echo "FATAL: agent wsapi did not come up"; exit 2; }
for _ in $(seq 1 60); do grep -q "Starting Bridge" "$WORK/logs/region.log" 2>/dev/null && break; sleep 2; done
sleep 15   # registrations + first full exports settle

echo "### dataplane probe (plain stream, recipient-bound stream, spy)"
PROBE=$("$PY" "$HERE/peer_identity_dataplane.py" --tx-port "$WS0" --rx-port $((WS0+2)) --spy-port $((WS0+1)) --rx-path rgr_b1 --key "$KEY" --messages 200)
echo "$PROBE"
sleep 2

P=0; F=0
ck() { if [ "$1" = "1" ]; then P=$((P+1)); echo "PASS  $2"; else F=$((F+1)); echo "FAIL  $2"; fi; }
has() { grep -qE "$2" "$WORK/logs/$1.log" 2>/dev/null && echo 1; }
none() { grep -qE "$2" "$WORK/logs/$1.log" 2>/dev/null || echo 1; }
L="$WORK/logs"
ck "$(has a1 'Peer identity verified: broker connection .* is region=glob, agent=gc')" "a1's broker connections are bound to the global glob/gc"
ck "$(has b1 'Peer identity verified: broker connection .* is region=rgr, agent=rc')" "b1's broker connections are bound to the region rgr/rc"
ck "$(has region 'Peer identity verified: bridge .* is region=glob, agent=gc')" "the region's bridge is bound to the global glob/gc"
ck "$(has global 'Broker bridge from verified controller CrescoIdentity\{tenant=t1, region=rgr, agent=rc')" "the global admitted the bridge from the discovered region controller"
ck "$(has global 'mTLS identity bound from certificate: CrescoIdentity\{tenant=t1, region=glob, agent=a1')" "a1 is identified at the global by a certificate naming its joined region"
ck "$(has region 'mTLS identity bound from certificate: CrescoIdentity\{tenant=t1, region=rgr, agent=b1')" "b1 is identified at the region by a certificate naming its joined region"
for n in global region a1 b1; do
  ck "$(none $n 'PEER IDENTITY MISMATCH|BRIDGE REFUSED')" "$n: no identity refusal in a normal fabric"
done
ck "$(echo "$PROBE" | grep -q '"plain_received": 200' && echo 1)" "an ordinary stream crossed global -> bridge -> region -> b1 in full (200/200)"
ck "$(echo "$PROBE" | grep -q '"bound_received": 200' && echo 1)" "the recipient-bound stream reached its recipient rgr_b1 in full (200/200)"
ck "$(echo "$PROBE" | grep -q '"bound_spy_received": 0' && echo 1)" "the spy agent glob/a1 received none of rgr_b1's stream"
ck "$(has global "DENY READ 'global.event.*a1.*stream of recipient rgr_b1")" "the global broker refused a1's subscription to rgr_b1's stream"
echo "--- other DENY lines (tenant policy), per node:"
for n in global region a1 b1; do echo "  $n: $(grep -E 'DENY ' "$L/$n.log" 2>/dev/null | grep -vc 'stream of recipient')"; done
echo "==================================================="; echo "  PASS=$P  FAIL=$F  (logs: $L)"
exit $F
