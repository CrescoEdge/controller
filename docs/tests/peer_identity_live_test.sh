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
#   PYCRESCOLIB_PATH, when set, is put first on the probe's PYTHONPATH (to test a pycrescolib checkout).
#   PORT0 (default 33000) and WS0 (default 8482) move the port block. Set REGIONAL_CA=1 to run under the regional CA.
#   RESTART_DRILL=1 (default) then restarts the global and then the region (same data directories) and checks that agents
#   and bridges reconnect under client certificates with no TLS refusal and without rediscovering first, and that the
#   dataplane works again. RESTART_DRILL=0 skips it.
# Starts only its own JVMs and stops only those (by PID). Secrets are generated per run.
set -uo pipefail
JAR="$1"; WORK="$2"; PY="$3"; shift 3; EXTRA="$*"
HERE="$(cd "$(dirname "$0")" && pwd)"
P0="${PORT0:-33000}"; WS0="${WS0:-8482}"
rm -rf "$WORK"; mkdir -p "$WORK/logs"
rnd() { "$PY" -c 'import secrets; print(secrets.token_hex(16))'; }
KEY="$(rnd)"
SEC="-Ddiscovery_secret_global=$(rnd) -Ddiscovery_secret_region=$(rnd) -Ddiscovery_secret_agent=$(rnd) -Dcresco_service_key=$KEY"
# broker_require_client_auth is left to its default (on, because broker security is on)
SECURITY="-Dbroker_security_enabled=true -Dtenant_id=t1 -Dpeer_identity_mode=enforce"
[ "${REGIONAL_CA:-0}" = "1" ] && SECURITY="$SECURITY -Dsecurity_regional_ca=true"
COMMON="-Djava.net.preferIPv4Stack=true -Xmx1024M -Droot_log_level=INFO -Denable_console=false -Denable_dashboard=false"
PIDS="$WORK/pids"; : > "$PIDS"

launch() { # name flags [append]
  local d="$WORK/nodes/$1"; mkdir -p "$d"
  echo "$2" > "$WORK/flags.$1"
  if [ "${3:-}" = "append" ]; then
    ( cd "$d" && exec java $COMMON $SECURITY $SEC $2 $EXTRA -jar "$JAR" ) >> "$WORK/logs/$1.log" 2>&1 < /dev/null &
  else
    ( cd "$d" && exec java $COMMON $SECURITY $SEC $2 $EXTRA -jar "$JAR" ) > "$WORK/logs/$1.log" 2>&1 < /dev/null &
  fi
  echo $! >> "$PIDS"; echo $! > "$WORK/pid.$1"
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

probe() {
  PYTHONPATH="${PYCRESCOLIB_PATH:-}${PYTHONPATH:+:$PYTHONPATH}" "$PY" "$HERE/peer_identity_dataplane.py" --tx-port "$WS0" --rx-port $((WS0+2)) \
    --spy-port $((WS0+1)) --spy2-port "$WS0" --rx-path rgr_b1 --key "$KEY" --messages 200
}
echo "### dataplane probe (plain stream, recipient-bound stream, spy)"
PROBE=$(probe)
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
ck "$(echo "$PROBE" | grep -q '"bound_spy2_received": 0' && echo 1)" "a client of the global's own wsapi received none of rgr_b1's stream"
ck "$(has global "DENY READ 'global.event.*glob, agent=gc.*stream of recipient rgr_b1")" "the global broker refused its own controller's (wsapi client's) subscription to rgr_b1's stream"
typed() { echo "$PROBE" | "$PY" -c 'import json,sys; d=json.loads(sys.stdin.read()); v=d.get(sys.argv[1]) or ""; sys.exit(0 if "Subscription refused" in v else 1)' "$1" && echo 1; }
ck "$(typed bound_spy_refused)" "the spy's client got a typed refusal (DataplaneSubscriptionRefused)"
ck "$(typed bound_spy2_refused)" "the global's wsapi client got a typed refusal (DataplaneSubscriptionRefused)"
for n in global region; do ck "$(has $n 'Broker mutual-TLS ENABLED')" "$n: client certificates required by default with broker security"; done
ck "$(has global 'discovery v2: authenticated GLOBAL exchange with rgr/rc')" "the region and the global authenticated each other with discovery v2"
ck "$(has b1 'discovery v2: authenticated AGENT exchange with rgr/rc')" "b1 and the region authenticated each other with discovery v2"
ck "$(has global "DENY READ 'global.event.*a1.*stream of recipient rgr_b1")" "the global broker refused a1's subscription to rgr_b1's stream"
if [ "${RESTART_DRILL:-1}" = "1" ]; then
  # lines after a mark (taken just before a restart) only
  markall() { for n in global region a1 b1; do wc -l < "$L/$n.log" | tr -d ' ' > "$WORK/mark.$n"; done; }
  newlines() { tail -n +$(( $(cat "$WORK/mark.$1") + 1 )) "$L/$1.log" 2>/dev/null; }
  after() { newlines "$1" | grep -qE "$2" && echo 1; }
  nofail() { for n in global region a1 b1; do newlines "$n" | grep -qE 'PKIX path|unable to find valid certification path|certificate_unknown|bad_certificate|BRIDGE REFUSED|PEER IDENTITY MISMATCH' && return; done; echo 1; }
  waitlog() { # seconds node regex -> seconds it took (or "timeout")
    local s=0; while [ $s -lt "$1" ]; do after "$2" "$3" >/dev/null && { echo $s; return; }; sleep 1; s=$((s+1)); done; echo timeout; }
  restart() { # name: stop it (TERM, then KILL) and start it again on the same data directory
    local p; p=$(cat "$WORK/pid.$1"); kill "$p" 2>/dev/null
    for _ in $(seq 1 30); do kill -0 "$p" 2>/dev/null || break; sleep 1; done; kill -9 "$p" 2>/dev/null; sleep 1
    markall; launch "$1" "$(cat "$WORK/flags.$1")" append; }
  # time (HH:MM:SS,mmm) of the first new line of node $1 matching $2
  firstts() { newlines "$1" | grep -E "$2" | head -1 | awk '{print $4}'; }
  # the SERVER ($1) admitted the client by certificate ($2) before the client ($3) ran any new discovery exchange:
  # the connection came back by failover, not because the client rediscovered (a client-side "verified" line alone
  # only means the client accepted the server)
  noredisc() { local a d; a=$(firstts "$1" "$2"); d=$(firstts "$3" 'discovery v2: authenticated'); [ -n "$a" ] && { [ -z "$d" ] || [[ "$a" < "$d" ]]; } && echo 1; }

  echo "### restart drill 1: the global restarts (same data directory)"
  restart global
  waitport $((P0+10)) && waitport "$WS0" || echo "global did not come back"
  A1='mTLS identity bound from certificate: CrescoIdentity\{tenant=t1, region=glob, agent=a1'
  RC='Broker bridge from verified controller CrescoIdentity\{tenant=t1, region=rgr, agent=rc'
  T1=$(waitlog 180 global "$A1")
  T2=$(waitlog 180 global "$RC")
  echo "  the restarted global admitted a1 after ${T1}s and the region's bridge after ${T2}s of its ports being up"
  ck "$( [ "$T1" != timeout ] && echo 1)" "global restart: the restarted global admitted a1 by its certificate (identity verified)"
  ck "$( [ "$T2" != timeout ] && echo 1)" "global restart: the region's bridge is back and the global admitted it as a verified controller"
  ck "$(nofail)" "global restart: no TLS trust failure or identity refusal on any node"
  ck "$(noredisc global "$A1" a1)" "global restart: a1 came back by failover, without rediscovering first"
  ck "$(noredisc global "$RC" region)" "global restart: the region's bridge came back without rediscovering first"
  sleep 15
  PROBE=$(probe); echo "$PROBE"
  ck "$(echo "$PROBE" | grep -q '"plain_received": 200' && echo "$PROBE" | grep -q '"bound_received": 200' && echo "$PROBE" | grep -q '"bound_spy_received": 0' && echo 1)" "global restart: the dataplane works again (200/200 plain and recipient-bound, spy 0)"

  echo "### restart drill 2: the region restarts (same data directory)"
  restart region
  waitport $((P0+20)) || echo "region did not come back"
  B1='mTLS identity bound from certificate: CrescoIdentity\{tenant=t1, region=rgr, agent=b1'
  T4=$(waitlog 180 region "$B1")
  T6=$(waitlog 180 global "$RC")
  echo "  the restarted region admitted b1 after ${T4}s; the global admitted the region's bridge after ${T6}s"
  ck "$( [ "$T4" != timeout ] && echo 1)" "region restart: the restarted region admitted b1 by its certificate (identity verified)"
  ck "$( [ "$T6" != timeout ] && echo 1)" "region restart: the region's bridge to the global is back and admitted"
  ck "$(nofail)" "region restart: no TLS trust failure or identity refusal on any node"
  ck "$(noredisc region "$B1" b1)" "region restart: b1 came back by failover, without rediscovering first"
  sleep 15
  PROBE=$(probe); echo "$PROBE"
  ck "$(echo "$PROBE" | grep -q '"plain_received": 200' && echo "$PROBE" | grep -q '"bound_received": 200' && echo "$PROBE" | grep -q '"bound_spy_received": 0' && echo 1)" "region restart: the dataplane works again (200/200 plain and recipient-bound, spy 0)"
fi

echo "--- other DENY lines (tenant policy), per node:"
for n in global region a1 b1; do echo "  $n: $(grep -E 'DENY ' "$L/$n.log" 2>/dev/null | grep -vc 'stream of recipient')"; done
echo "==================================================="; echo "  PASS=$P  FAIL=$F  (logs: $L)"
exit $F
