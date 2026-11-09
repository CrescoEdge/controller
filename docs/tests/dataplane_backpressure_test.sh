#!/usr/bin/env bash
# Dataplane backpressure, live: a global, a region bridged to it, an agent behind each (a1 behind the region, a2 on the
# global), and the Python client (agent repo scripts/bench/dataplane_perf.py) streaming through wsapi. Every case must
# deliver every frame, whole and in order (received == sent, no duplicate, none out of order, none damaged):
#   1. fast, one broker (a2 -> a2);  2. fast, across the bridge (a2 -> global -> region -> a1) and back (a1 -> a2);
#   3. a slow reader across the bridge each way, and on one broker: the writer must be SLOWED to it (it sends for at least
#      most of the time the reader needs), nothing discarded at any broker or wsapi.
# Before the fix the reader behind a node lost frames (1993/2000 locally, 1908/2000 on the DGX) and a slow one lost most.
#
# Usage: dataplane_backpressure_test.sh <agent.jar> <workdir> <python-with-pycrescolib> <dataplane_perf.py>
#   PORT0 (default 36400) moves the port block (PORT0..PORT0+60). JAVA (default java). Prints PASS/FAIL lines and a summary;
#   exit 0 only when every case passes. Starts and stops only its own JVMs (by PID).
set -uo pipefail
JAR="$1"; WORK="$2"; PY="$3"; PERF="$4"
J="${JAVA:-java}"; P0="${PORT0:-36400}"
# 32 MiB per topic (default 256 MiB): what a broker holds for a slow reader before its writers wait, kept small so the
# writer's wait is plain to see (each broker hop holds up to this much)
TOPIC_MEM="${TOPIC_MEM:-33554432}"
rm -rf "$WORK"; mkdir -p "$WORK/secrets"; chmod 700 "$WORK/secrets"
for s in sk dg dr da; do openssl rand -hex 32 > "$WORK/secrets/$s"; done; chmod 600 "$WORK/secrets/"*
PIDS="$WORK/pids"; : > "$PIDS"
stopall() { while read -r p; do kill "$p" 2>/dev/null; done < "$PIDS"; sleep 4; while read -r p; do kill -9 "$p" 2>/dev/null; done < "$PIDS"; }
trap stopall EXIT
waitlog() { for _ in $(seq 1 90); do grep -qE "$2" "$1" 2>/dev/null && return 0; sleep 2; done; return 1; }
fp() { head -1 "$(find "$WORK/$1/cresco-data" -name ws.fingerprint | head -1)"; }
node() { # name region flags
  mkdir -p "$WORK/$1"
  ( cd "$WORK/$1" && export CRESCO_cresco_service_key="$(cat "$WORK/secrets/sk")" CRESCO_discovery_secret_global="$(cat "$WORK/secrets/dg")" \
      CRESCO_discovery_secret_region="$(cat "$WORK/secrets/dr")" CRESCO_discovery_secret_agent="$(cat "$WORK/secrets/da")" && \
    exec "$J" -Djava.net.preferIPv4Stack=true -Xmx2g -Denable_console=false -Denable_dashboard=false -Dtenant_id=default \
      -Dactivemq_topic_memory_limit=$TOPIC_MEM \
      -Dregionname="$2" -Dagentname="$1" $3 -jar "$JAR" ) > "$WORK/$1/agent.out" 2>&1 < /dev/null &
  echo $! >> "$PIDS"
}
node g bpg "-Dis_global=true -Dnetdiscoveryport=$((P0+5)) -Dbroker_port=$((P0+10)) -Ddiscovery_port=$((P0+10)) -Ddiscovery_port_remote=$((P0+10)) -Dport=$((P0+40)) -Denable_wsapi=false"
waitlog "$WORK/g/agent.out" "Broker transport|REFUSING" || { echo "FAIL global did not start"; exit 2; }
sleep 5
node r bpr "-Dis_region=true -Dglobal_controller_host=127.0.0.1 -Dglobal_controller_port=$((P0+5)) -Dnetdiscoveryport=$((P0+15)) -Dbroker_port=$((P0+20)) -Ddiscovery_port=$((P0+20)) -Ddiscovery_port_remote=$((P0+10)) -Dport=$((P0+41)) -Denable_wsapi=false"
waitlog "$WORK/r/agent.out" "joined global|registered with Global|REFUSING" || { echo "FAIL region not registered"; exit 2; }
node a1 bpr "-Dis_agent=true -Dregional_controller_host=127.0.0.1 -Dregional_controller_port=$((P0+15)) -Dnetdiscoveryport=$((P0+25)) -Dbroker_port=$((P0+26)) -Dport=$((P0+42)) -Denable_wsapi=true -Dwsapi_port=$((P0+50))"
node a2 bpg "-Dis_agent=true -Dregional_controller_host=127.0.0.1 -Dregional_controller_port=$((P0+5)) -Dnetdiscoveryport=$((P0+30)) -Dbroker_port=$((P0+31)) -Dport=$((P0+43)) -Denable_wsapi=true -Dwsapi_port=$((P0+51))"
for a in a1 a2; do waitlog "$WORK/$a/agent.out" "registered with Region|REFUSING" || { echo "FAIL $a not registered"; exit 2; }; done
for a in a1 a2; do waitlog "$WORK/$a/agent.out" "wsapi TLS provider|REFUSING" || { echo "FAIL $a: no wsapi"; exit 2; }; done
sleep 15
KEY="$(cat "$WORK/secrets/sk")"
fails=0; n=0
run() { # name tx-agent tx-port rx-agent rx-port messages rx-sleep-ms
  local out res
  out=$("$PY" "$PERF" --host 127.0.0.1 --port "$3" --fingerprint "$(fp "$2")" --rx-port "$5" --rx-fingerprint "$(fp "$4")" --key "$KEY" \
        --messages "$6" --size 262144 --settle 300 --rx-sleep-ms "$7" 2>&1 | grep '^BENCH' | sed 's/^BENCH //')
  n=$((n + 1))
  res=$("$PY" - "$out" "$6" "$7" <<'EOF'
import json, sys
try:
    d = json.loads(sys.argv[1])
except ValueError:
    print("FAIL no result"); sys.exit()
n, nap = int(sys.argv[2]), float(sys.argv[3])
bad = []
if d["received"] != n: bad.append(f"received {d['received']}/{n}")
for k in ("missing", "dups", "out_of_order", "bad_size"):
    if d.get(k): bad.append(f"{k} {d[k]}")
if nap > 0:
    need = n * nap / 1000.0                  # what the reader takes at least
    if d["send_s"] < 0.5 * need: bad.append(f"the writer was not slowed: sent in {d['send_s']} s, the reader needs {need:.1f} s")
print(("FAIL " + "; ".join(bad)) if bad else "PASS", json.dumps({k: d[k] for k in ("received", "send_s", "recv_mib_s", "p50_ms", "p99_ms")}))
EOF
)
  echo "$res" | grep -q '^PASS' || fails=$((fails + 1))
  echo "${res%% *} $1: ${res#* }"
}
A1=$((P0+50)); A2=$((P0+51))
run "fast, one broker (a2 -> a2)"                        a2 $A2 a2 $A2 2000 0
run "fast, across the bridge (a2 -> global -> region -> a1)" a2 $A2 a1 $A1 2000 0
run "fast, across the bridge (a1 -> region -> global -> a2)" a1 $A1 a2 $A2 2000 0
run "slow reader, across the bridge (a2 -> a1)"          a2 $A2 a1 $A1 3000 5
run "slow reader, across the bridge (a1 -> a2)"          a1 $A1 a2 $A2 3000 5
run "slow reader, one broker (a1 -> a1)"                 a1 $A1 a1 $A1 3000 5
lost=$(cat "$WORK"/*/agent.out | grep -cE "broker DISCARDED|WS consumer overloaded|dropped [0-9]+ frame" || true)
n=$((n + 1)); if [ "$lost" = 0 ]; then echo "PASS no broker discard, no wsapi drop"; else echo "FAIL $lost discard/drop log line(s)"; fails=$((fails + 1)); fi
echo "dataplane backpressure: $((n - fails))/$n passed"
[ "$fails" = 0 ]
