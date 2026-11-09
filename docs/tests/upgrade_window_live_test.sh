#!/usr/bin/env bash
# Live proof of the upgrade guide (docs/upgrade-1.3-secure-defaults.md sections 3-4) on one host: a fabric of agents of an
# EARLIER 1.3 build (no security setting at all, as they ran) keeps its membership while its controllers are upgraded with
# the upgrade window open, its agents are upgraded one by one, and the window closes by its date at run time.
#
#   global glob/gc (upgraded, wsapi :WS0)   agent glob/a1 (earlier build, then upgraded WITH the window settings)
#   region rgr/rc  (upgraded, bridged)      agents rgr/b1 (earlier build, then upgraded WITHOUT them), rgr/c1 (never upgraded)
#                                           agent  rgr/d1 (earlier build, started after the window closed)
#
# Usage: upgrade_window_live_test.sh <upgraded agent.jar> <earlier agent.jar> <workdir> <python-with-pycrescolib>
#   The earlier build is, e.g., agent commit 5f28a0a (controller 0bdd3b5): `mvn -q package -DskipTests` in a worktree.
#   PORT0 (default 33000) moves the port block (PORT0..PORT0+60, wsapi PORT0+480). WINDOW_S (default 330): the window
#   ends that many seconds after the controllers start (discovery_compat_until as an ISO instant).
#   PYCRESCOLIB_PATH, when set, is put first on PYTHONPATH.
# Starts only its own JVMs and stops only those (by PID). Secrets are generated per run.
# (No grep -q at the end of a pipeline: with pipefail, the writer's SIGPIPE would fail the check.)
set -uo pipefail
NEW="$1"; OLD="$2"; WORK="$3"; PY="$4"
HERE="$(cd "$(dirname "$0")" && pwd)"
P0="${PORT0:-33000}"; WS0=$((P0+480)); WINDOW_S="${WINDOW_S:-330}"
export PYTHONPATH="${PYCRESCOLIB_PATH:-}${PYTHONPATH:+:$PYTHONPATH}"
rm -rf "$WORK"; mkdir -p "$WORK/logs"
rnd() { "$PY" -c 'import secrets; print(secrets.token_hex(16))'; }
KEY="$(rnd)"
SEC="-Ddiscovery_secret_global=$(rnd) -Ddiscovery_secret_region=$(rnd) -Ddiscovery_secret_agent=$(rnd) -Dcresco_service_key=$KEY"
COMMON="-Djava.net.preferIPv4Stack=true -Xmx768M -Droot_log_level=INFO -Denable_console=false -Denable_dashboard=false"
UNTIL=$("$PY" -c 'import datetime,sys; print((datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(seconds=int(sys.argv[1]))).strftime("%Y-%m-%dT%H:%M:%SZ"))' "$WINDOW_S")
UNTIL_EPOCH=$(( $(date +%s) + WINDOW_S ))
WIN="-Ddiscovery_auth=compat -Ddiscovery_compat_until=$UNTIL"
PIDS="$WORK/pids"; : > "$PIDS"

launch() { # name jar flags
  local d="$WORK/nodes/$1"; mkdir -p "$d"
  echo "=== $(date +%T) $(basename "$(dirname "$(dirname "$2")")") $3" >> "$WORK/logs/$1.log"
  ( cd "$d" && exec java $COMMON $SEC $3 -jar "$2" ) >> "$WORK/logs/$1.log" 2>&1 < /dev/null &
  echo $! >> "$PIDS"; echo $! > "$WORK/pid.$1"
}
stopnode() { local p; p=$(cat "$WORK/pid.$1" 2>/dev/null); [ -n "$p" ] || return 0; kill "$p" 2>/dev/null
  for _ in $(seq 1 15); do kill -0 "$p" 2>/dev/null || break; sleep 1; done; kill -9 "$p" 2>/dev/null; rm -f "$WORK/pid.$1"; }
stopall() { while read -r p; do kill "$p" 2>/dev/null; done < "$PIDS"; sleep 3; while read -r p; do kill -9 "$p" 2>/dev/null; done < "$PIDS"; }
trap stopall EXIT
waitport() { for _ in $(seq 1 90); do nc -z 127.0.0.1 "$1" 2>/dev/null && return 0; sleep 2; done; return 1; }
mark() { wc -l < "$WORK/logs/$1.log" | tr -d ' '; }
# waitlog node regex timeout-s [from-line]: the regex appears after line from-line
waitlog() { local end=$(( $(date +%s) + $3 )); while [ "$(date +%s)" -lt "$end" ]; do
  tail -n +"$(( ${4:-0} + 1 ))" "$WORK/logs/$1.log" 2>/dev/null | grep -E "$2" > /dev/null && return 0; sleep 2; done; return 1; }
after() { tail -n +"$(( $2 + 1 ))" "$WORK/logs/$1.log" 2>/dev/null; }

P=0; F=0
ck() { if [ "$1" = "1" ]; then P=$((P+1)); echo "PASS  $2"; else F=$((F+1)); echo "FAIL  $2"; fi; }
yes() { "$@" && echo 1; }
pin() { local f; f=$(find "$WORK/nodes/global/cresco-data/plugin-data" -name ws.fingerprint 2>/dev/null | head -1); [ -n "$f" ] && head -1 "$f" | tr -d '[:space:]'; }
agents() { "$PY" "$HERE/fleet_agents.py" --port "$WS0" --key "$KEY" --pin "$(pin)"; }
# lists region/agent... : the global's registry lists every one of them (retried while registrations settle)
# answers region/agent: its controller answers one RPC from a client of the global (retried while it settles)
answers() { for _ in $(seq 1 10); do agents_ping "$1" | grep -F "\"$1\": true" > /dev/null && { echo 1; return; }; sleep 3; done; }
agents_ping() { "$PY" "$HERE/fleet_agents.py" --port "$WS0" --key "$KEY" --pin "$(pin)" --ping "$1"; }
lists() { local out; for _ in $(seq 1 20); do out=$(agents); local ok=1
  for n in "$@"; do echo "$out" | grep -F "\"$n\"" > /dev/null || ok=0; done; [ $ok = 1 ] && { echo "$out" >> "$WORK/agents.json"; echo 1; return; }; sleep 3; done
  echo "$out" >> "$WORK/agents.json"; }

GLOBAL="-Dis_global=true -Dregionname=glob -Dagentname=gc -Dnetdiscoveryport=$((P0+5)) -Dbroker_port=$((P0+10)) -Ddiscovery_port=$((P0+10)) -Denable_wsapi=true -Dwsapi_port=$WS0"
REGION="-Dis_region=true -Dregionname=rgr -Dagentname=rc -Dglobal_controller_host=127.0.0.1 -Dglobal_controller_port=$((P0+5)) -Dnetdiscoveryport=$((P0+15)) -Dbroker_port=$((P0+20)) -Ddiscovery_port_remote=$((P0+10)) -Denable_wsapi=false"
agent() { # name region discovery-port-offset
  local cp=$((P0+5)); [ "$2" = rgr ] && cp=$((P0+15))
  echo "-Dis_agent=true -Dregionname=$2 -Dagentname=$1 -Dregional_controller_host=127.0.0.1 -Dregional_controller_port=$cp -Dnetdiscoveryport=$((P0+$3)) -Denable_wsapi=false"
}

echo "### upgraded global glob/gc and region rgr/rc, window open until $UNTIL"
launch global "$NEW" "$GLOBAL $WIN"
waitport "$WS0" || { echo "FATAL: global wsapi did not come up"; exit 2; }
launch region "$NEW" "$REGION $WIN"
waitport $((P0+20)) && waitport $((P0+15)) || { echo "FATAL: region broker/discovery did not come up"; exit 2; }
waitlog global "Broker bridge from verified controller" 120
echo "### earlier-build agents glob/a1, rgr/b1, rgr/c1 (no security setting)"
launch a1 "$OLD" "$(agent a1 glob 25)"
launch b1 "$OLD" "$(agent b1 rgr 35)"
launch c1 "$OLD" "$(agent c1 rgr 45)"
for n in a1 b1 c1; do waitlog $n "registered with Region" 150; done

ck "$(yes grep -q 'registered with Region: glob' "$WORK/logs/a1.log")" "window open: earlier-build a1 registered with the upgraded global's region"
ck "$(yes grep -q 'registered with Region: rgr' "$WORK/logs/b1.log")" "window open: earlier-build b1 registered with the upgraded region"
ck "$(yes grep -q 'registered with Region: rgr' "$WORK/logs/c1.log")" "window open: earlier-build c1 registered with the upgraded region"
for n in global region; do
  ck "$(yes grep -q 'UPGRADE WINDOW OPEN until' "$WORK/logs/$n.log")" "$n logs UPGRADE WINDOW OPEN at boot"
  ck "$(yes grep -q 'accepting the LEGACY unauthenticated certificate exchange' "$WORK/logs/$n.log")" "$n accepted the legacy exchange of an earlier-build agent"
  ck "$(yes grep -q 'also consumes the raw inbox' "$WORK/logs/$n.log")" "$n also consumes its raw inbox (earlier builds run without tenant namespacing)"
done
ck "$(lists glob/a1 rgr/b1 rgr/c1)" "the global's registry lists glob/a1, rgr/b1 and rgr/c1 (earlier builds, through the region)"
ck "$(waitlog region 'mesh: child rgr_c1 health OK' 60 && echo 1)" "the region health-pings earlier-build c1 (replies reach its raw inbox)"

echo "### upgrade a1 (with the window settings) and b1 (without them), one at a time"
m=$(mark global); ma=$(mark a1); stopnode a1; launch a1 "$NEW" "$(agent a1 glob 25) $WIN"
ck "$(waitlog a1 'registered with Region: glob' 150 "$ma" && echo 1)" "upgraded a1 registered"
ck "$(waitlog global 'glob/a1 UPGRADED' 5 "$m" && echo 1)" "the global replaced a1's legacy binding by the key it proved over the authenticated exchange"
ck "$(after a1 "$ma" | grep -E 'discovery v2: authenticated AGENT exchange with glob/gc' > /dev/null && echo 1)" "upgraded a1 joined over the authenticated exchange (v2)"
m=$(mark region); mb=$(mark b1); stopnode b1; launch b1 "$NEW" "$(agent b1 rgr 35)"
ck "$(waitlog b1 'registered with Region: rgr' 150 "$mb" && echo 1)" "upgraded b1 registered"
ck "$(waitlog region 'rgr/b1 UPGRADED' 5 "$m" && echo 1)" "the region replaced b1's legacy binding (b1 upgraded without the window settings)"
ck "$(after b1 "$mb" | grep -E 'discovery v2: authenticated AGENT exchange with rgr/rc' > /dev/null && echo 1)" "upgraded b1 joined over the authenticated exchange (v2)"
ck "$(waitlog b1 'message signatures ON: this node signs' 60 "$mb" && echo 1)" "upgraded b1 signs what it originates"
ck "$(lists glob/a1 rgr/b1 rgr/c1)" "mid-rollout: the global's registry lists upgraded a1, b1 and earlier-build c1"
for n in global region; do
  ck "$(after $n 0 | grep -E 'MESSAGE SIGNATURE: REFUSED' > /dev/null || echo 1)" "$n refused no message while the window was open"
done

echo "### the window closes by its date ($UNTIL), no restart"
wait_s=$(( UNTIL_EPOCH - $(date +%s) + 5 )); [ "$wait_s" -gt 0 ] && sleep "$wait_s"
mr=$(mark region); mg=$(mark global)
launch d1 "$OLD" "$(agent d1 rgr 55)"
ck "$(waitlog region 'MESSAGE SIGNATURE: REFUSED an unsigned message \(from agent:[^)]*/rgr/c1' 90 "$mr" && echo 1)" "window closed: earlier-build c1's messages are refused (unsigned)"
ck "$(waitlog region 'REFUSED a (legacy \(replayable|node of an earlier build)' 90 "$mr" && echo 1)" "window closed: earlier-build d1 is refused at discovery (logged)"
sleep 30
ck "$(grep -q 'registered with Region' "$WORK/logs/d1.log" || echo 1)" "window closed: earlier-build d1 never registered"
ck "$(answers rgr/b1)" "window closed: upgraded b1 answers an RPC (through the global and the region)"
ck "$(lists glob/a1 rgr/b1)" "window closed: the global's registry still lists upgraded a1 and b1"
ck "$(after global "$mg" | grep -E 'MESSAGE SIGNATURE: REFUSED.*(glob/a1|rgr/b1)' > /dev/null || echo 1)" "window closed: nothing of upgraded a1 or b1 is refused"
stopnode d1; stopnode c1

echo "### remove the window: the global restarts with the expired settings still set, the region without them"
stopnode global; launch global "$NEW" "$GLOBAL $WIN"
waitport "$WS0" || { echo "FATAL: restarted global wsapi did not come up"; exit 2; }
mr=$(mark region); stopnode region; launch region "$NEW" "$REGION"
waitport $((P0+20))
ck "$(waitlog global 'the upgrade window has CLOSED' 5 "$(grep -n '^=== ' "$WORK/logs/global.log" | tail -1 | cut -d: -f1)" && echo 1)" "a restarted node with the expired settings logs that the window has CLOSED"
ck "$(after region "$mr" | grep -E 'UPGRADE WINDOW|also consumes the raw inbox' > /dev/null || echo 1)" "the region restarted without the settings: no window, no raw inbox"
waitlog region "Broker bridge|Peer identity verified: bridge" 120 "$mr"
sleep 45
ck "$(lists glob/a1 rgr/b1)" "after both controllers restarted: the global's registry lists upgraded a1 and b1"
ck "$(answers rgr/b1)" "after the region restarted: upgraded b1 answers an RPC"
ck "$(answers glob/a1)" "after the global restarted: upgraded a1 answers an RPC"
for n in global region a1 b1; do
  ck "$(grep -qE 'PEER IDENTITY MISMATCH|BRIDGE REFUSED|REFUSING TO START' "$WORK/logs/$n.log" || echo 1)" "$n: no identity refusal and no refused start"
done

echo "registries seen: $(tail -1 "$WORK/agents.json" 2>/dev/null)"
echo "PASS=$P FAIL=$F"
[ "$F" = 0 ]
