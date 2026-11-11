#!/usr/bin/env bash
#
# Live test of "authority follows the tree" (docs/caller-authorization.md section 3a) on four real nodes, one host:
#
#   global  glob/gc  (wsapi :WS0)      agent glob/a1 (wsapi :WS0+1)   -- a1 joins the global's region
#   region  rgr/rc   (wsapi :WS0+3)    agent rgr/b1  (wsapi :WS0+2)   -- b1 joins the region
#
# The same admin connects to each node's gateway in turn and sends the management action getagentinfo (direction DOWN,
# read-only) to every node, and the open action getcontrollerstatus. A management action is honoured only when the
# gateway's node is the target itself or a controller above it:
#
#   from gc: gc rc a1 b1      from rc: rc b1      from a1: a1      from b1: b1
#
# Phase 1, authority_direction unset (log, the default): all 16 are allowed, and each target logs the calls enforce
#   would refuse, naming the gateway's node as the speaker and its position.
# Phase 2, every node restarted with authority_direction=enforce: exactly the 8 above are allowed, the other 8 are
#   answered 403 with the reason; the open action is allowed everywhere; the fabric stays whole (what a child reports
#   upward, WATCHDOG and registration, is not refused); nothing is logged as "would refuse".
#
# The four gateways share one service key here so that one admin can stand at each of them. A deployment must not do
# that: a controller's gateway credentials must not exist on its leaves (section 3a).
#
# Usage: authority_live_test.sh <agent.jar> <workdir> <python-with-pycrescolib>
#   PORT0 (default 35200) and WS0 (default 8582) move the port block. Needs a JDK 21 java on PATH.
# Starts only its own JVMs and stops only those (by PID). Secrets are generated per run.
set -uo pipefail
JAR="$1"; WORK="$2"; PY="$3"
HERE="$(cd "$(dirname "$0")" && pwd)"
P0="${PORT0:-35200}"; WS0="${WS0:-8582}"
rm -rf "$WORK"; mkdir -p "$WORK"
rnd() { "$PY" -c 'import secrets; print(secrets.token_hex(16))'; }
KEY="$(rnd)"
SEC="-Ddiscovery_secret_global=$(rnd) -Ddiscovery_secret_region=$(rnd) -Ddiscovery_secret_agent=$(rnd) -Dcresco_service_key=$KEY"
COMMON="-Djava.net.preferIPv4Stack=true -Xmx768M -Droot_log_level=INFO -Denable_console=false -Denable_dashboard=false"
PIDS="$WORK/pids"; : > "$PIDS"
PASS=0; FAIL=0
ck() { if [ "${1:-}" = "1" ]; then PASS=$((PASS+1)); echo "PASS  $2"; else FAIL=$((FAIL+1)); echo "FAIL  $2"; fi; }

launch() { # phase name flags
  local d="$WORK/$1/nodes/$2"; mkdir -p "$d" "$WORK/$1/logs"
  ( cd "$d" && exec java $COMMON $SEC $3 $MODE -jar "$JAR" ) > "$WORK/$1/logs/$2.log" 2>&1 < /dev/null &
  echo $! >> "$PIDS"
}
stopall() { while read -r p; do kill "$p" 2>/dev/null; done < "$PIDS"; sleep 3; while read -r p; do kill -9 "$p" 2>/dev/null; done < "$PIDS"; : > "$PIDS"; }
trap stopall EXIT
waitport() { for _ in $(seq 1 90); do nc -z 127.0.0.1 "$1" 2>/dev/null && return 0; sleep 2; done; return 1; }
pin() { # the wsapi certificate fingerprint of node $2 of phase $1
  local f; f=$(find "$WORK/$1/nodes/$2/cresco-data/plugin-data" -name ws.fingerprint 2>/dev/null | head -1)
  [ -n "$f" ] && head -1 "$f" | tr -d '[:space:]'
}

fabric() { # phase: start the four nodes and wait until they are joined
  local ph="$1"
  launch "$ph" global "-Dis_global=true -Dregionname=glob -Dagentname=gc -Dnetdiscoveryport=$((P0+5)) -Dbroker_port=$((P0+10)) -Ddiscovery_port=$((P0+10)) -Denable_wsapi=true -Dwsapi_port=$WS0"
  waitport "$WS0" || { echo "FATAL: global wsapi did not come up"; exit 2; }
  launch "$ph" region "-Dis_region=true -Dregionname=rgr -Dagentname=rc -Dglobal_controller_host=127.0.0.1 -Dglobal_controller_port=$((P0+5)) -Dnetdiscoveryport=$((P0+15)) -Dbroker_port=$((P0+20)) -Ddiscovery_port_remote=$((P0+10)) -Denable_wsapi=true -Dwsapi_port=$((WS0+3))"
  waitport $((P0+20)) && waitport $((P0+15)) && waitport $((WS0+3)) || { echo "FATAL: the region did not come up"; exit 2; }
  launch "$ph" a1 "-Dis_agent=true -Dagentname=a1 -Dregional_controller_host=127.0.0.1 -Dregional_controller_port=$((P0+5)) -Dnetdiscoveryport=$((P0+25)) -Denable_wsapi=true -Dwsapi_port=$((WS0+1))"
  launch "$ph" b1 "-Dis_agent=true -Dagentname=b1 -Dregional_controller_host=127.0.0.1 -Dregional_controller_port=$((P0+15)) -Dnetdiscoveryport=$((P0+35)) -Denable_wsapi=true -Dwsapi_port=$((WS0+2))"
  waitport $((WS0+1)) && waitport $((WS0+2)) || { echo "FATAL: agent wsapi did not come up"; exit 2; }
  for _ in $(seq 1 60); do grep -q "Starting Bridge" "$WORK/$ph/logs/region.log" 2>/dev/null && break; sleep 2; done
  sleep 20   # registrations, the first trust bundles and the region's certificate at the global settle
}
probe() { # phase -> the probe's JSON
  "$PY" "$HERE/authority_live_probe.py" --key "$KEY" \
    --node "gc=glob/gc@$WS0#$(pin "$1" global)" --node "rc=rgr/rc@$((WS0+3))#$(pin "$1" region)" \
    --node "a1=glob/a1@$((WS0+1))#$(pin "$1" a1)" --node "b1=rgr/b1@$((WS0+2))#$(pin "$1" b1)" 2> "$WORK/$1/probe.err"
}
# judge <phase> <json> <"allowed pairs"> : every pair is ok or 403 as the list says; the open action is ok everywhere
judge() {
  "$PY" - "$2" "$3" <<'PYEOF'
import json, sys
got, allowed = json.loads(sys.argv[1]), set(sys.argv[2].split())
names = ["gc", "rc", "a1", "b1"]
bad = []
for g in names:
    for t in names:
        k = f"{g}->{t}"; v = got.get(k, "missing")
        if k in allowed:
            if v != "ok": bad.append(f"{k} should be honoured: {v}")
        else:
            if not (v.startswith("403: ") and "honoured only from the node itself or a controller above it" in v):
                bad.append(f"{k} should be refused 403 with the reason: {v}")
        o = got.get("open:" + k, "missing")
        if o != "ok": bad.append(f"open action {k}: {o}")
print("1" if not bad else "0")
for b in bad: print("   " + b, file=sys.stderr)
PYEOF
}
has() { grep -q -E "$2" "$WORK/$1/logs/$3.log" 2>/dev/null && echo 1; }
ALL="gc->gc gc->rc gc->a1 gc->b1 rc->gc rc->rc rc->a1 rc->b1 a1->gc a1->rc a1->a1 a1->b1 b1->gc b1->rc b1->a1 b1->b1"
TREE="gc->gc gc->rc gc->a1 gc->b1 rc->rc rc->b1 a1->a1 b1->b1"

echo "### phase 1: authority_direction unset (log)"
MODE=""; fabric log
ck "$(has log 'authority_direction=log \(this release' global)" "log: the node says at start that it only logs"
J=$(probe log); echo "$J" > "$WORK/log/probe.json"
ck "$(judge log "$J" "$ALL" 2> "$WORK/log/judge.err")" "log: every call is allowed, the management action from all 16 gateway/target pairs"
cat "$WORK/log/judge.err" 2>/dev/null
W='AUTHORITY: WOULD REFUSE CONFIG getagentinfo from api:'
ck "$(has log "$W.* the word of default/rgr/rc \\(BELOW\\)" global)" "log: the global names a call through the region's gateway as from below"
ck "$(has log "$W.* the word of default/glob/a1 \\(ELSEWHERE\\)" region)" "log: the region controller names a call through another region's agent as from elsewhere"
ck "$(has log "$W.* the word of default/rgr/b1 \\(BELOW\\)" region)" "log: the region controller names a call through its own agent's gateway as from below"
ck "$(has log "$W.* the word of default/rgr/rc \\(ELSEWHERE\\)" a1)" "log: an agent names a call through another region's controller as from elsewhere"
ck "$(has log "$W.* the word of default/glob/a1 \\(ELSEWHERE\\)" b1)" "log: an agent names a call through a sibling's gateway as from elsewhere"
ck "$( ! grep -q -E 'AUTHORITY: WOULD REFUSE (WATCHDOG|KPI|CONFIG (agent|region)_(enable|disable))' "$WORK"/log/logs/*.log && echo 1)" "log: nothing a child reports upward would be refused"
stopall

echo "### phase 2: authority_direction=enforce on every node"
MODE="-Dauthority_direction=enforce"; fabric enforce
J=$(probe enforce); echo "$J" > "$WORK/enforce/probe.json"
ck "$(judge enforce "$J" "$TREE" 2> "$WORK/enforce/judge.err")" "enforce: the management action is honoured from the node itself or a controller above it (8 pairs) and answered 403 from the other 8; the open action everywhere"
cat "$WORK/enforce/judge.err" 2>/dev/null
ck "$( ! grep -q 'AUTHORITY: WOULD REFUSE [A-Z]' "$WORK"/enforce/logs/*.log && echo 1)" "enforce: nothing is logged as 'would refuse'"
sleep 45   # several WATCHDOG periods under enforce
J2=$(probe enforce); echo "$J2" > "$WORK/enforce/probe2.json"
ck "$(judge enforce "$J2" "$TREE" 2> "$WORK/enforce/judge2.err")" "enforce: 45 s later the fabric is whole and decides the same (what children report upward is not refused)"
cat "$WORK/enforce/judge2.err" 2>/dev/null
ck "$( ! grep -q -E 'unauthorized: (WATCHDOG|KPI|agent_enable|agent_disable|region_enable|region_disable|regionalimport)' "$WORK"/enforce/logs/*.log && echo 1)" "enforce: no WATCHDOG, KPI or registration was refused for its direction"
stopall
echo "  PASS=$PASS  FAIL=$FAIL  (logs: $WORK/log/logs, $WORK/enforce/logs)"
[ "$FAIL" = 0 ]
