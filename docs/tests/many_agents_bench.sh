#!/usr/bin/env bash
# Membership at scale on one host: a global and a region, then N light agents (no plugins) started together -- half on the
# global's broker, half behind the region -- as a restart storm would. Measures how long until the global's registry lists
# every agent (discovery, enrollment, registration, the region's export), the controllers' CPU while the storm lasts and
# then at steady state (keepalives, pings), and counts refusals. Run it per build and compare the JSON line it prints.
#
# Usage: many_agents_bench.sh <agent.jar> <workdir> <python-with-pycrescolib>
#   N (default 40) agents; AGENT_XMX (default 160m) each. LEGACY=1: the jar is an earlier build (no hardening settings).
#   PORT0 (default 33500) moves the port block (PORT0..PORT0+20 and PORT0+100+i for the agents' discovery ports, wsapi
#   PORT0+480). PYCRESCOLIB_PATH goes first on PYTHONPATH. Each agent JVM takes ~200 MB: size N to the host.
# Starts and stops only its own JVMs (by PID).
set -uo pipefail
JAR="$1"; WORK="$2"; PY="$3"
HERE="$(cd "$(dirname "$0")" && pwd)"
P0="${PORT0:-33500}"; WS0=$((P0+480)); N="${N:-40}"
export PYTHONPATH="${PYCRESCOLIB_PATH:-}${PYTHONPATH:+:$PYTHONPATH}"
rm -rf "$WORK"; mkdir -p "$WORK/logs"
rnd() { "$PY" -c 'import secrets; print(secrets.token_hex(16))'; }
KEY="$(rnd)"
SEC="-Ddiscovery_secret_global=$(rnd) -Ddiscovery_secret_region=$(rnd) -Ddiscovery_secret_agent=$(rnd) -Dcresco_service_key=$KEY"
COMMON="-Djava.net.preferIPv4Stack=true -Droot_log_level=WARN -Denable_console=false -Denable_dashboard=false"
LIGHT="-Denable_stunnel=false -Denable_sysinfo=false -Denable_executor=false -Denable_filerepo=false -Denable_repo=false -Denable_wsapi=false"
PIDS="$WORK/pids"; : > "$PIDS"
launch() { # name xmx flags
  local d="$WORK/nodes/$1"; mkdir -p "$d"
  ( cd "$d" && exec java $COMMON -Xmx$2 $SEC $3 -jar "$JAR" ) > "$WORK/logs/$1.log" 2>&1 < /dev/null &
  echo $! >> "$PIDS"; echo $! > "$WORK/pid.$1"
}
stopall() { while read -r p; do kill "$p" 2>/dev/null; done < "$PIDS"; sleep 3; while read -r p; do kill -9 "$p" 2>/dev/null; done < "$PIDS"; }
trap stopall EXIT
waitport() { for _ in $(seq 1 90); do nc -z 127.0.0.1 "$1" 2>/dev/null && return 0; sleep 2; done; return 1; }
cpu() { ps -o time= -p "$(cat "$WORK/pid.$1")" 2>/dev/null | awk -F'[:.]' '{ if (NF==3) print $1*60+$2; else print $1*3600+$2*60+$3 }'; }
pin() { local f; f=$(find "$WORK/nodes/global/cresco-data/plugin-data" -name ws.fingerprint 2>/dev/null | head -1); [ -n "$f" ] && head -1 "$f" | tr -d '[:space:]'; }
count() { "$PY" "$HERE/fleet_agents.py" --port "$WS0" --key "$KEY" --pin "$(pin)" | "$PY" -c 'import json,sys
d=json.loads(sys.stdin.read() or "{}"); print(len([a for a in d.get("agents", []) if a.split("/")[1].startswith("s")]))' 2>/dev/null || echo 0; }

launch global 1024m "-Dis_global=true -Dregionname=glob -Dagentname=gc -Dnetdiscoveryport=$((P0+5)) -Dbroker_port=$((P0+10)) -Ddiscovery_port=$((P0+10)) -Denable_wsapi=true -Dwsapi_port=$WS0"
waitport "$WS0" || { echo "FATAL: the global did not come up"; exit 2; }
launch region 1024m "-Dis_region=true -Dregionname=rgr -Dagentname=rc -Dglobal_controller_host=127.0.0.1 -Dglobal_controller_port=$((P0+5)) -Dnetdiscoveryport=$((P0+15)) -Dbroker_port=$((P0+20)) -Ddiscovery_port_remote=$((P0+10)) -Denable_wsapi=false"
waitport $((P0+20)) || { echo "FATAL: the region did not come up"; exit 2; }
for _ in $(seq 1 60); do grep -q "Broker bridge from verified controller\|Starting Bridge" "$WORK/logs/global.log" "$WORK/logs/region.log" 2>/dev/null && break; sleep 2; done
sleep 20
g0=$(cpu global); r0=$(cpu region); t0=$(date +%s)
echo "### $N agents start together"
for i in $(seq 1 "$N"); do
  if [ $((i % 2)) = 1 ]; then reg=glob; cp=$((P0+5)); else reg=rgr; cp=$((P0+15)); fi
  launch "s$i" "${AGENT_XMX:-160m}" "-Dis_agent=true -Dregionname=$reg -Dagentname=s$i -Dregional_controller_host=127.0.0.1 -Dregional_controller_port=$cp -Dnetdiscoveryport=$((P0+100+i)) $LIGHT"
done
listed=0; t_all=timeout
for _ in $(seq 1 120); do
  listed=$(count); [ "$listed" -ge "$N" ] && { t_all=$(( $(date +%s) - t0 )); break; }; sleep 5
done
g1=$(cpu global); r1=$(cpu region); t1=$(date +%s)
echo "### steady state, 60 s"
sleep 60
g2=$(cpu global); r2=$(cpu region)
refused() { cat "$WORK/logs/global.log" "$WORK/logs/region.log" | grep -cE "MESSAGE SIGNATURE: REFUSED|Discovery REFUSED|REFUSING TO START" || true; }
echo "BENCH {\"agents\": $N, \"listed\": $listed, \"all_listed_after_s\": \"$t_all\", \"storm_s\": $((t1 - t0)), \"global_cpu_s_storm\": $((g1 - g0)), \"region_cpu_s_storm\": $((r1 - r0)), \"global_cpu_pct_steady\": $(( (g2 - g1) * 100 / 60 )), \"region_cpu_pct_steady\": $(( (r2 - r1) * 100 / 60 )), \"refusal_lines\": $(refused)}"
[ "$listed" -ge "$N" ]
