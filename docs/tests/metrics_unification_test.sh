#!/usr/bin/env bash
# B-2 metrics-unification end-to-end test.
# Brings up global + region + agent, then uses the Python client's NEW get_metric_inventory() to pull the
# unified inventory (scope=global) and asserts:
#   - the controller's own Micrometer groups (jvm/controller) are present at every node
#   - the newly un-stubbed role gauges (regional/global group) are present
#   - the sysinfo-derived resource_summary is present (legacy path folded in)
#   - at least one plugin exposes metrics via the unified getmetrics contract (wsapi is always up on the global)
#   - mesh fan-out reaches child nodes (region/agent) — proving region/global scope aggregation
set -uo pipefail
cd "$(dirname "$0")/.."
source ./cresco.env
RUN="$(pwd)"; JAR="$RUN/${AGENT_JAR}"; LOGDIR="$RUN/tests/metrics-logs"; XMX=1024M
SEC="-Ddiscovery_secret_global=$CRESCO_DISCOVERY_SECRET_GLOBAL -Ddiscovery_secret_region=$CRESCO_DISCOVERY_SECRET_REGION -Ddiscovery_secret_agent=$CRESCO_DISCOVERY_SECRET_AGENT -Dcresco_service_key=$CRESCO_SERVICE_KEY"

pkill -9 -f "$JAR" 2>/dev/null
for _ in $(seq 1 20); do pgrep -f "$JAR" >/dev/null || break; sleep 1; done   # wait for full exit (Derby/port release)
sleep 2
rm -rf "$RUN/nodes" "$RUN/cresco-data" "$LOGDIR" 2>/dev/null; mkdir -p "$LOGDIR"

launch(){ local region="$1" name="$2" extra="$3" log="$4"; local dir="$RUN/nodes/${region}-${name}"; mkdir -p "$dir"
  ( cd "$dir" && exec java -Djava.net.preferIPv4Stack=true -Xmx"$XMX" -Dregionname="$region" -Dagentname="$name" \
      -Droot_log_level=INFO -Denable_controllermon=true $extra $SEC -jar "$JAR" ) > "$log" 2>&1 & }

echo "### launch global (wsapi on)"
launch global-region global-controller "-Dis_global=true -Denable_wsapi=true -Dport=8080 -Denable_dashboard=false" "$LOGDIR/global.log"
# gate on the global actually going active (wss listener up) — not just the process starting
for i in $(seq 1 60); do nc -z localhost 8282 2>/dev/null && break; sleep 2; done
if ! nc -z localhost 8282 2>/dev/null; then echo "FAIL  global never opened wss:8282 (see $LOGDIR/global.log)"; tail -5 "$LOGDIR/global.log"; pkill -9 -f "$JAR"; exit 1; fi
echo "### global up on 8282; launch region + agent"
launch edge-region edge-controller "-Dis_region=true -Dglobal_controller_host=127.0.0.1 -Dnetdiscoveryport=32015 -Dbroker_port=32020 -Denable_wsapi=false -Denable_dashboard=false" "$LOGDIR/region.log"
sleep 5
launch edge-region edge-agent-001 "-Dis_agent=true -Dregional_controller_host=127.0.0.1 -Dregional_controller_port=32015 -Ddiscovery_port=32020 -Dnetdiscoveryport=32025 -Dbroker_port=32030 -Denable_wsapi=false -Denable_dashboard=false" "$LOGDIR/agent.log"

echo "### wait for the agent to register with the region"
for i in $(seq 1 60); do grep -qE "registered with Region|Agent .* registered" "$LOGDIR/agent.log" 2>/dev/null && break; sleep 2; done
sleep 8   # let plugins settle + metrics register

echo "### query unified metric inventory via the Python client"
source venv/bin/activate 2>/dev/null
OUT="$(CRESCO_HOST=localhost CRESCO_PORT=8282 CRESCO_KEY=$CRESCO_SERVICE_KEY python3 tests/metrics_query.py 2>/dev/null)"
echo "$OUT"

echo "### assertions"
P=0; F=0; ck(){ if [ "$1" = "1" ]; then P=$((P+1)); echo "PASS  $2"; else F=$((F+1)); echo "FAIL  $2"; fi; }
val(){ echo "$OUT" | grep "RESULT $1=" | tail -1 | sed "s/RESULT $1=//"; }

ck "$([ "$(val connect)" = "ok" ] && echo 1)" "python client connected + queried getmetricinventory"
ck "$([ "$(val has_jvm)" = "1" ] && echo 1)" "controller JVM metrics present (unified Micrometer)"
ck "$([ "$(val has_controller_group)" = "1" ] && echo 1)" "controller group present (message.transaction.time timer)"
ck "$([ "$(val has_role_gauge)" = "1" ] && echo 1)" "un-stubbed role gauge present (regional/global group)"
ck "$([ "$(val has_resource_summary)" = "1" ] && echo 1)" "resource_summary present (sysinfo path folded in)"
ck "$([ "$(val has_wsapi)" = "1" ] && echo 1)" "wsapi plugin metrics present via unified getmetrics"
ck "$([ "$(val has_sysinfo_group)" = "1" ] && echo 1)" "sysinfo cpu/mem/disk/net metrics present via unified getmetrics"
ck "$([ "$(val has_stunnel)" = "1" ] && echo 1)" "stunnel plugin metrics present via unified getmetrics"
ck "$([ "$(val has_repo)" = "1" ] && echo 1)" "repo plugin metrics present via unified getmetrics"
ck "$([ "$(val children)" -ge 1 ] 2>/dev/null && echo 1)" "mesh fan-out reached child node(s) (region/agent scope)"
ck "$([ "$(val plugin_sources)" -ge 1 ] 2>/dev/null && echo 1)" "at least one plugin source in the unified inventory"

pkill -9 -f "$JAR" 2>/dev/null
echo "==================================================="; echo "  PASS=$P  FAIL=$F"
[ "$F" -eq 0 ] && echo "  METRICS UNIFICATION: PASS" || echo "  METRICS UNIFICATION: PARTIAL/FAIL (see printed inventory)"
exit "$F"
