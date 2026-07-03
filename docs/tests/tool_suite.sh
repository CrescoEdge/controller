#!/usr/bin/env bash
# Exhaustive tool-suite harness: bring up global + region + agent, then run tool_suite.py to exercise
# every capability-inventory tool against the live mesh and capture real responses to /tmp/tool-results.json.
set -uo pipefail
cd "$(dirname "$0")/.."
source ./cresco.env
RUN="$(pwd)"; JAR="$RUN/${AGENT_JAR}"; LOGDIR="$RUN/tests/toolsuite-logs"; XMX=1024M
SEC="-Ddiscovery_secret_global=$CRESCO_DISCOVERY_SECRET_GLOBAL -Ddiscovery_secret_region=$CRESCO_DISCOVERY_SECRET_REGION -Ddiscovery_secret_agent=$CRESCO_DISCOVERY_SECRET_AGENT -Dcresco_service_key=$CRESCO_SERVICE_KEY"

pkill -9 -f "$JAR" 2>/dev/null
for _ in $(seq 1 25); do pgrep -f "$JAR" >/dev/null || break; sleep 1; done
sleep 3
rm -rf "$RUN/nodes" "$RUN/cresco-data" "$LOGDIR" 2>/dev/null; mkdir -p "$LOGDIR"

launch(){ local region="$1" name="$2" extra="$3" log="$4"; local dir="$RUN/nodes/${region}-${name}"; mkdir -p "$dir"
  ( cd "$dir" && exec java -Djava.net.preferIPv4Stack=true -Xmx"$XMX" -Dregionname="$region" -Dagentname="$name" \
      -Droot_log_level=INFO -Denable_controllermon=true $extra $SEC -jar "$JAR" ) > "$log" 2>&1 & }

echo "### launch global (wsapi on) + region + agent"
launch global-region global-controller "-Dis_global=true -Denable_wsapi=true -Dport=8080 -Denable_dashboard=false" "$LOGDIR/global.log"
for i in $(seq 1 90); do nc -z localhost 8282 2>/dev/null && break; sleep 2; done
if ! nc -z localhost 8282 2>/dev/null; then echo "FAIL global never opened wss:8282"; tail -8 "$LOGDIR/global.log"; pkill -9 -f "$JAR"; exit 1; fi
launch edge-region edge-controller "-Dis_region=true -Dglobal_controller_host=127.0.0.1 -Dnetdiscoveryport=32015 -Dbroker_port=32020 -Denable_wsapi=false -Denable_dashboard=false" "$LOGDIR/region.log"
sleep 5
launch edge-region edge-agent-001 "-Dis_agent=true -Dregional_controller_host=127.0.0.1 -Dregional_controller_port=32015 -Ddiscovery_port=32020 -Dnetdiscoveryport=32025 -Dbroker_port=32030 -Denable_wsapi=false -Denable_dashboard=false" "$LOGDIR/agent.log"
for i in $(seq 1 60); do grep -qE "registered with Region|Agent .* registered" "$LOGDIR/agent.log" 2>/dev/null && break; sleep 2; done
sleep 10

echo "### exercise every tool"
source venv/bin/activate 2>/dev/null
CRESCO_KEY=$CRESCO_SERVICE_KEY python3 tests/tool_suite.py 2>/dev/null | grep -vE "SSL certificate"

pkill -9 -f "$JAR" 2>/dev/null
echo "results -> /tmp/tool-results.json"
