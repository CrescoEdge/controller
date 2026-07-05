#!/usr/bin/env bash
# CEP validation harness: bring up a single global node (global-tier controller is also an
# agent, so it has AgentExecutor.cepadd + DataPlaneService), then create one CEP per query
# (core engine + every bundled extension) and confirm each compiles/starts under Siddhi 5.1.33.
set -uo pipefail
cd "$(dirname "$0")/.."
source ./cresco.env
RUN="$(pwd)"; JAR="$RUN/${AGENT_JAR}"; LOGDIR="$RUN/tests/cep-df-logs"; XMX=1024M
SEC="-Ddiscovery_secret_global=$CRESCO_DISCOVERY_SECRET_GLOBAL -Ddiscovery_secret_region=$CRESCO_DISCOVERY_SECRET_REGION -Ddiscovery_secret_agent=$CRESCO_DISCOVERY_SECRET_AGENT -Dcresco_service_key=$CRESCO_SERVICE_KEY"

pkill -9 -f "$JAR" 2>/dev/null
for _ in $(seq 1 25); do pgrep -f "$JAR" >/dev/null || break; sleep 1; done
sleep 2
rm -rf "$RUN/nodes" "$RUN/cresco-data" "$LOGDIR" 2>/dev/null; mkdir -p "$LOGDIR"

launch(){ local region="$1" name="$2" extra="$3" log="$4"; local dir="$RUN/nodes/${region}-${name}"; mkdir -p "$dir"
  ( cd "$dir" && exec java -Djava.net.preferIPv4Stack=true -Xmx"$XMX" -Dregionname="$region" -Dagentname="$name" \
      -Droot_log_level=INFO $extra $SEC -jar "$JAR" ) > "$log" 2>&1 & }

echo "### launch global (wsapi on)"
launch global-region global-controller "-Dis_global=true -Denable_wsapi=true -Dport=8080 -Denable_dashboard=false" "$LOGDIR/global.log"
for i in $(seq 1 90); do nc -z localhost 8282 2>/dev/null && break; sleep 2; done
if ! nc -z localhost 8282 2>/dev/null; then echo "FAIL global never opened wss:8282"; tail -12 "$LOGDIR/global.log"; pkill -9 -f "$JAR"; exit 1; fi
sleep 8

echo "### run CEP data-flow"
source venv/bin/activate 2>/dev/null
CRESCO_KEY=$CRESCO_SERVICE_KEY CEP_REGION=global-region CEP_AGENT=global-controller \
  python3 tests/cep_dataflow.py 2>&1 | grep -vE "SSL certificate"
rc=${PIPESTATUS[0]}

echo "### siddhi-related agent log lines"
grep -iE "siddhi|CEP|extension|NoClassDef|ClassNotFound" "$LOGDIR/global.log" | grep -viE "INFO .*wsapi" | tail -20

pkill -9 -f "$JAR" 2>/dev/null
echo "### rc=$rc"
exit $rc
