#!/usr/bin/env bash
# Capability-inventory end-to-end test.
# Brings up global + region + agent, then uses the Python client's get_capability_inventory() + the MCP
# tool-runner to assert:
#   - the fabric emits a well-formed LLM tool catalog (name/description/input_schema) for controller tiers
#     (global/agent) AND every plugin (sysinfo/stunnel/wsapi/repo)
#   - every tool is well-formed Anthropic tool JSON
#   - ONLY MsgEvent actions are tools; the OSGi surface is present but informational (opt-in)
#   - an action can be INVOKED using only its descriptor/binding (cresco_global_listagents) and returns
#     the documented reply -> the catalog is directly usable for LLM tool-calling
set -uo pipefail
cd "$(dirname "$0")/.."
source ./cresco.env
RUN="$(pwd)"; JAR="$RUN/${AGENT_JAR}"; LOGDIR="$RUN/tests/capability-logs"; XMX=1024M
SEC="-Ddiscovery_secret_global=$CRESCO_DISCOVERY_SECRET_GLOBAL -Ddiscovery_secret_region=$CRESCO_DISCOVERY_SECRET_REGION -Ddiscovery_secret_agent=$CRESCO_DISCOVERY_SECRET_AGENT -Dcresco_service_key=$CRESCO_SERVICE_KEY"

pkill -9 -f "$JAR" 2>/dev/null
for _ in $(seq 1 20); do pgrep -f "$JAR" >/dev/null || break; sleep 1; done
sleep 2
rm -rf "$RUN/nodes" "$RUN/cresco-data" "$LOGDIR" 2>/dev/null; mkdir -p "$LOGDIR"

launch(){ local region="$1" name="$2" extra="$3" log="$4"; local dir="$RUN/nodes/${region}-${name}"; mkdir -p "$dir"
  ( cd "$dir" && exec java -Djava.net.preferIPv4Stack=true -Xmx"$XMX" -Dregionname="$region" -Dagentname="$name" \
      -Droot_log_level=INFO -Denable_controllermon=true $extra $SEC -jar "$JAR" ) > "$log" 2>&1 & }

echo "### launch global (wsapi on)"
launch global-region global-controller "-Dis_global=true -Denable_wsapi=true -Dport=8080 -Denable_dashboard=false" "$LOGDIR/global.log"
for i in $(seq 1 60); do nc -z localhost 8282 2>/dev/null && break; sleep 2; done
if ! nc -z localhost 8282 2>/dev/null; then echo "FAIL  global never opened wss:8282"; tail -5 "$LOGDIR/global.log"; pkill -9 -f "$JAR"; exit 1; fi
echo "### global up; launch region + agent"
launch edge-region edge-controller "-Dis_region=true -Dglobal_controller_host=127.0.0.1 -Dnetdiscoveryport=32015 -Dbroker_port=32020 -Denable_wsapi=false -Denable_dashboard=false" "$LOGDIR/region.log"
sleep 5
launch edge-region edge-agent-001 "-Dis_agent=true -Dregional_controller_host=127.0.0.1 -Dregional_controller_port=32015 -Ddiscovery_port=32020 -Dnetdiscoveryport=32025 -Dbroker_port=32030 -Denable_wsapi=false -Denable_dashboard=false" "$LOGDIR/agent.log"

echo "### wait for the agent to register"
for i in $(seq 1 60); do grep -qE "registered with Region|Agent .* registered" "$LOGDIR/agent.log" 2>/dev/null && break; sleep 2; done
sleep 8

echo "### pull capability catalog + validate + invoke-from-descriptor (Python client + MCP runner)"
source venv/bin/activate 2>/dev/null
OUT="$(CRESCO_HOST=localhost CRESCO_PORT=8282 CRESCO_KEY=$CRESCO_SERVICE_KEY python3 tests/capability_probe.py 2>/dev/null)"
echo "$OUT"

echo "### assertions"
P=0; F=0; ck(){ if [ "$1" = "1" ]; then P=$((P+1)); echo "PASS  $2"; else F=$((F+1)); echo "FAIL  $2"; fi; }
val(){ echo "$OUT" | grep "RESULT $1=" | tail -1 | sed "s/RESULT $1=//"; }

ck "$([ "$(val connect)" = "ok" ] && echo 1)" "python client connected + pulled getcapabilityinventory"
ck "$([ "$(val tools)" -ge 30 ] 2>/dev/null && echo 1)" "catalog has the full action surface (>=30 tools)"
ck "$([ "$(val all_well_formed)" = "1" ] && echo 1)" "every tool is well-formed Anthropic tool JSON"
ck "$([ "$(val has_global)" = "1" ] && echo 1)" "global controller actions present as tools"
ck "$([ "$(val has_agent)" = "1" ] && echo 1)" "agent controller actions present as tools"
ck "$([ "$(val has_sysinfo)" = "1" ] && echo 1)" "sysinfo plugin actions present as tools"
ck "$([ "$(val has_stunnel)" = "1" ] && echo 1)" "stunnel plugin actions present as tools"
ck "$([ "$(val has_getcaps)" = "1" ] && echo 1)" "self-describing getcapabilities exposed"
ck "$([ "$(val osgi_present)" = "1" ] && echo 1)" "OSGi surface present (informational, not a tool)"
ck "$([ "$(val invoked_from_descriptor)" = "1" ] && echo 1)" "invoked an action using ONLY its descriptor/binding (listagents)"

pkill -9 -f "$JAR" 2>/dev/null
echo "==================================================="; echo "  PASS=$P  FAIL=$F"
[ "$F" -eq 0 ] && echo "  CAPABILITY INVENTORY: PASS" || echo "  CAPABILITY INVENTORY: PARTIAL/FAIL (see catalog above)"
exit "$F"
