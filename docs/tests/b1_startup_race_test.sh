#!/usr/bin/env bash
# B-1 regression: transient SSL/PKIX handshake race on rapid same-host launch.
#
# On a rapid same-host bring-up an agent can reach its first TLS connect to the regional broker a beat
# before the broker's cert is trust-ready (discovery cert exchange still settling / broker TLS transport
# still coming up). The ActiveMQ failover transport's first handshake then fails with
#   "PKIX path building failed: unable to find valid certification path to requested target"
# and dumps full FailoverTransport reconnect stack traces before the mesh self-heals — false-alarm noise.
#
# The fix (ControllerSMHandler.initIOChannels + ActiveClient.waitForBrokerTlsReady, flag
# broker_tls_ready_probe, default ON) gates the first connect on a quiet trust-ready TLS probe so the
# failover transport only ever touches a ready endpoint.
#
# This test launches global -> region -> agent back-to-back with NO stagger (the stagger the other suites
# use is exactly what hides this race) and asserts:
#   (1) probe ON  (default): agent registers AND its log has ZERO PKIX / FailoverTransport reconnect stacks.
#   (2) probe OFF (control): informational — report the PKIX count so the contrast is visible.
set -uo pipefail
cd "$(dirname "$0")/.."
source ./cresco.env
RUN="$(pwd)"; JAR="$RUN/${AGENT_JAR}"; LOGDIR="$RUN/tests/b1-logs"; XMX=1024M
PKIX_RE='PKIX path building failed|unable to find valid certification path|FailoverTransport.*(handshake_failure|bad_certificate)|SunCertPathBuilderException'

pkill -9 -f "$JAR" 2>/dev/null; sleep 2
rm -rf "$RUN/nodes" "$RUN/cresco-data" "$LOGDIR" 2>/dev/null; mkdir -p "$LOGDIR"

launch() { # $1=agentname $2=region $3=logfile ; rest = -D args
  local name="$1"; local region="$2"; local log="$3"; shift 3
  local dir="$RUN/nodes/${region}-${name}"; rm -rf "$dir"; mkdir -p "$dir/logs"
  ( cd "$dir" && exec java -Djava.net.preferIPv4Stack=true -Xmx"$XMX" \
      -Dregionname="$region" -Dagentname="$name" -Droot_log_level=INFO \
      -Ddiscovery_secret_global="$CRESCO_DISCOVERY_SECRET_GLOBAL" \
      -Ddiscovery_secret_region="$CRESCO_DISCOVERY_SECRET_REGION" \
      -Ddiscovery_secret_agent="$CRESCO_DISCOVERY_SECRET_AGENT" \
      -Dcresco_service_key="$CRESCO_SERVICE_KEY" \
      "$@" -jar "$JAR" ) > "$log" 2>&1 &
}
waitlog(){ local end=$((SECONDS+$3)); while [ $SECONDS -lt $end ]; do grep -qE "$2" "$1" 2>/dev/null && return 0; sleep 1; done; return 1; }

P=0; F=0; ck(){ if [ "$1" = "1" ]; then P=$((P+1)); echo "PASS  $2"; else F=$((F+1)); echo "FAIL  $2"; fi; }

run_round(){ # $1=label $2=probe(true|false) $3=agentlog
  local label="$1" probe="$2" alog="$3"
  echo "### round: $label (broker_tls_ready_probe=$probe)"
  pkill -9 -f "$JAR" 2>/dev/null; sleep 2; rm -rf "$RUN/nodes" "$RUN/cresco-data" 2>/dev/null

  # global first, wait until its control port is accepting (broker/region can then form)
  launch global-controller global-region "$LOGDIR/${label}-global.log" \
    -Dport=8080 -Dis_global=true -Denable_wsapi=true -Denable_console=false -Denable_dashboard=false
  for i in $(seq 1 45); do nc -z localhost 8282 2>/dev/null && break; sleep 2; done

  # region + agent back-to-back, NO stagger -> maximize the agent<->region-broker readiness race.
  launch edge-controller edge-region "$LOGDIR/${label}-region.log" \
    -Dis_region=true -Dglobal_controller_host=127.0.0.1 -Dnetdiscoveryport=32015 -Dbroker_port=32020 \
    -Denable_wsapi=false -Denable_console=false -Denable_dashboard=false
  launch edge-agent-001 edge-region "$alog" \
    -Dis_agent=true -Dregional_controller_host=127.0.0.1 -Dregional_controller_port=32015 -Ddiscovery_port=32020 \
    -Dnetdiscoveryport=32025 -Dbroker_port=32030 -Denable_wsapi=false -Denable_console=false -Denable_dashboard=false \
    -Dbroker_tls_ready_probe="$probe"

  # agent registers with its region
  waitlog "$alog" "registered with Region|Agent .* registered" 150 && local reg=1 || local reg=0
  local pkix; pkix=$(grep -cE "$PKIX_RE" "$alog" 2>/dev/null); pkix=${pkix:-0}
  local probed; probed=$(grep -cE "became trust-ready after [0-9]+ probe attempt" "$alog" 2>/dev/null); probed=${probed:-0}
  local engaged; engaged=$(grep -cE "Probing broker TLS readiness" "$alog" 2>/dev/null); engaged=${engaged:-0}
  echo "    agent registered=$reg  gate-engaged=$engaged  PKIX/failover-stack lines=$pkix  probe-absorbed-race=$probed"
  RREG=$reg; RPKIX=$pkix; RPROBED=$probed; RENGAGED=$engaged
}

# ---------------- (1) probe ON (default) ----------------
run_round "probe-on" true "$LOGDIR/probe-on-agent.log"
ck "$([ "$RREG" = "1" ] && echo 1)" "probe ON: agent registered with region under TLS"
ck "$([ "$RENGAGED" -ge 1 ] && echo 1)" "probe ON: trust-ready gate engaged on the real first-connect path"
ck "$([ "$RPKIX" = "0" ] && echo 1)" "probe ON: agent log has ZERO PKIX/failover reconnect stacks"
[ "$RPROBED" -ge 1 ] && echo "NOTE  the probe actively absorbed a real readiness race this run ($RPROBED time(s))" \
                     || echo "NOTE  endpoint was ready on first probe this run (no race window hit) — clean either way"

# ---------------- (2) probe OFF (control, informational) ----------------
run_round "probe-off" false "$LOGDIR/probe-off-agent.log"
ck "$([ "$RREG" = "1" ] && echo 1)" "probe OFF (control): agent still eventually registers (failover self-heals)"
echo "NOTE  probe OFF PKIX/failover-stack lines this run = $RPKIX (contrast; may be 0 if timing didn't race)"

pkill -9 -f "$JAR" 2>/dev/null
echo "==================================================="; echo "  PASS=$P  FAIL=$F"
[ "$F" -eq 0 ] && echo "  B-1 STARTUP-RACE: PASS" || echo "  B-1 STARTUP-RACE: FAIL"
exit "$F"
