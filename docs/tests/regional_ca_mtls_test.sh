#!/usr/bin/env bash
# Regional-CA enrollment + fabric-wide mutual TLS test (distributed-identity design, Option C).
# Brings up a GLOBAL (acting as the issuing CA) + region controllers with:
#   security_regional_ca=true   -> region enrolls with the global; global signs its identity, region
#                                  installs it and trusts the global CA (chain-based, O(regions) trust)
#   broker_require_client_auth  -> the broker demands a validated client cert (fabric-wide mutual TLS)
# and proves the region JOINS the global under mutual TLS (the boundary that per-node cert exchange
# could not cross), with federation + RTT healthy.
set -uo pipefail
RUN="$(cd "$(dirname "$0")/.." && pwd)"; JARP="$RUN/agent-1.3-SNAPSHOT.jar"; source "$RUN/cresco.env"
SEC="-Ddiscovery_secret_global=$CRESCO_DISCOVERY_SECRET_GLOBAL -Ddiscovery_secret_region=$CRESCO_DISCOVERY_SECRET_REGION -Ddiscovery_secret_agent=$CRESCO_DISCOVERY_SECRET_AGENT -Dcresco_service_key=$CRESCO_SERVICE_KEY"
SECURITY="-Dbroker_security_enabled=true -Dbroker_require_client_auth=true -Dsecurity_regional_ca=true"
NREGIONS="${1:-2}"

pkill -9 -f "agent-1.3-SNAPSHOT.jar" 2>/dev/null; sleep 2; rm -rf "$RUN/nodes" "$RUN/cresco-data" /tmp/rca-*.log 2>/dev/null
launch(){ local d="$RUN/nodes/$1"; mkdir -p "$d"; ( cd "$d"; nohup java -Djava.net.preferIPv4Stack=true -Xmx2048M -Dregionname=$1 -Dagentname=$2 $3 \
  -Denable_console=false -Denable_dashboard=false -Droot_log_level=INFO -Dnet_metrics_log=true $SECURITY $4 $SEC -jar "$JARP" >"$5" 2>&1 </dev/null & disown ) 2>/dev/null; }

echo "### launch global (regional CA issuer) with mutual TLS"
launch global-region global-controller "-Dis_global=true -Denable_wsapi=true -Dport=8080" "-Dtenant_id=global" /tmp/rca-global.log
for i in $(seq 1 45); do nc -z localhost 8282 2>/dev/null && break; sleep 2; done

echo "### launch $NREGIONS region(s), each enrolling with the global under mutual TLS"
P=0; F=0; ck(){ if [ "$1" = "1" ]; then P=$((P+1)); echo "PASS  $2"; else F=$((F+1)); echo "FAIL  $2"; fi; }
tenants=(tenantA tenantB tenantC tenantD)
for n in $(seq 1 "$NREGIONS"); do
  bp=$((32010 + n*10)); dp=$((32014 + n)); t=${tenants[$((n-1))]}
  launch "region-$n" "controller-$n" "-Dis_region=true -Dglobal_controller_host=127.0.0.1 -Dnetdiscoveryport=$dp -Dbroker_port=$bp -Denable_wsapi=false" "-Dtenant_id=$t" "/tmp/rca-region-$n.log"
done

echo "### wait for enrollment + federation + RTT samples (up to ~3min; ping fires every 5s)"
for i in $(seq 1 60); do
  ok=1
  for n in $(seq 1 "$NREGIONS"); do
    grep "NETLINK" "/tmp/rca-region-$n.log" 2>/dev/null | grep -qE "Link\[global-region.*samples=[1-9]" || ok=0
  done
  [ "$ok" = "1" ] && break
  sleep 3
done

echo "### assertions"
ck "$([ "$(grep -c 'Region CA established' /tmp/rca-global.log 2>/dev/null)" -ge 1 ] && echo 1)" "global established a region CA (issuer)"
for n in $(seq 1 "$NREGIONS"); do
  L="/tmp/rca-region-$n.log"
  ck "$([ "$(grep -c 'Installed region-CA-signed identity' $L 2>/dev/null)" -ge 1 ] && echo 1)" "region-$n installed a global-CA-signed identity (enrolled)"
  ck "$([ "$(grep -c 'Starting Bridge' $L 2>/dev/null)" -ge 1 ] && echo 1)" "region-$n bridged to global under MUTUAL TLS"
  ck "$([ "$(grep -cE 'PKIX|unable to find valid cert|bad_certificate|handshake_failure' $L 2>/dev/null)" = 0 ] && echo 1)" "region-$n: no TLS trust failures"
  rtt=$(grep NETLINK $L 2>/dev/null | grep -cE 'Link\[global-region.*samples=[1-9]')
  ck "$([ "${rtt:-0}" -ge 1 ] && echo 1)" "region-$n: federation RTT harvested (healthy)"
done
ck "$([ "$(grep -c 'mutual-TLS ENABLED' /tmp/rca-global.log 2>/dev/null)" -ge 1 ] && echo 1)" "global broker requires client certs (mutual TLS ON)"

pkill -9 -f "agent-1.3-SNAPSHOT.jar" 2>/dev/null
echo "==================================================="; echo "  PASS=$P  FAIL=$F"
[ "$F" -eq 0 ] && echo "  REGIONAL-CA mTLS FEDERATION: PASS" || echo "  REGIONAL-CA mTLS FEDERATION: FAIL"
exit $F
