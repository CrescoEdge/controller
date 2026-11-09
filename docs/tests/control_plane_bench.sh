#!/usr/bin/env bash
# Repeatable control-plane benchmark: MsgEvent round-trip latency (p50/p90/p99) and concurrent rate through a live fabric,
# direct and across a bridge, for one build and one security mode. Run it per build/mode and compare the JSON it prints.
#
#   global glob/gc (wsapi :WS0)   agent glob/a1 (on the global's broker: "direct")
#   region rgr/rc (bridged)       agent rgr/b1  (behind the region: "bridge")
#
# Usage: control_plane_bench.sh <agent.jar> <workdir> <python-with-pycrescolib>
#   MODE=standard (default: the secure defaults, message signatures on) | nosign (msg_signing_required=false: what the
#        signatures cost) | nodirect (msg_signing_direct=false: every message signed, as before direct links) |
#        insecure (broker_security_enabled=false: the floor) | production (security_profile=production,
#        every identity issued by tools/cresco-pki.sh, PKI discovery, wsapi client certificates)
#   LEGACY=1: the jar is an earlier build (no hardening settings at all; MODE is ignored)
#   N (300) serial calls per target, THREADS (1,8,32) concurrent callers, SECONDS_PER (15) per concurrency level
#   PORT0 (default 33000) moves the port block (PORT0..PORT0+40, wsapi PORT0+480). PYCRESCOLIB_PATH goes first on PYTHONPATH.
#   HOSTS: for a multi-host run, see the comment at the end of this file.
# Prints one JSON line (BENCH ...) with the mode, the build and the results. Starts and stops only its own JVMs.
set -uo pipefail
JAR="$1"; WORK="$2"; PY="$3"
HERE="$(cd "$(dirname "$0")" && pwd)"; TOOL="$HERE/../../tools/cresco-pki.sh"
P0="${PORT0:-33000}"; WS0=$((P0+480)); MODE="${MODE:-standard}"
export PYTHONPATH="${PYCRESCOLIB_PATH:-}${PYTHONPATH:+:$PYTHONPATH}"
rm -rf "$WORK"; mkdir -p "$WORK/logs"
rnd() { "$PY" -c 'import secrets; print(secrets.token_hex(16))'; }
COMMON="-Djava.net.preferIPv4Stack=true -Xmx1024M -Droot_log_level=WARN -Denable_console=false -Denable_dashboard=false"
KEY="$(rnd)"
SEC="-Ddiscovery_secret_global=$(rnd) -Ddiscovery_secret_region=$(rnd) -Ddiscovery_secret_agent=$(rnd) -Dcresco_service_key=$KEY"
SECURITY=""
case "${LEGACY:-0}:$MODE" in
  1:*) SECURITY="" ;;
  0:standard) SECURITY="" ;;
  0:nosign) SECURITY="-Dmsg_signing_required=false" ;;
  0:nodirect) SECURITY="-Dmsg_signing_direct=false" ;;
  0:insecure) SECURITY="-Dbroker_security_enabled=false" ;;
  0:production)
    PKI="$WORK/pki"
    { bash "$TOOL" init "$PKI" --days 30 && bash "$TOOL" region "$PKI" t1 rgr --days 30 \
      && bash "$TOOL" node "$PKI" t1 glob gc --global --days 10 && bash "$TOOL" node "$PKI" t1 rgr rc --controller --days 10 \
      && bash "$TOOL" node "$PKI" t1 glob a1 --days 10 && bash "$TOOL" node "$PKI" t1 rgr b1 --days 10 \
      && bash "$TOOL" api "$PKI" t1 bench admin --days 10 && bash "$TOOL" crl "$PKI"; } > "$WORK/pki.log" 2>&1 || { echo "FATAL: the PKI tool failed"; exit 2; }
    openssl req -x509 -newkey rsa:3072 -sha384 -days 10 -nodes -keyout "$WORK/codesign.key" -out "$WORK/codesign.pem" \
      -subj "/O=t1/CN=bench code signing" >/dev/null 2>&1 && chmod 600 "$WORK/codesign.key" "$WORK/codesign.pem"
    SEC=""; KEY=""
    export CRESCO_TLS_CLIENT_CERT="$PKI/api/t1_bench/api.pem" CRESCO_TLS_CLIENT_KEY="$PKI/api/t1_bench/api.key"
    ;;
  *) echo "FATAL: MODE=$MODE?"; exit 2 ;;
esac
PIDS="$WORK/pids"; : > "$PIDS"
launch() { # name region agent flags
  local d="$WORK/nodes/$1" id=""; mkdir -p "$d"
  if [ "${LEGACY:-0}" = 0 ] && [ "$MODE" = production ]; then
    local n="$PKI/nodes/$2_$3"
    id="-Dsecurity_profile=production -Dtenant_id=t1 -Didentity_keystore=$n/node.p12 -Didentity_keystore_password_file=$n/node.p12.pass"
    id="$id -Dtrust_anchors=$n/trust-anchors.pem -Drevocation_file=$PKI/revocations.pem -Dplugin_signing_truststore=$WORK/codesign.pem"
    id="$id -Dwsapi_client_ca=$n/trust-anchors.pem"
  fi
  ( cd "$d" && exec java $COMMON $SEC $SECURITY $id -Dregionname=$2 -Dagentname=$3 $4 -jar "$JAR" ) > "$WORK/logs/$1.log" 2>&1 < /dev/null &
  echo $! >> "$PIDS"
}
stopall() { while read -r p; do kill "$p" 2>/dev/null; done < "$PIDS"; sleep 3; while read -r p; do kill -9 "$p" 2>/dev/null; done < "$PIDS"; }
trap stopall EXIT
waitport() { for _ in $(seq 1 90); do nc -z 127.0.0.1 "$1" 2>/dev/null && return 0; sleep 2; done; return 1; }

launch global glob gc "-Dis_global=true -Dnetdiscoveryport=$((P0+5)) -Dbroker_port=$((P0+10)) -Ddiscovery_port=$((P0+10)) -Denable_wsapi=true -Dwsapi_port=$WS0"
waitport "$WS0" || { echo "FATAL: the global did not come up"; exit 2; }
launch region rgr rc "-Dis_region=true -Dglobal_controller_host=127.0.0.1 -Dglobal_controller_port=$((P0+5)) -Dnetdiscoveryport=$((P0+15)) -Dbroker_port=$((P0+20)) -Ddiscovery_port_remote=$((P0+10)) -Denable_wsapi=false"
waitport $((P0+20)) || { echo "FATAL: the region did not come up"; exit 2; }
launch a1 glob a1 "-Dis_agent=true -Dregional_controller_host=127.0.0.1 -Dregional_controller_port=$((P0+5)) -Dnetdiscoveryport=$((P0+25)) -Denable_wsapi=false"
launch b1 rgr b1 "-Dis_agent=true -Dregional_controller_host=127.0.0.1 -Dregional_controller_port=$((P0+15)) -Dnetdiscoveryport=$((P0+35)) -Denable_wsapi=false"
pin() { local f; f=$(find "$WORK/nodes/global/cresco-data/plugin-data" -name ws.fingerprint 2>/dev/null | head -1); [ -n "$f" ] && head -1 "$f" | tr -d '[:space:]'; }
ready() { "$PY" - "$WS0" "$KEY" "$(pin)" <<'EOF'
import sys
from pycrescolib.clientlib import clientlib
port, key, pin = int(sys.argv[1]), sys.argv[2] or None, sys.argv[3]
c = clientlib("127.0.0.1", port, key, **({"fingerprint": pin} if pin else {}))
ok = False
try:
    if c.connect():
        ok = all(str(c.messaging.global_agent_msgevent(True, "EXEC", {"action": "iscontrolleractive"}, r, a, timeout=5.0)
                     .get("is_controller_active", "")).lower() in ("true", "1") for r, a in (("glob", "a1"), ("rgr", "b1")))
except Exception:
    ok = False
finally:
    try: c.close()
    except Exception: pass
print("ready" if ok else "not-ready")
EOF
}
for _ in $(seq 1 60); do [ "$(ready 2>/dev/null)" = ready ] && break; sleep 3; done
[ "$(ready 2>/dev/null)" = ready ] || { echo "FATAL: the fabric did not answer (see $WORK/logs)"; exit 2; }
sleep 10    # registrations and first exports settle
OUT=$("$PY" "$HERE/control_plane_bench.py" --port "$WS0" --key "$KEY" --pin "$(pin)" --n "${N:-300}" --threads "${THREADS:-1,8,32}" \
  --seconds "${SECONDS_PER:-15}" --target global=glob/gc --target direct=glob/a1 --target bridge=rgr/b1 2> "$WORK/bench.err")
BUILD=$(unzip -p "$JAR" controller.jar > "$WORK/c.jar" 2>/dev/null && unzip -p "$WORK/c.jar" META-INF/MANIFEST.MF 2>/dev/null | grep -i '^Bundle-Version' | tr -d '\r' | cut -d' ' -f2)
echo "BENCH {\"mode\": \"$([ "${LEGACY:-0}" = 1 ] && echo legacy || echo "$MODE")\", \"controller\": \"${BUILD:-?}\", \"results\": ${OUT:-null}}"
[ -n "$OUT" ]

# Multi-host (the DGX): run each node on its own host with the same flags, replacing 127.0.0.1 by the global's and the
# region's addresses (global_controller_host, regional_controller_host), then run control_plane_bench.py from any host
# against the global's wsapi with the same --target list. Compare MODE=standard against LEGACY=1 (the earlier build) and
# MODE=nosign / MODE=insecure (what signatures and the whole hardening cost).
