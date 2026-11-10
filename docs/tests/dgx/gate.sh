#!/usr/bin/env bash
# The Cresco release gate on the DGX, multi-host: the SAME harness against an earlier build and a candidate, standard and
# production (FIPS). Runs inside a SLURM job of 4+ nodes (gate.sbatch); the job's first node is H1.
#
#   H1: the global (glob/gc, wsapi)        H2: a region (rgr/rc, bridged to the global)
#   H3: agent rgr/b1 behind the region     H4: agent glob/a1 on the global's broker; the benchmark clients run here
#   light agents s1..sN (no plugins) on every host, alternately on the global and on the region
#
# Per build: (1) control plane -- RPC rate and p50/p99 to the global, the region and the agent behind the region, serial
# and with 8 and 32 callers (control_plane_bench.py); (2) dataplane across hosts through wsapi with the Python client --
# a1 (H4) -> global (H1) -> bridge -> region (H2) -> b1 (H3), 1 and 4 flows, 256 KiB and 4 MiB frames, received == sent,
# whole and in order (agent repo scripts/bench/dataplane_perf.py); (3) scale -- light agents in steps (SCALE_STEPS,
# default "50 100 200") started together: time until the global lists them all while a probe keeps calling the global,
# the global's and region's CPU, heap and threads at steady state, refusals and errors in every log, and the control
# plane again at each step (per-op cost as agents grow). One JSON per build in $GATE_OUT/<build>.json; report.py compares.
#
# Usage (inside the job): gate.sh <builds...>   builds: old | std | prod
#   GATE_JAR_OLD, GATE_JAR_NEW: agent jars; GATE_OUT: results dir (shared filesystem); GATE_PY: python with pycrescolib's
#   dependencies; GATE_PYCRESCOLIB: pycrescolib checkout; GATE_TESTS: controller docs/tests; GATE_PERF: dataplane_perf.py;
#   GATE_PKI: tools/cresco-pki.sh. Ports 36000-36999 only (P0, default 36000). GATE_PHASES (default "cp dp scale") picks
#   the phases; GATE_EXTRA_FLAGS is added to every node's flags (e.g. -Dactivemq_topic_memory_limit=67108864).
#   DP_SIZES (default "262144 4194304"), DP_FLOWS (default "1 4") and DP_RUNS (default "fire:1 fire:2 fire:3 paced:1
#   paced:2") pick the dataplane lines, so one line can be run again alone. GATE_JFR_DIR: a JFR recording (profile
#   settings) of the global and the region over the dataplane lines, written to <dir>/<build>-gc.jfr and -rc.jfr.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
J="${GATE_JAVA:-$HOME/cresco/jdk/bin/java}"
PY="${GATE_PY:?}"; export PYTHONPATH="${GATE_PYCRESCOLIB:?}"
T="${GATE_TESTS:-$HERE/..}"; PERF="${GATE_PERF:?}"; PKI_TOOL="${GATE_PKI:?}"
OUT="${GATE_OUT:?}"; mkdir -p "$OUT"
P0="${P0:-36000}"; STEPS="${SCALE_STEPS:-50 100 200}"
HOSTS=( $(scontrol show hostnames "$SLURM_JOB_NODELIST") )
[ "${#HOSTS[@]}" -ge 4 ] || { echo "FATAL: the gate needs 4 hosts, the job has ${#HOSTS[@]}"; exit 2; }
H1=${HOSTS[0]}; H2=${HOSTS[1]}; H3=${HOSTS[2]}; H4=${HOSTS[3]}
GW=$((P0+480)); AW=$((P0+481)); BW=$((P0+482))
log() { echo "[$(date +%H:%M:%S)] $*"; }
on() { local h=$1; shift; srun --overlap -N1 -n1 -w "$h" --cpu-bind=none "$@"; }
fp() { openssl s_client -connect "$1:$2" </dev/null 2>/dev/null | openssl x509 -noout -fingerprint -sha256 | cut -d= -f2; }
waitport() { for _ in $(seq 1 120); do (echo > "/dev/tcp/$1/$2") 2>/dev/null && return 0; sleep 2; done; return 1; }
jq_() { "$PY" -c "import json,sys; d=json.load(sys.stdin); print($1)"; }
log "hosts: H1=$H1 H2=$H2 H3=$H3 H4=$H4 ($(nproc) cpus here)"

build() { # name
  local B=$1 JAR ID_G ID_R ID_A1 ID_B1 TEN
  local RD="$OUT/$B"; rm -rf "$RD"; mkdir -p "$RD/spawn" "$RD/logs"; chmod 700 "$RD"
  local R="$RD/result.json"; echo "{}" > "$R"
  put() { "$PY" - "$R" "$1" "$2" <<'EOF'
import json, sys
p, k, v = sys.argv[1], sys.argv[2], sys.argv[3]
d = json.load(open(p))
try:
    v = json.loads(v)
except ValueError:
    pass
d[k] = v
json.dump(d, open(p, "w"), indent=1)
EOF
  }
  case $B in old) JAR="$GATE_JAR_OLD" ;; *) JAR="$GATE_JAR_NEW" ;; esac
  put build "\"$B\""; put jar "\"$(basename "$JAR") $(sha256sum "$JAR" | cut -c1-12)\""
  local KEY="" ; ident() { echo ""; }
  if [ "$B" = prod ]; then
    local PKI="$RD/pki"
    { bash "$PKI_TOOL" init "$PKI" --days 5 --key ec && bash "$PKI_TOOL" region "$PKI" t1 rgr --days 5 \
      && bash "$PKI_TOOL" node "$PKI" t1 glob gc --global --days 3 && bash "$PKI_TOOL" node "$PKI" t1 rgr rc --controller --days 3 \
      && bash "$PKI_TOOL" node "$PKI" t1 glob a1 --days 3 && bash "$PKI_TOOL" node "$PKI" t1 rgr b1 --days 3 \
      && bash "$PKI_TOOL" api "$PKI" t1 bench admin --days 3; } > "$RD/pki.log" 2>&1 || { log "FATAL pki"; return 1; }
    local top=0; for s in $STEPS; do top=$s; done
    [[ " ${GATE_PHASES:-cp dp scale} " == *" scale "* ]] || top=0
    for i in $(seq 1 "$top"); do
      if [ $((i % 2)) = 1 ]; then bash "$PKI_TOOL" node "$PKI" t1 glob "s$i" --days 3; else bash "$PKI_TOOL" node "$PKI" t1 rgr "s$i" --days 3; fi
    done >> "$RD/pki.log" 2>&1 || { log "FATAL pki agents"; return 1; }
    bash "$PKI_TOOL" crl "$PKI" >> "$RD/pki.log" 2>&1
    # FIPS approved-only keystores are BCFKS, converted with the candidate's own tool, 16 at a time
    local n c=0
    for n in "$PKI"/nodes/*/; do
      "$J" -jar "$JAR" --crypto convert-keystore "${n}node.p12" "${n}node.p12.pass" "${n}node.bcfks" >/dev/null 2>&1 &
      c=$((c + 1)); [ $((c % 16)) = 0 ] && wait
    done; wait
    for n in "$PKI"/nodes/*/; do [ -f "${n}node.bcfks" ] || { log "FATAL keystore conversion: $n"; return 1; }; done
    openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-384 -sha384 -days 3 -nodes -keyout "$RD/codesign.key" -out "$RD/codesign.pem" \
      -subj "/O=t1/CN=gate code signing" -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" >/dev/null 2>&1
    export CRESCO_TLS_CLIENT_CERT="$PKI/api/t1_bench/api.p12"
    CRESCO_TLS_CLIENT_KEY_PASSWORD="$(cat "$PKI/api/t1_bench/api.p12.pass")"; export CRESCO_TLS_CLIENT_KEY_PASSWORD
    ident() { # region agent [wsapi]
      local n="$PKI/nodes/$1_$2"
      echo "-Dsecurity_profile=production -Dtenant_id=t1 -Didentity_keystore=$n/node.bcfks -Didentity_keystore_password_file=$n/node.p12.pass -Dtrust_anchors=$n/trust-anchors.pem -Drevocation_file=$PKI/revocations.pem -Dplugin_signing_truststore=$RD/codesign.pem${3:+ -Dwsapi_client_ca=$n/trust-anchors.pem}"
    }
  else
    unset CRESCO_TLS_CLIENT_CERT CRESCO_TLS_CLIENT_KEY_PASSWORD
    mkdir -p "$RD/secrets"; chmod 700 "$RD/secrets"
    for s in sk dg dr da; do openssl rand -hex 32 > "$RD/secrets/$s"; done; chmod 600 "$RD/secrets/"*
    KEY="$(cat "$RD/secrets/sk")"
    [ "$B" = std ] && ident() { echo "-Dtenant_id=t1"; }
  fi
  local LOGQ="-Droot_log_level=INFO ${GATE_EXTRA_FLAGS:-}"
  echo "gc|glob|${GLOBAL_XMX:-4g}|-Dis_global=true -Dnetdiscoveryport=$((P0+5)) -Dbroker_port=$((P0+10)) -Ddiscovery_port=$((P0+10)) -Denable_wsapi=true -Dwsapi_port=$GW $LOGQ $(ident glob gc w)" > "$RD/spawn/$H1.0"
  echo "rc|rgr|${REGION_XMX:-3g}|-Dis_region=true -Dglobal_controller_host=$H1 -Dglobal_controller_port=$((P0+5)) -Dnetdiscoveryport=$((P0+15)) -Dbroker_port=$((P0+20)) -Ddiscovery_port=$((P0+20)) -Ddiscovery_port_remote=$((P0+10)) -Denable_wsapi=false $LOGQ $(ident rgr rc)" > "$RD/spawn/$H2.0.wait"
  echo "a1|glob|2g|-Dis_agent=true -Dregional_controller_host=$H1 -Dregional_controller_port=$((P0+5)) -Dnetdiscoveryport=$((P0+25)) -Denable_wsapi=true -Dwsapi_port=$AW $LOGQ $(ident glob a1 w)" > "$RD/spawn/$H4.0.wait"
  echo "b1|rgr|2g|-Dis_agent=true -Dregional_controller_host=$H2 -Dregional_controller_port=$((P0+15)) -Dnetdiscoveryport=$((P0+35)) -Denable_wsapi=true -Dwsapi_port=$BW $LOGQ $(ident rgr b1 w)" > "$RD/spawn/$H3.0.wait"
  local h; for h in "$H1" "$H2" "$H3" "$H4"; do
    on "$h" bash "$HERE/host.sh" run "$RD" "$h" "$JAR" > "$RD/host-$h.out" 2>&1 &
  done
  waitport "$H1" "$GW" || { log "$B: the global did not come up"; touch "$RD/stop"; wait; return 1; }
  mv "$RD/spawn/$H2.0.wait" "$RD/spawn/$H2.0"
  waitport "$H2" $((P0+20)) || { log "$B: the region did not come up"; touch "$RD/stop"; wait; return 1; }
  sleep 10
  mv "$RD/spawn/$H4.0.wait" "$RD/spawn/$H4.0"; mv "$RD/spawn/$H3.0.wait" "$RD/spawn/$H3.0"
  local GP; GP=$(fp "$H1" "$GW")
  local -a CL=(--host "$H1" --port "$GW" --key "$KEY" --pin "$GP")
  # ask the nodes only once the global lists both agents and the trust bundle has had two ping rounds to travel (an RPC
  # before then is refused, fail closed: see control_plane_bench.sh)
  for _ in $(seq 1 90); do "$PY" "$HERE/gate_probe.py" count "${CL[@]}" --require glob/a1 --require rgr/b1 >/dev/null 2>&1 && break; sleep 4; done
  sleep "${TRUST_SETTLE_S:-15}"
  local ok=0
  for _ in $(seq 1 90); do "$PY" "$HERE/gate_probe.py" ready "${CL[@]}" --target glob/gc --target rgr/rc --target glob/a1 --target rgr/b1 >/dev/null 2>&1 && { ok=1; break; }; sleep 4; done
  [ "$ok" = 1 ] || { log "$B: the fabric did not answer"; touch "$RD/stop"; wait; return 1; }
  waitport "$H4" "$AW"; waitport "$H3" "$BW"; sleep 15
  log "$B: fabric up"

  local PH=" ${GATE_PHASES:-cp dp scale} "
  # ---- 1. control plane, 0 light agents
  if [[ "$PH" == *" cp "* ]]; then
  local cp; cp=$(on "$H4" "$PY" "$T/control_plane_bench.py" "${CL[@]}" --n "${CP_N:-300}" --threads 8,32 --seconds "${CP_SECONDS:-15}" \
      --target global=glob/gc --target region=rgr/rc --target agent=rgr/b1 2>/dev/null | tail -1)
  put control_plane "${cp:-null}"; log "$B: control plane $cp"
  fi

  # ---- 2. dataplane across hosts (a1 on H4 -> b1 on H3), Python client through wsapi
  if [[ "$PH" == *" dp "* ]]; then
  local AP BP; AP=$(fp "$H4" "$AW"); BP=$(fp "$H3" "$BW")
  # warm-up (JIT, connections, TLS sessions): one untimed run first
  on "$H4" "$PY" "$PERF" --host 127.0.0.1 --port $AW --fingerprint "$AP" --rx-host "$H3" --rx-port $BW --rx-fingerprint "$BP" \
      ${KEY:+--key $KEY} --messages 500 --size 262144 --settle 120 --stream "gate-$B-warmup" >/dev/null 2>&1
  local dp="{" first=1 size flows rep
  if [ -n "${GATE_JFR_DIR:-}" ]; then on "$H1" bash "$HERE/host.sh" jfr "$RD" gc start; on "$H2" bash "$HERE/host.sh" jfr "$RD" rc start; fi
  for size in ${DP_SIZES:-262144 4194304}; do
    local msgs=$([ "$size" = 262144 ] && echo "${DP_MSGS_256K:-2000}" || echo "${DP_MSGS_4M:-150}")
    for flows in ${DP_FLOWS:-1 4}; do
      # fire: as fast as the reader takes it (throughput, loss); paced: a fixed offered load per flow (latency that is
      # the pipeline's, not the depth of the queue a faster writer builds ahead of its reader)
      local runs="${DP_RUNS:-fire:1 fire:2 fire:3 paced:1 paced:2}" run mode rate n
      for run in $runs; do
        mode=${run%:*}; rep=${run#*:}; rate=0; n=$msgs
        if [ "$mode" = paced ]; then rate="${DP_RATE_MIB_S:-100}"; n=$(( rate * 3 * 1048576 / size )); fi
        local at; at=$(( $(date +%s) + 8 ))
        local res; res=$(on "$H4" bash -c "
          for f in \$(seq 1 $flows); do
            '$PY' '$PERF' --host 127.0.0.1 --port $AW --fingerprint '$AP' --rx-host $H3 --rx-port $BW --rx-fingerprint '$BP' \
              ${KEY:+--key $KEY} \
              --messages $n --size $size --rate-mib-s $rate --settle ${DP_SETTLE_S:-90} --start-at $at --stream gate-$B-$size-$flows-$mode-$rep-\$f 2>/dev/null | grep '^BENCH' &
          done; wait" | sed 's/^BENCH //' | "$PY" -c '
import json, sys
rs = [json.loads(l) for l in sys.stdin if l.strip()]
if not rs: print("null"); sys.exit()
first = min(r["first_rx"] for r in rs if r["first_rx"]); last = max(r["last_rx"] for r in rs if r["last_rx"])
tot = sum(r["recv_bytes"] for r in rs)
print(json.dumps({"flows": len(rs), "sent": sum(r["messages"] for r in rs), "received": sum(r["received"] for r in rs),
  "missing": sum(r["missing"] for r in rs), "dups": sum(r["dups"] for r in rs), "out_of_order": sum(r["out_of_order"] for r in rs),
  "bad_size": sum(r["bad_size"] for r in rs), "mib_s": round(tot / 1048576 / max(1e-6, last - first), 1),
  "p50_ms": sorted(r["p50_ms"] for r in rs)[len(rs) // 2], "p99_ms": max(r["p99_ms"] for r in rs)}))')
        [ $first = 1 ] || dp="$dp,"; first=0
        dp="$dp\"${size}_${flows}f_${mode}_r$rep\": ${res:-null}"
        log "$B: dataplane size=$size flows=$flows $mode rep=$rep $res"
      done
    done
  done
  put dataplane "$dp}"
  if [ -n "${GATE_JFR_DIR:-}" ]; then
    on "$H1" bash "$HERE/host.sh" jfr "$RD" gc dump "$GATE_JFR_DIR/$B-gc.jfr"; on "$H2" bash "$HERE/host.sh" jfr "$RD" rc dump "$GATE_JFR_DIR/$B-rc.jfr"
  fi
  fi

  # ---- 3. scale: light agents in steps, all hosts, alternately on the global and the region
  local steps_run="$STEPS"; [[ "$PH" == *" scale "* ]] || steps_run=""
  local LIGHT="${GATE_EXTRA_FLAGS:-} -Denable_stunnel=false -Denable_sysinfo=false -Denable_executor=false -Denable_filerepo=false -Denable_repo=false -Denable_wsapi=false -Droot_log_level=WARN -XX:+UseSerialGC -XX:TieredStopAtLevel=1"
  local prev=0 step sc="{" sfirst=1 i k
  local -a HS=("$H1" "$H2" "$H3" "$H4")
  for step in $steps_run; do
    for k in 0 1 2 3; do : > "$RD/spawn/${HS[$k]}.s$step.tmp"; done
    for i in $(seq $((prev + 1)) "$step"); do
      local reg host cp_port; host=${HS[$(( (i - 1) % 4 ))]}
      if [ $((i % 2)) = 1 ]; then reg=glob; cp_port=$((P0+5)); ch=$H1; else reg=rgr; cp_port=$((P0+15)); ch=$H2; fi
      echo "s$i|$reg|${LIGHT_XMX:-192m}|-Dis_agent=true -Dregional_controller_host=$ch -Dregional_controller_port=$cp_port -Dnetdiscoveryport=$((P0+100+i)) $LIGHT $(ident $reg s$i)" >> "$RD/spawn/$host.s$step.tmp"
    done
    local g0 r0; g0=$(on "$H1" bash "$HERE/host.sh" stats "$RD" gc); r0=$(on "$H2" bash "$HERE/host.sh" stats "$RD" rc)
    rm -f "$RD/storm.stop"
    on "$H4" "$PY" "$HERE/gate_probe.py" storm "${CL[@]}" --target glob/gc --until "$RD/storm.stop" > "$RD/storm-$step.json" 2>/dev/null &
    local sp=$!; sleep 3
    local t0; t0=$(date +%s.%N)
    for k in 0 1 2 3; do [ -f "$RD/spawn/${HS[$k]}.s$step.tmp" ] && mv "$RD/spawn/${HS[$k]}.s$step.tmp" "$RD/spawn/${HS[$k]}.s$step"; done
    local listed=0 t_all=null
    for _ in $(seq 1 300); do
      listed=$("$PY" "$HERE/gate_probe.py" count "${CL[@]}" --prefix s 2>/dev/null | jq_ 'd.get("listed", 0)' 2>/dev/null || echo 0)
      [ "${listed:-0}" -ge "$step" ] && { t_all=$("$PY" -c "import time; print(round(time.time() - $t0, 1))"); break; }
      sleep 3
    done
    sleep 10; touch "$RD/storm.stop"; wait "$sp"
    local storm; storm=$(cat "$RD/storm-$step.json" 2>/dev/null)
    local g1 r1; g1=$(on "$H1" bash "$HERE/host.sh" stats "$RD" gc); r1=$(on "$H2" bash "$HERE/host.sh" stats "$RD" rc)
    sleep "${STEADY_S:-60}"
    local g2 r2; g2=$(on "$H1" bash "$HERE/host.sh" stats "$RD" gc); r2=$(on "$H2" bash "$HERE/host.sh" stats "$RD" rc)
    local cps; cps=$(on "$H4" "$PY" "$T/control_plane_bench.py" "${CL[@]}" --n 200 --threads 8 --seconds 10 \
        --target global=glob/gc --target agent=rgr/b1 2>/dev/null | tail -1)
    local s; s=$("$PY" - "$step" "$listed" "$t_all" "$g0" "$g1" "$g2" "$r0" "$r1" "$r2" "${STEADY_S:-60}" "${storm:-null}" "${cps:-null}" <<'EOF'
import json, sys
step, listed, t_all = int(sys.argv[1]), int(sys.argv[2] or 0), sys.argv[3]
def js(x):
    try:
        return json.loads(x)
    except ValueError:
        return {"error": "unreadable: %s" % x[:80]}
g0, g1, g2, r0, r1, r2 = (js(x) for x in sys.argv[4:10])
steady = float(sys.argv[10])
def node(a, b, c):
    try:
        return {"cpu_pct_steady": round((c["cpu_s"] - b["cpu_s"]) * 100.0 / steady, 1), "cpu_s_storm": round(b["cpu_s"] - a["cpu_s"], 1),
                "heap_used_mb": c["heap_used_mb"], "rss_mb": c["rss_mb"], "threads": c["threads"]}
    except Exception as e:
        return {"error": str(e)}
print(json.dumps({"agents": step, "listed": listed, "all_listed_after_s": None if t_all == "null" else float(t_all),
                  "global": node(g0, g1, g2), "region": node(r0, r1, r2), "global_probe_during_storm": js(sys.argv[11]),
                  "control_plane": js(sys.argv[12])}))
EOF
)
    [ $sfirst = 1 ] || sc="$sc,"; sfirst=0; sc="$sc\"$step\": $s"
    log "$B: scale $s"
    prev=$step
  done
  put scale "$sc}"

  # ---- errors and refusals in every log
  local e; e=$("$PY" - "$RD/logs" <<'EOF'
import json, os, re, sys
d = sys.argv[1]; errs = {}; refusals = {}; lines = {}
pat_r = re.compile(r"REFUS|does not verify|SIGNATURE: REFUSED|Discovery REFUSED|DISCARDED|overloaded|dropped [0-9]+ frame")
for f in sorted(os.listdir(d)):
    n = f[:-4]
    for l in open(os.path.join(d, f), errors="replace"):
        if " ERROR " in l:
            errs[n] = errs.get(n, 0) + 1
            k = re.sub(r"^\S+ \S+ \S+ +ERROR \[[^]]*\] - ", "", l.strip())[:160]
            lines[k] = lines.get(k, 0) + 1
        if pat_r.search(l):
            refusals[n] = refusals.get(n, 0) + 1
            k = "REFUSAL/LOSS " + re.sub(r"^\S+ \S+ \S+ +\w+ \[[^]]*\] - ", "", l.strip())[:160]
            lines[k] = lines.get(k, 0) + 1
print(json.dumps({"error_lines": sum(errs.values()), "refusal_or_loss_lines": sum(refusals.values()), "by_node_errors": errs,
                  "by_node_refusals": refusals, "top": sorted(lines.items(), key=lambda x: -x[1])[:15]}))
EOF
)
  put logs "$e"
  touch "$RD/stop"; wait
  log "$B: done -> $R"
  cp "$R" "$OUT/$B.json"
}

for b in "$@"; do build "$b"; done
"$PY" "$HERE/report.py" "$OUT" ${@} | tee "$OUT/report.txt"
