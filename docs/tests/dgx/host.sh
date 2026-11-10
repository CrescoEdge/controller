#!/usr/bin/env bash
# One host of the DGX gate (gate.sh runs it with srun --overlap on every node of the job).
#
#   host.sh run <rundir> <host> <jar>    run the JVMs gate.sh asks of this host, until it says stop
#   host.sh stats <rundir> <name>        one JSON line: CPU seconds, RSS, threads, heap of a JVM running on this host
#   host.sh jfr <rundir> <name> start|dump <file>   start, or write and stop, a JFR recording of a JVM on this host
#
# run: every spec file <rundir>/spawn/<host>.<n> (lines "name|region|xmx|flags") is started once, in order, each JVM in a
# node-local directory (/tmp, removed at the end), its output in <rundir>/logs/<name>.log; secrets come from
# <rundir>/secrets (environment, as an operator's service would set them). <rundir>/stop (or stop.<host>) stops this
# host's JVMs (TERM, then KILL) and exits. Only this host's own JVMs are ever signalled (their PIDs are in <rundir>/pids/<host>).
set -uo pipefail
J="${GATE_JAVA:-$HOME/cresco/jdk/bin/java}"
cmd="$1"; RD="$2"
case "$cmd" in
run)
  H="$3"; JAR="$4"
  LOCAL="/tmp/cresco-gate-$(id -un)-${SLURM_JOB_ID:-x}/$(basename "$RD")"
  mkdir -p "$LOCAL" "$RD/pids" "$RD/logs"; chmod 700 "$LOCAL"
  : > "$RD/pids/$H"
  # production: a node refuses identity files below a directory that another user than itself or root owns (on the DGX
  # /home is owned by the cluster manager's account), so each host runs from a node-local copy of the PKI and the
  # code-signing anchor, and the flags gate.sh wrote are pointed at the copy
  if [ -d "$RD/pki" ]; then
    rm -rf "$LOCAL/pki"; cp -rp "$RD/pki" "$LOCAL/pki" && { [ ! -f "$RD/codesign.pem" ] || cp -p "$RD/codesign.pem" "$LOCAL/codesign.pem"; } \
      || { echo "host $H: cannot copy the PKI to $LOCAL"; exit 1; }
  fi
  launch() { # name region xmx flags
    local d="$LOCAL/$1" f="$4"; mkdir -p "$d"
    if [ -d "$LOCAL/pki" ]; then f="${f//"$RD/pki/"/$LOCAL/pki/}"; f="${f//"$RD/codesign.pem"/$LOCAL/codesign.pem}"; fi
    (
      cd "$d" || exit 1
      if [ -f "$RD/secrets/sk" ]; then
        export CRESCO_cresco_service_key="$(cat "$RD/secrets/sk")" CRESCO_discovery_secret_global="$(cat "$RD/secrets/dg")" \
               CRESCO_discovery_secret_region="$(cat "$RD/secrets/dr")" CRESCO_discovery_secret_agent="$(cat "$RD/secrets/da")"
      fi
      # shellcheck disable=SC2086
      exec "$J" -Djava.net.preferIPv4Stack=true -Xmx"$3" -Denable_console=false -Denable_dashboard=false \
        -Dregionname="$2" -Dagentname="$1" $f -jar "$JAR"
    ) > "$RD/logs/$1.log" 2>&1 < /dev/null &
    echo "$1 $!" >> "$RD/pids/$H"
  }
  while [ ! -f "$RD/stop" ] && [ ! -f "$RD/stop.$H" ]; do
    for f in "$RD"/spawn/"$H".*; do
      [ -f "$f" ] || continue
      case "$f" in *.started|*.tmp|*.wait) continue ;; esac
      [ -f "$f.started" ] && continue
      touch "$f.started"
      while IFS='|' read -r name region xmx flags; do
        [ -n "$name" ] && launch "$name" "$region" "$xmx" "$flags"
      done < "$f"
    done
    sleep 1
  done
  while read -r _ p; do kill "$p" 2>/dev/null; done < "$RD/pids/$H"
  for _ in $(seq 1 20); do
    alive=0; while read -r _ p; do kill -0 "$p" 2>/dev/null && alive=1; done < "$RD/pids/$H"
    [ "$alive" = 0 ] && break; sleep 1
  done
  while read -r _ p; do kill -9 "$p" 2>/dev/null; done < "$RD/pids/$H"
  rm -rf "$LOCAL"
  echo "host $H stopped"
  ;;
stats)
  name="$3"; p=""
  for f in "$RD"/pids/*; do p=$(awk -v n="$name" '$1==n {print $2}' "$f"); [ -n "$p" ] && break; done
  [ -n "$p" ] && kill -0 "$p" 2>/dev/null || { echo "{\"name\": \"$name\", \"error\": \"not running\"}"; exit 0; }
  tck=$(getconf CLK_TCK)
  cpu=$(awk -v t="$tck" '{printf "%.2f", ($14+$15)/t}' "/proc/$p/stat")
  rss=$(awk '/^VmRSS/ {print $2}' "/proc/$p/status"); thr=$(awk '/^Threads/ {print $2}' "/proc/$p/status")
  heap=$("$(dirname "$J")/jcmd" "$p" GC.heap_info 2>/dev/null | grep -oE 'used [0-9]+K' | head -1 | grep -oE '[0-9]+')
  echo "{\"name\": \"$name\", \"cpu_s\": $cpu, \"rss_mb\": $(( ${rss:-0} / 1024 )), \"threads\": ${thr:-0}, \"heap_used_mb\": $(( ${heap:-0} / 1024 ))}"
  ;;
jfr)
  # host.sh jfr <rundir> <name> start | dump <file>: a JFR recording (profile settings) of a JVM running on this host
  name="$3"; p=""
  for f in "$RD"/pids/*; do p=$(awk -v n="$name" '$1==n {print $2}' "$f"); [ -n "$p" ] && break; done
  [ -n "$p" ] && kill -0 "$p" 2>/dev/null || { echo "jfr: $name is not running"; exit 1; }
  case "$4" in
  start) "$(dirname "$J")/jcmd" "$p" JFR.start name=gate settings=profile ;;
  dump) "$(dirname "$J")/jcmd" "$p" JFR.dump name=gate filename="$5" && "$(dirname "$J")/jcmd" "$p" JFR.stop name=gate ;;
  esac
  ;;
*) echo "usage: host.sh run|stats|jfr ..."; exit 2 ;;
esac
