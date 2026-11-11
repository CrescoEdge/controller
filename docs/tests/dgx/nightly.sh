#!/usr/bin/env bash
# The fabric matrix every night on one DGX node, on what is on `main` that night (no GitHub Actions: the DGX builds).
#
#   1. clones library, controller, wsapi, agent and pycrescolib at the head of main (public repositories, no token);
#   2. builds the three bundles and an agent jar that embeds them (the agent's other bundles stay the committed ones);
#   3. runs every cell of docs/tests/fabric_matrix.py, one after another, each with --json;
#   4. writes <NIGHTLY_HOME>/<date>/: <cell>.json, <cell>.log, build.log, summary.txt, and appends one line to
#      <NIGHTLY_HOME>/history.txt: date, the commits, PASS or FAIL per cell. A cell that fails keeps its node logs
#      (<date>/<cell>-nodes.tgz); runs older than NIGHTLY_KEEP_DAYS (30) are removed.
#
# nightly.sbatch runs it and submits the next night first, so a failed night does not end the chain.
# By hand, inside a job or on a quiet node:  NIGHTLY_HOME=~/cresco-gate/nightly bash nightly.sh [cell ...]
#   NIGHTLY_JAR=<agent jar>  test that jar instead of building main (the matrix script is then the one beside this file)
#   NIGHTLY_CELLS            default: "tree tree:production tenants alias mesh"  (topology[:profile])
#   NIGHTLY_FLAGS            extra matrix arguments for every cell, e.g. "--flag=-Dauthority_direction=enforce"
#   NIGHTLY_PY               python with pycrescolib's dependencies (websockets); NIGHTLY_MVN, JAVA_HOME; NIGHTLY_PORT (24000)
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
HOME_N="${NIGHTLY_HOME:?set NIGHTLY_HOME (results directory)}"
export JAVA_HOME="${JAVA_HOME:-$HOME/cresco/jdk}"; export PATH="$JAVA_HOME/bin:$PATH"
MVN="${NIGHTLY_MVN:-/project/ibi-staff/LLM_TEXT/gfs-ci/tools/apache-maven-3.9.9/bin/mvn}"
PY="${NIGHTLY_PY:-/project/ibi-staff/LLM_TEXT/gfs-ci/venv/bin/python}"
PORT="${NIGHTLY_PORT:-24000}"     # below the kernel's range for outgoing connections (32768 up)
CELLS="${*:-${NIGHTLY_CELLS:-tree tree:production tenants alias mesh}}"
DAY="$(date +%Y-%m-%d)"; OUT="$HOME_N/$DAY"; [ -e "$OUT" ] && OUT="$OUT-$(date +%H%M)"
W="${NIGHTLY_WORK:-/local/cresco-nightly-${SLURM_JOB_ID:-$$}}"
umask 077; mkdir -p "$OUT" "$W" || exit 2
trap 'rm -rf "$W"' EXIT
log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$OUT/summary.txt"; }
if ! command -v unzip > /dev/null 2>&1; then
  # the DGX compute nodes have no unzip, and the agent's prebuild.sh reads a bundle's manifest with `unzip -p`
  mkdir -p "$W/bin"
  cat > "$W/bin/unzip" <<'SHIM'
#!/usr/bin/env bash
[ "${1:-}" = -p ] && [ $# = 3 ] || { echo "unzip (nightly.sh's stand-in): only -p <archive> <member>" >&2; exit 2; }
exec "$NIGHTLY_SHIM_PY" -c 'import sys, zipfile; sys.stdout.buffer.write(zipfile.ZipFile(sys.argv[1]).read(sys.argv[2]))' "$2" "$3"
SHIM
  chmod +x "$W/bin/unzip"; export NIGHTLY_SHIM_PY="$PY" PATH="$W/bin:$PATH"
fi
commits=""

if [ -n "${NIGHTLY_JAR:-}" ]; then
  JAR="$NIGHTLY_JAR"; TESTS="$HERE/.."; PYLIB="${NIGHTLY_PYCRESCOLIB:?with NIGHTLY_JAR set NIGHTLY_PYCRESCOLIB (a pycrescolib checkout)}"
  commits="jar=$(basename "$JAR"):$(sha256sum "$JAR" | cut -c1-12)"
else
  # the job's own Maven repository, kept between nights (dependencies only: what it builds is installed over each night)
  export MAVEN_ARGS="-Dmaven.repo.local=$HOME_N/m2"
  for r in library controller wsapi agent pycrescolib; do
    git clone -q --depth 1 -b main "https://github.com/CrescoEdge/$r.git" "$W/$r" >> "$OUT/build.log" 2>&1 \
      || { log "BUILD FAIL: clone of $r"; echo "$DAY build-failed clone:$r" >> "$HOME_N/history.txt"; exit 1; }
    commits="$commits $r=$(git -C "$W/$r" rev-parse --short=12 HEAD)"
  done
  commits="${commits# }"
  build() { # repository, goals...
    local r=$1; shift
    ( cd "$W/$r" && "$MVN" -q -B "$@" "-Dcresco.commit=$(git rev-parse HEAD)" ) >> "$OUT/build.log" 2>&1 \
      || { log "BUILD FAIL: $r ($*): see build.log"; echo "$DAY build-failed $r $commits" >> "$HOME_N/history.txt"; exit 1; }
  }
  build library clean install bundle:bundle -DskipTests
  build controller clean package bundle:bundle -DskipTests
  build wsapi clean package bundle:bundle -DskipTests
  for r in library controller wsapi; do
    j=$(ls "$W/$r"/target/"$r"-*.jar 2>/dev/null | grep -v -E "sources|javadoc|original" | head -1)
    [ -f "$j" ] || { log "BUILD FAIL: no bundle of $r"; echo "$DAY build-failed bundle:$r $commits" >> "$HOME_N/history.txt"; exit 1; }
    cp "$j" "$W/agent/src/main/resources/$r.jar"
  done
  ( cd "$W/agent" && ./prebuild.sh --lock library controller wsapi ) >> "$OUT/build.log" 2>&1
  build agent clean package
  JAR=$(ls "$W"/agent/target/agent-*.jar 2>/dev/null | grep -v -E "sources|javadoc|original" | head -1)
  [ -f "$JAR" ] || { log "BUILD FAIL: no agent jar"; echo "$DAY build-failed agent $commits" >> "$HOME_N/history.txt"; exit 1; }
  TESTS="$W/controller/docs/tests"; PYLIB="$W/pycrescolib"
fi
log "night of $DAY on $(hostname): $commits"

line="$DAY $commits"; bad=0
for cell in $CELLS; do
  topo=${cell%%:*}; prof=standard; [ "$cell" != "$topo" ] && prof=${cell#*:}
  name=$topo; [ "$prof" = standard ] || name="$topo-$prof"
  t0=$(date +%s)
  # shellcheck disable=SC2086
  PYTHONPATH="$PYLIB" "$PY" "$TESTS/fabric_matrix.py" --jar "$JAR" --work "$W/fm-$name" --base-port "$PORT" \
      --topology "$topo" --profile "$prof" --json "$OUT/$name.json" --keep ${NIGHTLY_FLAGS:-} > "$OUT/$name.log" 2>&1
  rc=$?
  if [ $rc = 0 ]; then res=PASS; else
    res=FAIL; bad=1
    tar -C "$W" -czf "$OUT/$name-nodes.tgz" --exclude='*.jar' --exclude='cresco-data/plugin-data' "fm-$name" 2>/dev/null
  fi
  rm -rf "$W/fm-$name"
  log "$name $res in $(( ($(date +%s) - t0) / 60 )) min$( [ $res = FAIL ] && grep -E '^FAIL  ' "$OUT/$name.log" | head -6 | cut -c1-220 | sed 's/^/\n    /' | tr -d '\r')"
  line="$line $name=$res"
done
echo "$line" >> "$HOME_N/history.txt"
find "$HOME_N" -maxdepth 1 -type d -name '20??-??-??*' -mtime +"${NIGHTLY_KEEP_DAYS:-30}" -exec rm -rf {} + 2>/dev/null
exit $bad
