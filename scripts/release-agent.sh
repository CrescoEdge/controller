#!/usr/bin/env bash
#
# Build the controller bundle, embed it in a Cresco agent checkout, build the agent, and (only with
# --publish) upload the agent jar to the CrescoEdge/agent release. Runs by hand on a workstation or
# the DGX: no GitHub Actions, no eval.
#
#   scripts/release-agent.sh [options] <agent-checkout>
#
#   --offline        pass -o to Maven (use the local ~/.m2 only)
#   --skip-tests     skip the controller test suite (the agent is always built with tests skipped)
#   --publish        upload <agent>/target/agent-<version>.jar to the release (gh release upload --clobber)
#   --repo OWNER/NAME   release repository        (default CrescoEdge/agent)
#   --tag TAG           release tag               (default <version>, e.g. 1.3-SNAPSHOT)
#   -h, --help
#
# The agent's prebuild.sh is NOT run: it re-downloads every component from the Maven snapshot
# repository and would overwrite the controller.jar this script just embedded. The embedded
# controller.jar is left uncommitted in the agent checkout; commit it there when it is what you ship.
set -euo pipefail

usage() { sed -n '3,19p' "$0" | sed 's/^# \{0,1\}//'; }

OFFLINE=()
SKIP_TESTS=()
PUBLISH=0
REPO="CrescoEdge/agent"
TAG=""
AGENT_DIR=""

while [ $# -gt 0 ]; do
  case "$1" in
    --offline) OFFLINE=(-o) ;;
    --skip-tests) SKIP_TESTS=(-DskipTests) ;;
    --publish) PUBLISH=1 ;;
    --repo) shift; REPO="${1:?--repo needs OWNER/NAME}" ;;
    --tag) shift; TAG="${1:?--tag needs a tag}" ;;
    -h|--help) usage; exit 0 ;;
    -*) echo "release-agent: unknown option $1" >&2; usage >&2; exit 2 ;;
    *) [ -z "$AGENT_DIR" ] || { echo "release-agent: one agent checkout only" >&2; exit 2; }
       AGENT_DIR="$1" ;;
  esac
  shift
done
[ -n "$AGENT_DIR" ] || { usage >&2; exit 2; }

say() { printf 'release-agent: %s\n' "$*"; }
die() { printf 'release-agent: ERROR: %s\n' "$*" >&2; exit 1; }

sha256() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1
  else shasum -a 256 "$1" | cut -d' ' -f1; fi
}

CONTROLLER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
AGENT_DIR="$(cd "$AGENT_DIR" 2>/dev/null && pwd)" || die "agent checkout not found"

command -v mvn >/dev/null || die "mvn not on PATH"
command -v java >/dev/null || die "java not on PATH"
command -v unzip >/dev/null || die "unzip not on PATH"
JAVA_MAJOR="$(java -version 2>&1 | sed -n 's/.*version "\([0-9]*\).*/\1/p' | head -1)"
[ "${JAVA_MAJOR:-0}" -ge 21 ] || die "JDK 21+ required (java -version reports ${JAVA_MAJOR:-unknown})"
if [ "$PUBLISH" -eq 1 ]; then
  command -v gh >/dev/null || die "--publish needs the gh CLI"
fi

grep -q '<artifactId>controller</artifactId>' "$CONTROLLER_DIR/pom.xml" || die "$CONTROLLER_DIR is not the controller repo"
grep -q '<artifactId>agent</artifactId>' "$AGENT_DIR/pom.xml" 2>/dev/null || die "$AGENT_DIR is not an agent checkout (no agent pom.xml)"
[ -d "$AGENT_DIR/src/main/resources" ] || die "$AGENT_DIR/src/main/resources missing"

# project version: the controller inherits it from cresco-parent (no help:evaluate, so it works offline)
VERSION="$(sed -n '/<parent>/,/<\/parent>/s:.*<version>\(.*\)</version>.*:\1:p' "$CONTROLLER_DIR/pom.xml" | head -1)"
[ -n "$VERSION" ] || die "cannot read the version from $CONTROLLER_DIR/pom.xml"
[ -n "$TAG" ] || TAG="$VERSION"

# 1. controller bundle (jacoco breaks on JDK 21 classes, so it is always skipped)
say "building controller $VERSION in $CONTROLLER_DIR"
(cd "$CONTROLLER_DIR" && mvn ${OFFLINE[@]+"${OFFLINE[@]}"} -q package bundle:bundle -Djacoco.skip=true ${SKIP_TESTS[@]+"${SKIP_TESTS[@]}"})
CONTROLLER_JAR="$CONTROLLER_DIR/target/controller-$VERSION.jar"
[ -f "$CONTROLLER_JAR" ] || die "$CONTROLLER_JAR was not built"
unzip -p "$CONTROLLER_JAR" META-INF/MANIFEST.MF | grep -q '^Bundle-SymbolicName:' \
  || die "$CONTROLLER_JAR is not an OSGi bundle (bundle:bundle did not run)"
CONTROLLER_SHA="$(sha256 "$CONTROLLER_JAR")"
say "controller bundle $CONTROLLER_JAR sha256 $CONTROLLER_SHA"

# 2. embed it in the agent
cp "$CONTROLLER_JAR" "$AGENT_DIR/src/main/resources/controller.jar"
say "embedded as $AGENT_DIR/src/main/resources/controller.jar"

# 3. agent
say "building agent in $AGENT_DIR"
(cd "$AGENT_DIR" && mvn ${OFFLINE[@]+"${OFFLINE[@]}"} -q package -Dmaven.test.skip=true)
AGENT_JAR="$AGENT_DIR/target/agent-$VERSION.jar"
[ -f "$AGENT_JAR" ] || die "$AGENT_JAR was not built"
EMBEDDED_SHA="$(unzip -p "$AGENT_JAR" controller.jar | { if command -v sha256sum >/dev/null 2>&1; then sha256sum; else shasum -a 256; fi; } | cut -d' ' -f1)"
[ "$EMBEDDED_SHA" = "$CONTROLLER_SHA" ] || die "the agent jar does not carry the controller just built ($EMBEDDED_SHA != $CONTROLLER_SHA)"
AGENT_SHA="$(sha256 "$AGENT_JAR")"
say "agent jar $AGENT_JAR sha256 $AGENT_SHA (carries controller $CONTROLLER_SHA)"

# 4. publish (only when asked)
if [ "$PUBLISH" -eq 1 ]; then
  gh release view "$TAG" --repo "$REPO" >/dev/null 2>&1 || die "release $TAG not found in $REPO (this script uploads to an existing release; it does not create one)"
  say "uploading $(basename "$AGENT_JAR") to $REPO release $TAG"
  gh release upload "$TAG" "$AGENT_JAR" --repo "$REPO" --clobber
  say "published: https://github.com/$REPO/releases/tag/$TAG"
else
  say "not published (pass --publish to upload to $REPO release $TAG)"
fi
say "agent checkout now has an uncommitted controller.jar: git -C $AGENT_DIR status"
