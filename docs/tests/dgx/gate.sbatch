#!/bin/bash
# The Cresco release gate on 4 DGX nodes: sbatch gate.sbatch <builds...> (old oldprod std prod). See gate.sh.
#
#   sbatch -N4 -p defq -c16 --mem=48G -t 04:00:00 -o ~/cresco-gate/gate-%j.out \
#     --export=ALL,GATE_JAR_OLD=...,GATE_JAR_NEW=...,GATE_OUT=~/cresco-gate/out-<label>,GATE_PY=...,GATE_PYCRESCOLIB=...,GATE_PERF=...,GATE_PKI=... \
#     controller/docs/tests/dgx/gate.sbatch old oldprod std prod
#
# Everything a run writes lands under GATE_OUT (shared filesystem, 0700) and node-local /tmp (removed at the end); it
# signals only its own JVMs and uses ports 36000-36999.
#SBATCH -J cresco-gate
#SBATCH -N 4
#SBATCH --ntasks-per-node=1
set -u
umask 077
D="$(cd "$(dirname "${GATE_SCRIPT:-$0}")" && pwd)"
[ -f "$D/gate.sh" ] || D="${GATE_DIR:?set GATE_DIR to the directory of gate.sh}"
bash "$D/gate.sh" "$@"
