#!/bin/bash
# The nightly fabric matrix on one DGX node (see nightly.sh). Start the chain once:
#
#   sbatch -A <account> -o ~/cresco-gate/nightly/job-%j.out --export=NIGHTLY_HOME=$HOME/cresco-gate/nightly \
#     ~/cresco-gate/nightly/scripts/nightly.sbatch
#   (one night without a next one: add ,NIGHTLY_CHAIN=0; chosen cells: ,NIGHTLY_CELLS="tree mesh")
#
# Each night submits the next one first (01:30), with the same account and scripts, so a night that fails or is killed
# does not end the chain; a second chain is refused (one pending job of this name at most). The scripts it runs are the
# copies in NIGHTLY_HOME/scripts: update them by copying nightly.sh and nightly.sbatch there.
# Results: NIGHTLY_HOME/history.txt (one line a night) and NIGHTLY_HOME/<date>/. Ports 24000-24400; signals only its own JVMs.
#SBATCH -J cresco-nightly
#SBATCH -N 1
#SBATCH -p defq
#SBATCH -c 32
#SBATCH --mem=64G
#SBATCH -t 05:00:00
set -u
umask 077
: "${USER:=$(id -un)}"; : "${HOME:=$(getent passwd "$USER" | cut -d: -f6)}"; export USER HOME
N="${NIGHTLY_HOME:-$HOME/cresco-gate/nightly}"; export NIGHTLY_HOME="$N"
S="$N/scripts"
if [ "${NIGHTLY_CHAIN:-1}" = 1 ]; then
  # a login shell finds squeue and sbatch; --export names the one variable, so the next night does not inherit this
  # job's SLURM settings or an environment that grows from night to night
  pending=$(bash -lc "squeue -h -u $USER -n cresco-nightly -t PENDING -o %i" 2>/dev/null | grep -c -E '^[0-9]+$')
  if [ "$pending" = 0 ]; then
    bash -lc "sbatch -A $SLURM_JOB_ACCOUNT --begin=$(date -d 'tomorrow 01:30' +%Y-%m-%dT%H:%M:%S) -o $N/job-%j.out --export=NIGHTLY_HOME=$N $S/nightly.sbatch" 2>&1 \
      | grep -E "Submitted|rror" || echo "nightly: the next night could NOT be submitted"
  fi
fi
find "$N" -maxdepth 1 -name 'job-*.out' -mtime +30 -delete 2>/dev/null
bash "$S/nightly.sh"
