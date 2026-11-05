#!/bin/bash
# Runs measured cases one after another: run-steps.sh PREFIX USERS...
# Source the lab env first. A case stopped by the memory-pressure guard is
# retried up to 3 times; a generator that outlives its case is stopped.
set -uo pipefail
bench=$(cd "$(dirname "$0")" && pwd)
: "${LOAD_LAB_ROOT:?source the lab env first}"
prefix=$1; shift
for U in "$@"; do
  for attempt in 1 2 3; do
    label=$prefix-$U; [ $attempt -gt 1 ] && label=$prefix-$U-r$attempt
    (LOAD_LABEL=$label LOAD_USERS=$U LOAD_RAMP_SECONDS=${LOAD_RAMP_SECONDS:-60} LOAD_SECONDS=${LOAD_SECONDS:-300} bash "$bench/run-case.sh" >"$LOAD_LAB_ROOT/$label.case.log" 2>&1) &
    case_pid=$!
    limit=$(( (${LOAD_RAMP_SECONDS:-60} + ${LOAD_SECONDS:-300}) / 5 + 120 ))
    for i in $(seq 1 $limit); do sleep 5; kill -0 $case_pid 2>/dev/null || break; done
    if kill -0 $case_pid 2>/dev/null; then docker stop -t 5 gvsq-generator >/dev/null 2>&1; fi
    wait $case_pid
    if [ -e "$LOAD_LAB_ROOT/$label/STOPPED_MEMORY_PRESSURE" ]; then
      echo "$label stopped by host memory pressure; retrying in 60 s"; sleep 60; continue
    fi
    python3 "$bench/show.py" "$LOAD_LAB_ROOT" "$label" || tail -5 "$LOAD_LAB_ROOT/$label.case.log"
    echo "oom=$(docker inspect gvsq-runtime --format '{{.State.OOMKilled}}' 2>/dev/null)"
    break
  done
done
