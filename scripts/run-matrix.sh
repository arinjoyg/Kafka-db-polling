#!/usr/bin/env bash
# Runs the non-chaos load scenarios in order. Results land in results/*.json.
# Use a production-like environment: same partition counts, RF, min.insync.replicas, DB size & indexes.
#
#   BASE_URL=http://requestor.perf:8080 RATE=300 MAX_RATE=3000 ./scripts/run-matrix.sh
set -uo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
mkdir -p results
RATE=${RATE:-300}
MAX_RATE=${MAX_RATE:-3000}
declare -A RC

run() {
  local name=$1; shift
  echo; echo "================ $name ================"
  k6 run "$@" k6/scenarios.js
  RC[$name]=$?
}

run "1-baseline"         -e SCENARIO=baseline -e RATE="$RATE" -e DURATION=10m
run "2-ramp-to-knee"     -e SCENARIO=ramp     -e RATE="$RATE" -e MAX_RATE="$MAX_RATE"
run "3-spike"            -e SCENARIO=spike    -e RATE="$RATE" -e SPIKE_MULT=5
run "9-hot-key"          -e SCENARIO=baseline -e RATE="$RATE" -e DURATION=5m -e HOT_KEY_FRACTION=0.4 -e HOT_KEYS=3
run "11-payload-size"    -e SCENARIO=baseline -e RATE="$RATE" -e DURATION=5m -e MAX_PADDING=200000
run "12-duplicates"      -e SCENARIO=baseline -e RATE="$RATE" -e DURATION=5m -e DUP_FRACTION=0.05

echo; echo "================ summary (0 = pass) ================"
for k in "${!RC[@]}"; do echo "$k: ${RC[$k]}"; done | sort -V
echo "Soak (4) is separate:  k6 run -e SCENARIO=soak -e RATE=$RATE -e DURATION=8h k6/scenarios.js"
echo "Chaos (5-8, 10, 14):   see scripts/chaos-during-load.sh and docs/test-plan.md"
