#!/usr/bin/env bash
# Runs a baseline k6 load and injects one fault part-way through, then prints reconciliation results.
#
#   ACTION=pod-kill       ./scripts/chaos-during-load.sh   # Chaos Mesh: chaos/processor-pod-kill.yaml
#   ACTION=broker-kill    ./scripts/chaos-during-load.sh   # Chaos Mesh: chaos/kafka-broker-kill.yaml
#   ACTION=db-delay       ./scripts/chaos-during-load.sh   # Chaos Mesh: chaos/db-network-delay.yaml
#   ACTION=delete-pod     ./scripts/chaos-during-load.sh   # no Chaos Mesh: oc delete one processor pod
#   ACTION=rollout        ./scripts/chaos-during-load.sh   # rolling restart of processors (deploy-time rebalance)
#   ACTION=scale          ./scripts/chaos-during-load.sh   # scale processors 3 -> 5 -> 3
#
# Env: BASE_URL, RATE (default 300), DURATION (default 10m), FAULT_AT_SEC (default 120),
#      NAMESPACE (default payments-perf), PSQL (psql connection string for reconciliation, optional)
set -euo pipefail

ACTION=${ACTION:?set ACTION}
NAMESPACE=${NAMESPACE:-payments-perf}
RATE=${RATE:-300}
DURATION=${DURATION:-10m}
FAULT_AT_SEC=${FAULT_AT_SEC:-120}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
mkdir -p "$ROOT/results"

echo ">> starting baseline load: RATE=$RATE DURATION=$DURATION"
(cd "$ROOT" && k6 run -e SCENARIO=baseline -e RATE="$RATE" -e DURATION="$DURATION" k6/scenarios.js) &
K6_PID=$!

sleep "$FAULT_AT_SEC"
echo ">> injecting fault: $ACTION at t=${FAULT_AT_SEC}s"
case "$ACTION" in
  pod-kill)    oc apply -f "$ROOT/chaos/processor-pod-kill.yaml" ;;
  broker-kill) oc apply -f "$ROOT/chaos/kafka-broker-kill.yaml" ;;
  db-delay)    oc apply -f "$ROOT/chaos/db-network-delay.yaml" ;;
  delete-pod)
    POD=$(oc -n "$NAMESPACE" get pods -l app=kafka-rr-processor -o jsonpath='{.items[0].metadata.name}')
    oc -n "$NAMESPACE" delete pod "$POD" --wait=false ;;
  rollout)     oc -n "$NAMESPACE" rollout restart deployment/kafka-rr-processor ;;
  scale)
    oc -n "$NAMESPACE" scale deployment/kafka-rr-processor --replicas=5
    sleep 120
    oc -n "$NAMESPACE" scale deployment/kafka-rr-processor --replicas=3 ;;
  *) echo "unknown ACTION=$ACTION" >&2; kill "$K6_PID"; exit 2 ;;
esac

set +e
wait "$K6_PID"
K6_RC=$?
set -e

# Clean up Chaos Mesh objects so the next run starts clean.
for f in processor-pod-kill kafka-broker-kill db-network-delay; do
  oc -n "$NAMESPACE" delete -f "$ROOT/chaos/$f.yaml" --ignore-not-found >/dev/null 2>&1 || true
done

if [[ -n "${PSQL:-}" ]]; then
  echo ">> reconciliation"
  psql "$PSQL" -f "$ROOT/sql/reconcile.sql"
fi

echo ">> k6 exit code: $K6_RC (non-zero = SLA thresholds breached)"
exit "$K6_RC"
