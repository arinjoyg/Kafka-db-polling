#!/usr/bin/env bash
# Planned DB switchover orchestration (see docs/db-switchover.md). App-side steps are automated; DB / GoldenGate /
# DNS steps pause for the operator, because they run in different tools and need a human sign-off.
#
#   REQUESTORS="http://req-0:8080 http://req-1:8080" PROCESSORS="http://proc-0:8081 http://proc-1:8081" \
#     ./scripts/db-switchover.sh planned
#   ... ./scripts/db-switchover.sh failover      # unplanned: adds resync
#
# On OpenShift, build the pod lists with e.g.:
#   oc get pods -l app=kafka-rr-processor -o jsonpath='{range .items[*]}http://{.status.podIP}:8081 {end}'
set -euo pipefail

MODE=${1:?usage: db-switchover.sh planned|failover}
REQUESTORS=${REQUESTORS:?space-separated requestor base URLs}
PROCESSORS=${PROCESSORS:?space-separated processor base URLs}

post_all() {  # post_all "<urls>" <path>
  for u in $1; do
    echo "  POST $u$2 -> $(curl -fsS -X POST "$u$2")"
  done
}

confirm() {
  echo; echo ">>> $1"
  read -r -p "    Done and verified? [y/N] " ok
  [[ "$ok" == "y" ]] || { echo "Aborting. App is paused with intake closed; reverse with: intake/open + processing/resume"; exit 1; }
}

echo "== 1. Close intake";            post_all "$REQUESTORS" /admin/intake/close
echo "== 2. Pause processors";         post_all "$PROCESSORS" /admin/processing/pause
for u in $PROCESSORS; do
  until curl -fsS "$u/admin/processing/status" | grep -q '"paused":true'; do sleep 1; done
  echo "  $u paused"
done

if [[ "$MODE" == "planned" ]]; then
  confirm "3. Fence main: ALTER USER <app> ACCOUNT LOCK; kill remaining <app> sessions"
  confirm "4. GoldenGate drained: SEND EXTRACT <ext>, LOGEND = YES; Replicat at EOF; lag 0"
  confirm "5. sql/switchover-check.sql output identical on main and shadow"
  confirm "6. Forward Replicat stopped (reverse path started if you want fallback)"
else
  confirm "3. Main fenced or unreachable; it must not come back as a writer"
fi
confirm "7. Endpoint (DNS / TNS / service) now resolves to the shadow"

echo "== 7. Evict pools";              post_all "$REQUESTORS $PROCESSORS" /admin/db/evict-pool
if [[ "$MODE" == "failover" ]]; then
  echo "== 7b. Resync from shadow offsets (retried: a connection opened mid-repoint can still hit the fenced main)"
  for u in $PROCESSORS; do
    for attempt in 1 2 3 4 5 6 7 8 9 10; do
      if out=$(curl -fsS -X POST "$u/admin/processing/resync"); then echo "  $u -> $out"; break; fi
      echo "  $u resync failed (attempt $attempt); evicting pool and retrying"
      curl -fsS -X POST "$u/admin/db/evict-pool" >/dev/null || true
      sleep 2
      [[ $attempt == 10 ]] && { echo "Resync failed on $u; stop and investigate"; exit 1; }
    done
  done
fi
echo "== 8. Resume processors";         post_all "$PROCESSORS" /admin/processing/resume
echo "== 8. Reopen intake";             post_all "$REQUESTORS" /admin/intake/open
echo; echo "Now run sql/reconcile.sql and watch /admin/processing/status until replayWindows is empty."
