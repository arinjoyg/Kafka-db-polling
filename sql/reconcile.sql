-- Reconciliation queries. Run after every load / chaos run with app.audit.enabled=true.
-- Each query is tagged "-- name: <tag>" (DbLatencyLateWriteIT reads late_writes from this file).
-- Each query returns request_id as its first column.

-- name: late_writes
-- Caller was told PENDING (timeout) but the write committed. Expected to be non-zero under chaos;
-- the count must be explained, and the caller-side status-query path must resolve each of these.
SELECT a.request_id
  FROM requestor_audit a
  JOIN processed_requests p ON p.request_id = a.request_id
 WHERE a.outcome = 'TIMEOUT';

-- name: unresolved_timeouts
-- Caller was told PENDING and nothing was ever written. Should equal the processor's
-- rr.processor.expired counter (dropped as expired) for the run. Any excess = lost requests.
SELECT a.request_id
  FROM requestor_audit a
  LEFT JOIN processed_requests p ON p.request_id = a.request_id
 WHERE a.outcome = 'TIMEOUT'
   AND p.request_id IS NULL;

-- name: ok_without_write
-- Caller was told OK but there is no committed write. MUST be zero. Anything here is a correctness bug.
SELECT a.request_id
  FROM requestor_audit a
  LEFT JOIN processed_requests p ON p.request_id = a.request_id
 WHERE a.outcome IN ('OK', 'DUPLICATE')
   AND p.request_id IS NULL;

-- name: version_drift
-- Every applied request bumps the version by exactly one. If version != applied request count,
-- something wrote outside the idempotent path. MUST be zero rows.
SELECT pay.payment_id
  FROM payments pay
  JOIN (SELECT payment_id, count(*) AS applied
          FROM processed_requests
         GROUP BY payment_id) pr ON pr.payment_id = pay.payment_id
 WHERE pay.version <> pr.applied;

-- name: summary
SELECT outcome, count(*) AS requests,
       percentile_cont(0.50)  WITHIN GROUP (ORDER BY latency_ms) AS p50_ms,
       percentile_cont(0.99)  WITHIN GROUP (ORDER BY latency_ms) AS p99_ms,
       percentile_cont(0.999) WITHIN GROUP (ORDER BY latency_ms) AS p999_ms,
       max(latency_ms) AS max_ms
  FROM requestor_audit
 GROUP BY outcome
 ORDER BY outcome;
