-- Run on BOTH main and shadow after GoldenGate reports LOGEND=YES / Replicat at EOF (runbook step 5).
-- The outputs must be identical, row for row. Any difference = do not repoint yet.
SELECT topic, partition_id, next_offset
  FROM consumer_offsets
 ORDER BY topic, partition_id;

SELECT count(*) AS ledger_rows, max(processed_at) AS last_processed
  FROM processed_requests;
