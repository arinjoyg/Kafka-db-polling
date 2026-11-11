-- Which Kafka record first applied each request. Lets the processor tell apart:
--   * the SAME record delivered again (crash / rebalance / failover replay) -> resend the original success
--   * a NEW record with the same requestId (caller timed out and retried)    -> reject as duplicate
-- These columns must be included in the GoldenGate replication of PROCESSED_REQUESTS.
ALTER TABLE processed_requests ADD (
    source_topic      VARCHAR2(249),
    source_partition  NUMBER(10),
    source_offset     NUMBER(19)
);
