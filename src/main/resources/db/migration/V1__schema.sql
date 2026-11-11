-- Oracle schema. All four tables must be in the SAME GoldenGate Extract/Replicat path (see docs/db-switchover.md).
-- Natural keys only: no sequences/identity columns, so nothing to resynchronise at switchover.

-- Business table: one row per payment, updated in place.
CREATE TABLE payments (
    payment_id       VARCHAR2(64)  NOT NULL,
    amount           NUMBER(19, 4) NOT NULL,
    currency         CHAR(3)       NOT NULL,
    status           VARCHAR2(16)  NOT NULL,
    version          NUMBER(19)    NOT NULL,
    last_request_id  VARCHAR2(64)  NOT NULL,
    updated_at       TIMESTAMP     NOT NULL,
    CONSTRAINT pk_payments PRIMARY KEY (payment_id)
);

-- Idempotency ledger: the PK on request_id is what makes redelivery and replay safe.
-- Grows without bound -> purge rows older than the max replay window.
CREATE TABLE processed_requests (
    request_id      VARCHAR2(64)  NOT NULL,
    payment_id      VARCHAR2(64)  NOT NULL,
    result_version  NUMBER(19),
    processed_at    TIMESTAMP     NOT NULL,
    CONSTRAINT pk_processed_requests PRIMARY KEY (request_id)
);
CREATE INDEX ix_processed_requests_at ON processed_requests (processed_at);

-- Consumer progress, written in the SAME transaction as the business write.
-- After a DB switch, consumers resume from what THIS database has, not from Kafka's committed offset.
CREATE TABLE consumer_offsets (
    topic         VARCHAR2(249) NOT NULL,
    partition_id  NUMBER(10)    NOT NULL,
    next_offset   NUMBER(19)    NOT NULL,
    updated_at    TIMESTAMP     NOT NULL,
    CONSTRAINT pk_consumer_offsets PRIMARY KEY (topic, partition_id)
);

-- Test-harness audit of what the caller was told (app.audit.enabled=true). Not required on the shadow.
CREATE TABLE requestor_audit (
    request_id   VARCHAR2(64)  NOT NULL,
    payment_id   VARCHAR2(64)  NOT NULL,
    outcome      VARCHAR2(16)  NOT NULL,
    latency_ms   NUMBER(19)    NOT NULL,
    instance_id  VARCHAR2(64)  NOT NULL,
    recorded_at  TIMESTAMP     DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT pk_requestor_audit PRIMARY KEY (request_id)
);
CREATE INDEX ix_requestor_audit_outcome ON requestor_audit (outcome);
