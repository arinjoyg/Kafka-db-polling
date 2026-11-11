-- Business table: one row per payment, updated in place (upsert).
CREATE TABLE payments (
    payment_id       VARCHAR(64)    PRIMARY KEY,
    amount           NUMERIC(19, 4) NOT NULL,
    currency         CHAR(3)        NOT NULL,
    status           VARCHAR(16)    NOT NULL,
    version          BIGINT         NOT NULL,
    last_request_id  VARCHAR(64)    NOT NULL,
    updated_at       TIMESTAMPTZ    NOT NULL
);

-- Idempotency ledger: the PK on request_id is what makes redelivery safe.
-- Grows without bound -> purge rows older than the max redelivery window (soak test checks this).
CREATE TABLE processed_requests (
    request_id      VARCHAR(64)  PRIMARY KEY,
    payment_id      VARCHAR(64)  NOT NULL,
    result_version  BIGINT,
    processed_at    TIMESTAMPTZ  NOT NULL
);
CREATE INDEX ix_processed_requests_processed_at ON processed_requests (processed_at);

-- Test-harness audit of what the caller was told (enabled with app.audit.enabled=true).
CREATE TABLE requestor_audit (
    request_id   VARCHAR(64)  PRIMARY KEY,
    payment_id   VARCHAR(64)  NOT NULL,
    outcome      VARCHAR(16)  NOT NULL,
    latency_ms   BIGINT       NOT NULL,
    instance_id  VARCHAR(64)  NOT NULL,
    recorded_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX ix_requestor_audit_outcome ON requestor_audit (outcome);
