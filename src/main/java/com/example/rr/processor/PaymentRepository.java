package com.example.rr.processor;

import com.example.rr.model.PaymentCommand;
import java.util.List;
import java.util.Optional;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@Profile("processor")
public class PaymentRepository {

    private final JdbcTemplate jdbc;

    public PaymentRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Transaction-scoped guards (SET LOCAL resets at commit/rollback). Values are ints from config, not user input. */
    public void applyTimeouts(int lockTimeoutMs, int statementTimeoutMs) {
        jdbc.execute("SET LOCAL lock_timeout = '" + lockTimeoutMs + "ms'");
        jdbc.execute("SET LOCAL statement_timeout = '" + statementTimeoutMs + "ms'");
    }

    /**
     * Idempotency claim. If a concurrent transaction holds the same requestId uncommitted, Postgres makes
     * this wait on the unique index and then returns 0, so a duplicate never double-applies.
     *
     * @return true if this transaction owns the request (first time seen)
     */
    public boolean claimRequest(String requestId, String paymentId) {
        return jdbc.update("""
                INSERT INTO processed_requests (request_id, payment_id, processed_at)
                VALUES (?, ?, now())
                ON CONFLICT (request_id) DO NOTHING
                """, requestId, paymentId) == 1;
    }

    /** Insert-or-update in one round trip; returns the new row version. */
    public long upsertPayment(PaymentCommand c) {
        Long version = jdbc.queryForObject("""
                INSERT INTO payments (payment_id, amount, currency, status, version, last_request_id, updated_at)
                VALUES (?, ?, ?, 'ACCEPTED', 1, ?, now())
                ON CONFLICT (payment_id) DO UPDATE
                   SET amount          = EXCLUDED.amount,
                       currency        = EXCLUDED.currency,
                       status          = EXCLUDED.status,
                       version         = payments.version + 1,
                       last_request_id = EXCLUDED.last_request_id,
                       updated_at      = now()
                RETURNING version
                """, Long.class, c.paymentId(), c.amount(), c.currency(), c.requestId());
        if (version == null) {
            throw new IllegalStateException("upsert returned no version for " + c.paymentId());
        }
        return version;
    }

    public void recordResultVersion(String requestId, long version) {
        jdbc.update("UPDATE processed_requests SET result_version = ? WHERE request_id = ?", version, requestId);
    }

    public Optional<Long> findResultVersion(String requestId) {
        List<Long> rows = jdbc.queryForList(
                "SELECT result_version FROM processed_requests WHERE request_id = ?", Long.class, requestId);
        return rows.isEmpty() ? Optional.empty() : Optional.ofNullable(rows.get(0));
    }
}
