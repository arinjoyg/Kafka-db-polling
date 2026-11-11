package com.example.rr.processor;

import com.example.rr.model.PaymentCommand;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@Profile("processor")
public class PaymentRepository {

    /**
     * Idempotent apply in ONE statement (one round trip):
     * <ol>
     *   <li>{@code up}: upsert the payment, but only if this requestId is not already in the ledger
     *       (checked against the statement snapshot).</li>
     *   <li>{@code claim}: record the requestId with the resulting version. NO "ON CONFLICT" here on
     *       purpose: if a concurrent duplicate committed first, this raises a unique violation and the
     *       WHOLE statement (including the payment upsert) rolls back, so a duplicate can never double-apply.</li>
     *   <li>Main SELECT: the version we applied, or (for an already-committed duplicate) the prior one.</li>
     * </ol>
     * Explicit casts because parameters in an INSERT ... SELECT list are otherwise untyped.
     */
    private static final String APPLY_SQL = """
            WITH up AS (
                INSERT INTO payments (payment_id, amount, currency, status, version, last_request_id, updated_at)
                SELECT ?::varchar, ?::numeric, ?::char(3), 'ACCEPTED', 1, ?::varchar, now()
                 WHERE NOT EXISTS (SELECT 1 FROM processed_requests WHERE request_id = ?::varchar)
                ON CONFLICT (payment_id) DO UPDATE
                   SET amount          = EXCLUDED.amount,
                       currency        = EXCLUDED.currency,
                       status          = EXCLUDED.status,
                       version         = payments.version + 1,
                       last_request_id = EXCLUDED.last_request_id,
                       updated_at      = now()
                RETURNING version
            ), claim AS (
                INSERT INTO processed_requests (request_id, payment_id, result_version, processed_at)
                SELECT ?::varchar, ?::varchar, version, now() FROM up
                RETURNING result_version
            )
            SELECT (SELECT result_version FROM claim)                                      AS applied_version,
                   (SELECT result_version FROM processed_requests WHERE request_id = ?::varchar) AS prior_version
            """;

    /** @param appliedVersion non-null when this call applied the write; priorVersion set for a committed duplicate */
    public record ApplyResult(Long appliedVersion, Long priorVersion) {}

    private final JdbcTemplate jdbc;

    public PaymentRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Transaction-scoped guards (reset at commit/rollback), sent as ONE round trip.
     * Values are ints from config, not user input.
     */
    public void applyTimeouts(int lockTimeoutMs, int statementTimeoutMs) {
        jdbc.execute("SET LOCAL lock_timeout = '" + lockTimeoutMs + "ms'; "
                + "SET LOCAL statement_timeout = '" + statementTimeoutMs + "ms'");
    }

    /** @throws org.springframework.dao.DuplicateKeyException if a concurrent duplicate won the race */
    public ApplyResult apply(PaymentCommand c) {
        return jdbc.queryForObject(APPLY_SQL,
                (rs, n) -> new ApplyResult(nullableLong(rs, "applied_version"), nullableLong(rs, "prior_version")),
                c.paymentId(), c.amount(), c.currency(), c.requestId(), c.requestId(),
                c.requestId(), c.paymentId(),
                c.requestId());
    }

    public Optional<Long> findResultVersion(String requestId) {
        List<Long> rows = jdbc.queryForList(
                "SELECT result_version FROM processed_requests WHERE request_id = ?", Long.class, requestId);
        return rows.isEmpty() ? Optional.empty() : Optional.ofNullable(rows.get(0));
    }

    private static Long nullableLong(ResultSet rs, String column) throws SQLException {
        long v = rs.getLong(column);
        return rs.wasNull() ? null : v;
    }
}
