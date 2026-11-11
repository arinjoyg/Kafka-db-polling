package com.example.rr.processor;

import com.example.rr.model.PaymentCommand;
import com.example.rr.model.RecordPosition;
import java.sql.CallableStatement;
import java.sql.Types;
import java.util.List;
import java.util.Optional;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@Profile("processor")
public class PaymentRepository {

    /**
     * One PL/SQL block = one DB round trip (plus COMMIT):
     * <ol>
     *   <li>Claim the requestId in the ledger. DUP_VAL_ON_INDEX means it was already applied: if a concurrent
     *       transaction holds it uncommitted, Oracle waits for that commit first, so the prior version is visible.</li>
     *   <li>Otherwise MERGE the payment (falling back to UPDATE if a concurrent first insert of the same payment won),
     *       read the new version and record it on the ledger row.</li>
     *   <li>Advance the consumer offset for this partition in the same transaction (GREATEST: replays never move it back).</li>
     * </ol>
     * Binds go into local variables first so each value is bound once.
     */
    private static final String APPLY_PLSQL = """
            DECLARE
              p_req    VARCHAR2(64)  := ?;
              p_pay    VARCHAR2(64)  := ?;
              p_amt    NUMBER        := ?;
              p_ccy    CHAR(3)       := ?;
              p_topic  VARCHAR2(249) := ?;
              p_part   NUMBER        := ?;
              p_next   NUMBER        := ?;
              v_ver    NUMBER;
              v_dup    NUMBER := 0;
            BEGIN
              BEGIN
                INSERT INTO processed_requests (request_id, payment_id, processed_at)
                VALUES (p_req, p_pay, SYSTIMESTAMP);
              EXCEPTION
                WHEN DUP_VAL_ON_INDEX THEN v_dup := 1;
              END;

              IF v_dup = 0 THEN
                BEGIN
                  MERGE INTO payments t
                  USING (SELECT p_pay AS payment_id FROM dual) s
                     ON (t.payment_id = s.payment_id)
                  WHEN MATCHED THEN UPDATE
                       SET t.amount = p_amt, t.currency = p_ccy, t.status = 'ACCEPTED',
                           t.version = t.version + 1, t.last_request_id = p_req, t.updated_at = SYSTIMESTAMP
                  WHEN NOT MATCHED THEN INSERT
                       (payment_id, amount, currency, status, version, last_request_id, updated_at)
                       VALUES (p_pay, p_amt, p_ccy, 'ACCEPTED', 1, p_req, SYSTIMESTAMP);
                EXCEPTION
                  WHEN DUP_VAL_ON_INDEX THEN
                    UPDATE payments
                       SET amount = p_amt, currency = p_ccy, status = 'ACCEPTED',
                           version = version + 1, last_request_id = p_req, updated_at = SYSTIMESTAMP
                     WHERE payment_id = p_pay;
                END;
                SELECT version INTO v_ver FROM payments WHERE payment_id = p_pay;
                UPDATE processed_requests SET result_version = v_ver WHERE request_id = p_req;
              ELSE
                SELECT result_version INTO v_ver FROM processed_requests WHERE request_id = p_req;
              END IF;

              IF p_topic IS NOT NULL THEN
                MERGE INTO consumer_offsets o
                USING (SELECT p_topic AS topic, p_part AS partition_id FROM dual) s
                   ON (o.topic = s.topic AND o.partition_id = s.partition_id)
                WHEN MATCHED THEN UPDATE
                     SET o.next_offset = GREATEST(o.next_offset, p_next), o.updated_at = SYSTIMESTAMP
                WHEN NOT MATCHED THEN INSERT (topic, partition_id, next_offset, updated_at)
                     VALUES (p_topic, p_part, p_next, SYSTIMESTAMP);
              END IF;

              ? := v_ver;
              ? := v_dup;
            END;
            """;

    private static final String ADVANCE_OFFSET_SQL = """
            MERGE INTO consumer_offsets o
            USING (SELECT ? AS topic, ? AS partition_id, ? AS next_offset FROM dual) s
               ON (o.topic = s.topic AND o.partition_id = s.partition_id)
            WHEN MATCHED THEN UPDATE
                 SET o.next_offset = GREATEST(o.next_offset, s.next_offset), o.updated_at = SYSTIMESTAMP
            WHEN NOT MATCHED THEN INSERT (topic, partition_id, next_offset, updated_at)
                 VALUES (s.topic, s.partition_id, s.next_offset, SYSTIMESTAMP)
            """;

    public record ApplyResult(long version, boolean duplicate) {}

    private final JdbcTemplate jdbc;

    public PaymentRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Must run inside a transaction (the caller's TransactionTemplate). */
    public ApplyResult apply(PaymentCommand c, RecordPosition pos) {
        return jdbc.execute(APPLY_PLSQL, (CallableStatement cs) -> {
            cs.setString(1, c.requestId());
            cs.setString(2, c.paymentId());
            cs.setBigDecimal(3, c.amount());
            cs.setString(4, c.currency());
            if (pos != null) {
                cs.setString(5, pos.topic());
                cs.setInt(6, pos.partition());
                cs.setLong(7, pos.nextOffset());
            } else {
                cs.setNull(5, Types.VARCHAR);
                cs.setNull(6, Types.INTEGER);
                cs.setNull(7, Types.BIGINT);
            }
            cs.registerOutParameter(8, Types.BIGINT);
            cs.registerOutParameter(9, Types.INTEGER);
            cs.execute();
            return new ApplyResult(cs.getLong(8), cs.getInt(9) == 1);
        });
    }

    /** Records progress for a record that was consumed but not applied (expired drop). */
    public void advanceOffset(RecordPosition pos) {
        jdbc.update(ADVANCE_OFFSET_SQL, pos.topic(), pos.partition(), pos.nextOffset());
    }

    public Optional<Long> findNextOffset(String topic, int partition) {
        List<Long> rows = jdbc.queryForList(
                "SELECT next_offset FROM consumer_offsets WHERE topic = ? AND partition_id = ?",
                Long.class, topic, partition);
        return rows.isEmpty() ? Optional.empty() : Optional.ofNullable(rows.get(0));
    }

    public Optional<Long> findResultVersion(String requestId) {
        List<Long> rows = jdbc.queryForList(
                "SELECT result_version FROM processed_requests WHERE request_id = ?", Long.class, requestId);
        return rows.isEmpty() ? Optional.empty() : Optional.ofNullable(rows.get(0));
    }
}
