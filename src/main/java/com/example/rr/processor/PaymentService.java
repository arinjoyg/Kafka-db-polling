package com.example.rr.processor;

import com.example.rr.config.AppProperties;
import com.example.rr.model.PaymentCommand;
import com.example.rr.model.PaymentReply;
import com.example.rr.processor.PaymentRepository.ApplyResult;
import org.springframework.context.annotation.Profile;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

@Service
@Profile("processor")
public class PaymentService {

    private final PaymentRepository repo;
    private final TransactionTemplate tx;
    private final AppProperties props;

    public PaymentService(PaymentRepository repo, TransactionTemplate tx, AppProperties props) {
        this.repo = repo;
        this.tx = tx;
        this.props = props;
    }

    /**
     * Happy path = 3 DB round trips: SET LOCAL (both guards), the apply statement, COMMIT
     * (BEGIN is piggybacked by the driver). Previously 6.
     * The transaction commits before this returns, so a reply always implies a durable write.
     */
    public PaymentReply process(PaymentCommand cmd) {
        try {
            ApplyResult result = tx.execute(status -> {
                repo.applyTimeouts(props.db().lockTimeoutMs(), props.db().statementTimeoutMs());
                return repo.apply(cmd);
            });
            if (result != null && result.appliedVersion() != null) {
                return reply(cmd, result.appliedVersion(), false);
            }
            long prior = (result == null || result.priorVersion() == null) ? -1L : result.priorVersion();
            return reply(cmd, prior, true);
        } catch (DuplicateKeyException raced) {
            // A concurrent delivery of the same requestId committed first. Our statement, including the
            // payment upsert, was rolled back. Report the version the winner applied.
            return reply(cmd, repo.findResultVersion(cmd.requestId()).orElse(-1L), true);
        }
    }

    private PaymentReply reply(PaymentCommand cmd, long version, boolean duplicate) {
        return new PaymentReply(cmd.requestId(), cmd.paymentId(), "ACCEPTED", version, duplicate,
                props.instanceId(), System.currentTimeMillis());
    }
}
