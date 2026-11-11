package com.example.rr.processor;

import com.example.rr.config.AppProperties;
import com.example.rr.model.PaymentCommand;
import com.example.rr.model.PaymentReply;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Profile("processor")
public class PaymentService {

    private final PaymentRepository repo;
    private final AppProperties props;

    public PaymentService(PaymentRepository repo, AppProperties props) {
        this.repo = repo;
        this.props = props;
    }

    /**
     * One DB transaction: claim the requestId, upsert the payment, record the resulting version.
     * Commits before the caller (listener) returns the reply, so a reply always implies a durable write.
     */
    @Transactional
    public PaymentReply process(PaymentCommand cmd) {
        repo.applyTimeouts(props.db().lockTimeoutMs(), props.db().statementTimeoutMs());

        if (!repo.claimRequest(cmd.requestId(), cmd.paymentId())) {
            long previous = repo.findResultVersion(cmd.requestId()).orElse(-1L);
            return new PaymentReply(cmd.requestId(), cmd.paymentId(), "ACCEPTED", previous, true,
                    props.instanceId(), System.currentTimeMillis());
        }

        long version = repo.upsertPayment(cmd);
        repo.recordResultVersion(cmd.requestId(), version);
        return new PaymentReply(cmd.requestId(), cmd.paymentId(), "ACCEPTED", version, false,
                props.instanceId(), System.currentTimeMillis());
    }
}
