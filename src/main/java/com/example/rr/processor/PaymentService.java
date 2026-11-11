package com.example.rr.processor;

import com.example.rr.config.AppProperties;
import com.example.rr.model.PaymentCommand;
import com.example.rr.model.PaymentReply;
import com.example.rr.model.RecordPosition;
import com.example.rr.processor.PaymentRepository.ApplyResult;
import org.springframework.context.annotation.Profile;
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

    public PaymentReply process(PaymentCommand cmd) {
        return process(cmd, null);
    }

    /**
     * 2 DB round trips: the PL/SQL apply block and COMMIT. The business write, the ledger row and the consumer
     * offset commit atomically, and the commit happens before this returns, so a reply always means a durable write.
     */
    public PaymentReply process(PaymentCommand cmd, RecordPosition pos) {
        ApplyResult r = tx.execute(status -> repo.apply(cmd, pos));
        if (r == null) {
            throw new IllegalStateException("apply returned no result for " + cmd.requestId());
        }
        return new PaymentReply(cmd.requestId(), cmd.paymentId(), "ACCEPTED", r.version(), r.duplicate(),
                props.instanceId(), System.currentTimeMillis());
    }

    public void recordDropped(RecordPosition pos) {
        tx.executeWithoutResult(status -> repo.advanceOffset(pos));
    }
}
