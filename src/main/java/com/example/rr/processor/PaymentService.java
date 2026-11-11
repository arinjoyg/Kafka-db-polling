package com.example.rr.processor;

import com.example.rr.config.AppProperties;
import com.example.rr.model.PaymentCommand;
import com.example.rr.model.PaymentReply;
import com.example.rr.model.RecordPosition;
import com.example.rr.processor.PaymentRepository.ApplyResult;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

@Service
@Profile("processor")
public class PaymentService {

    private final PaymentRepository repo;
    private final TransactionTemplate tx;
    private final AppProperties props;
    private final Counter clientDuplicates;
    private final Counter redeliveries;

    public PaymentService(PaymentRepository repo, TransactionTemplate tx, AppProperties props, MeterRegistry registry) {
        this.repo = repo;
        this.tx = tx;
        this.props = props;
        this.clientDuplicates = Counter.builder("rr.processor.duplicates").tag("type", "client")
                .description("Requests rejected because the same requestId was already processed")
                .register(registry);
        this.redeliveries = Counter.builder("rr.processor.duplicates").tag("type", "redelivery")
                .description("Same Kafka record seen again (crash, rebalance, replay); original success re-sent")
                .register(registry);
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
        return switch (r.kind()) {
            case APPLIED -> accepted(cmd, r.version());
            case REDELIVERY -> {
                // Same Kafka record again: the caller may still be waiting for it. Re-send the original success.
                redeliveries.increment();
                yield accepted(cmd, r.version());
            }
            case CLIENT_DUPLICATE -> {
                // A NEW request reusing an already-processed requestId (caller retried after its timeout).
                clientDuplicates.increment();
                yield new PaymentReply(cmd.requestId(), cmd.paymentId(), PaymentReply.REJECTED_DUPLICATE,
                        r.version(), true, "requestId already processed", PaymentReply.ACCEPTED,
                        props.instanceId(), System.currentTimeMillis());
            }
        };
    }

    public void recordDropped(RecordPosition pos) {
        tx.executeWithoutResult(status -> repo.advanceOffset(pos));
    }

    private PaymentReply accepted(PaymentCommand cmd, long version) {
        return new PaymentReply(cmd.requestId(), cmd.paymentId(), PaymentReply.ACCEPTED, version, false,
                null, null, props.instanceId(), System.currentTimeMillis());
    }
}
