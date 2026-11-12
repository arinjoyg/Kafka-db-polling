package com.example.rr;

import com.example.rr.model.PaymentReply;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * REFERENCE for the upstream team (see docs/upstream-contract.md): how to consume the response topic safely.
 *
 * Kafka delivers responses at-least-once to a consumer group: after a rebalance or crash of the upstream's own
 * consumer, a response it already read (but whose offset it hadn't committed) is delivered again. Retries also
 * legitimately produce a second response (ACCEPTED for attempt 1, REJECTED_DUPLICATE for attempt 2).
 *
 * Rule: the FIRST response for a requestId settles it; every later one is ignored. In production the "settled"
 * map is the upstream's own request table, updated with a conditional write
 * ({@code UPDATE ... SET status=? WHERE request_id=? AND status='PENDING'}), not memory.
 */
public final class UpstreamResponseHandler {

    public enum Settlement { SUCCEEDED, FAILED }

    private final Map<String, Settlement> settled = new ConcurrentHashMap<>();

    /** @return true if this response settled the request; false if it was already settled and must be ignored. */
    public boolean onResponse(PaymentReply reply) {
        return settled.putIfAbsent(reply.requestId(), settlementOf(reply)) == null;
    }

    public Settlement settlement(String requestId) {
        return settled.get(requestId);
    }

    /** REJECTED_DUPLICATE with originalStatus=ACCEPTED means the payment DID go through. */
    public static Settlement settlementOf(PaymentReply reply) {
        boolean ok = PaymentReply.ACCEPTED.equals(reply.status())
                || (PaymentReply.REJECTED_DUPLICATE.equals(reply.status())
                    && PaymentReply.ACCEPTED.equals(reply.originalStatus()));
        return ok ? Settlement.SUCCEEDED : Settlement.FAILED;
    }
}
