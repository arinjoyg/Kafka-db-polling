package com.example.rr;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.rr.model.Outcome;
import com.example.rr.model.PaymentCommand;
import com.example.rr.model.PaymentReply;
import com.example.rr.processor.PaymentService;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

/** Scenario 12: duplicate injection – a redelivered/retried requestId must never apply twice. */
class IdempotencyIT extends AbstractIntegrationTest {

    @Autowired
    PaymentService paymentService;

    @Test
    void sameRequestIdIsAppliedExactlyOnce() {
        PaymentCommand cmd = newCommand();

        ResponseEntity<Outcome> first = rest.postForEntity("/payments", cmd, Outcome.class);
        ResponseEntity<Outcome> second = rest.postForEntity("/payments", cmd, Outcome.class);

        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(first.getBody()).isNotNull();
        assertThat(first.getBody().kind()).isEqualTo(Outcome.Kind.OK);

        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(second.getBody()).isNotNull();
        assertThat(second.getBody().kind()).isEqualTo(Outcome.Kind.DUPLICATE);
        assertThat(second.getBody().reply().version()).isEqualTo(first.getBody().reply().version());

        assertThat(jdbc.queryForObject("SELECT version FROM payments WHERE payment_id = ?", Long.class,
                cmd.paymentId())).isEqualTo(1L);
        assertThat(processedCount(cmd.requestId())).isEqualTo(1L);
    }

    /**
     * Kafka keying serializes same-payment requests on one consumer, so the DB-level race only happens on
     * rebalance overlap or redelivery. Call the service directly from two threads to force it.
     */
    @Test
    void concurrentDuplicatesApplyExactlyOnce() throws Exception {
        var pool = java.util.concurrent.Executors.newFixedThreadPool(2);
        try {
            for (int i = 0; i < 50; i++) {
                PaymentCommand cmd = newCommand();
                var gate = new java.util.concurrent.CountDownLatch(1);
                java.util.concurrent.Callable<PaymentReply> call = () -> {
                    gate.await();
                    return paymentService.process(cmd);
                };
                var a = pool.submit(call);
                var b = pool.submit(call);
                gate.countDown();
                PaymentReply ra = a.get(10, java.util.concurrent.TimeUnit.SECONDS);
                PaymentReply rb = b.get(10, java.util.concurrent.TimeUnit.SECONDS);

                assertThat(ra.duplicate() ^ rb.duplicate()).as("exactly one applies, iteration " + i).isTrue();
                assertThat(ra.version()).isEqualTo(1L);
                assertThat(rb.version()).isEqualTo(1L);
                assertThat(jdbc.queryForObject("SELECT version FROM payments WHERE payment_id = ?", Long.class,
                        cmd.paymentId())).isEqualTo(1L);
            }
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void distinctRequestsForSamePaymentAreOrderedUpdates() {
        PaymentCommand first = newCommand();
        PaymentCommand second = first.withRequestId(UUID.randomUUID().toString());

        assertThat(rest.postForEntity("/payments", first, Outcome.class).getBody().reply().version()).isEqualTo(1L);
        assertThat(rest.postForEntity("/payments", second, Outcome.class).getBody().reply().version()).isEqualTo(2L);
    }
}
