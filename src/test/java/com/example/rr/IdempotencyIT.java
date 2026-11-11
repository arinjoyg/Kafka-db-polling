package com.example.rr;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.rr.model.Outcome;
import com.example.rr.model.PaymentCommand;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

/** Scenario 12: duplicate injection – a redelivered/retried requestId must never apply twice. */
class IdempotencyIT extends AbstractIntegrationTest {

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

    @Test
    void distinctRequestsForSamePaymentAreOrderedUpdates() {
        PaymentCommand first = newCommand();
        PaymentCommand second = first.withRequestId(UUID.randomUUID().toString());

        assertThat(rest.postForEntity("/payments", first, Outcome.class).getBody().reply().version()).isEqualTo(1L);
        assertThat(rest.postForEntity("/payments", second, Outcome.class).getBody().reply().version()).isEqualTo(2L);
    }
}
