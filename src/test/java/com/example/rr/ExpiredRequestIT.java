package com.example.rr;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.rr.model.Outcome;
import com.example.rr.model.PaymentCommand;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Scenario 14: backlog / cold start. A request whose deadline has already passed when the processor
 * picks it up must be dropped – no DB write, no reply – rather than producing a late write.
 */
class ExpiredRequestIT extends AbstractIntegrationTest {

    @Test
    void expiredRequestIsDroppedAndNeverWritten() throws Exception {
        PaymentCommand cmd = newCommand();

        Outcome outcome = client.submit(cmd, System.currentTimeMillis() - 1).get(5, TimeUnit.SECONDS);

        assertThat(outcome.kind()).isEqualTo(Outcome.Kind.TIMEOUT);
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(3))
                .until(() -> processedCount(cmd.requestId()) == 0);
    }
}
