package com.example.rr;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.rr.config.AppProperties;
import com.example.rr.model.PaymentCommand;
import com.example.rr.model.PaymentReply;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Emulates the real upstream service: it produces to the request topic with NO reply headers, polls the response
 * topic for 300ms (-Drr.upstream.timeoutMs), and on timeout sends the SAME request (same requestId) again.
 *
 * DB latency is injected per round trip so the first attempt (2 DB round trips) takes longer than the timeout. Expected: exactly one write, and two
 * responses for the requestId: ACCEPTED (late, for attempt 1) and REJECTED_DUPLICATE carrying
 * originalStatus=ACCEPTED (for attempt 2).
 */
class UpstreamRetryIT extends AbstractIntegrationTest {

    private static final Duration UPSTREAM_TIMEOUT = Duration.ofMillis(Long.getLong("rr.upstream.timeoutMs", 300));

    /** Per DB round trip; attempt 1 needs 2 round trips, so it lands well past the upstream timeout. */
    private static final int DB_LATENCY_MS = (int) (UPSTREAM_TIMEOUT.toMillis() / 2 + 50);

    @Autowired
    KafkaTemplate<String, String> kafka;

    @Autowired
    ConsumerFactory<String, String> consumerFactory;

    @Autowired
    AppProperties props;

    @Autowired
    ObjectMapper mapper;

    @Test
    void retryAfterUpstreamTimeoutIsRejectedAsDuplicateWithOriginalOutcome() throws Exception {
        DB_PROXY.toxics().latency("slow-db", ToxicDirection.DOWNSTREAM, DB_LATENCY_MS);

        try (Consumer<String, String> responses = consumerFactory.createConsumer("upstream-sim", "upstream-sim")) {
            List<TopicPartition> partitions = responses.partitionsFor(props.topics().reply()).stream()
                    .map(p -> new TopicPartition(p.topic(), p.partition())).toList();
            responses.assign(partitions);
            responses.seekToEnd(partitions);
            partitions.forEach(responses::position);   // materialise the end positions before sending

            PaymentCommand cmd = newCommand();
            String json = mapper.writeValueAsString(cmd);

            // Attempt 1, exactly as the upstream sends it: no reply headers, key = paymentId.
            kafka.send(props.topics().request(), cmd.paymentId(), json).get();
            List<PaymentReply> got = pollFor(responses, cmd.requestId(), 1, UPSTREAM_TIMEOUT);
            assertThat(got).as("no response within the upstream's %dms timeout", UPSTREAM_TIMEOUT.toMillis()).isEmpty();

            // Attempt 2: the upstream times out and re-sends the SAME request.
            kafka.send(props.topics().request(), cmd.paymentId(), json).get();
            got = pollFor(responses, cmd.requestId(), 2, Duration.ofSeconds(15));

            assertThat(got).extracting(PaymentReply::status)
                    .containsExactlyInAnyOrder(PaymentReply.ACCEPTED, PaymentReply.REJECTED_DUPLICATE);
            PaymentReply rejection = got.stream()
                    .filter(r -> PaymentReply.REJECTED_DUPLICATE.equals(r.status())).findFirst().orElseThrow();
            assertThat(rejection.duplicate()).isTrue();
            assertThat(rejection.originalStatus()).as("caller can tell the payment DID go through")
                    .isEqualTo(PaymentReply.ACCEPTED);
            assertThat(rejection.version()).isEqualTo(1L);
            assertThat(jdbc.queryForObject("SELECT version FROM payments WHERE payment_id = ?", Long.class,
                    cmd.paymentId())).as("applied exactly once").isEqualTo(1L);
        }
    }

    /** Collects responses keyed by requestId until {@code want} arrive or the timeout passes. */
    private List<PaymentReply> pollFor(Consumer<String, String> consumer, String requestId, int want, Duration timeout)
            throws Exception {
        List<PaymentReply> out = new ArrayList<>();
        long deadline = System.nanoTime() + timeout.toNanos();
        while (out.size() < want) {
            long remainingMs = (deadline - System.nanoTime()) / 1_000_000;
            if (remainingMs <= 0) {
                break;
            }
            for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(Math.min(remainingMs, 100)))) {
                if (requestId.equals(r.key())) {
                    out.add(mapper.readValue(r.value(), PaymentReply.class));
                }
            }
        }
        return out;
    }
}
