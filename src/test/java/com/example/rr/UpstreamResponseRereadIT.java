package com.example.rr;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.rr.model.Outcome;
import com.example.rr.model.PaymentCommand;
import com.example.rr.model.PaymentReply;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;

/**
 * The upstream's OWN consumer rebalances: it has read a response but not committed the offset when it crashes (or
 * loses the partition). The next group member is handed the same response again. This is Kafka at-least-once
 * delivery on the consuming side; no producer-side setting can prevent it. The upstream must dedupe by requestId.
 */
class UpstreamResponseRereadIT extends AbstractIntegrationTest {

    @Autowired
    ConsumerFactory<String, String> consumerFactory;

    @Autowired
    ObjectMapper mapper;

    @Test
    void upstreamRebalanceRedeliversResponseAndRequestIdDedupeAbsorbsIt() throws Exception {
        String group = "upstream-" + UUID.randomUUID();
        String topic = appProperties.topics().reply();
        UpstreamResponseHandler handler = new UpstreamResponseHandler();
        PaymentCommand cmd = newCommand();

        // Upstream instance 1: joins the group, commits its starting position, then reads the response...
        try (Consumer<String, String> first = consumerFactory.createConsumer(group, "upstream-1")) {
            first.subscribe(List.of(topic));
            while (first.assignment().isEmpty()) {
                first.poll(Duration.ofMillis(100));
            }
            first.assignment().forEach(first::position);
            first.commitSync();

            Outcome outcome = client.submit(cmd).get(5, TimeUnit.SECONDS);
            assertThat(outcome.kind()).isEqualTo(Outcome.Kind.OK);

            PaymentReply firstDelivery = readResponse(first, cmd.requestId());
            assertThat(handler.onResponse(firstDelivery)).as("first delivery settles the request").isTrue();
            // ...and crashes before committing the offset (close without commit; auto-commit is off).
        }

        // Upstream instance 2: takes over the partitions and is handed the SAME response again.
        try (Consumer<String, String> second = consumerFactory.createConsumer(group, "upstream-2")) {
            second.subscribe(List.of(topic));
            PaymentReply redelivered = readResponse(second, cmd.requestId());
            System.out.printf("%n[upstream] response for %s delivered again after the upstream's rebalance%n%n",
                    cmd.requestId());
            assertThat(handler.onResponse(redelivered)).as("re-delivery is recognised and ignored").isFalse();
        }

        assertThat(handler.settlement(cmd.requestId())).isEqualTo(UpstreamResponseHandler.Settlement.SUCCEEDED);
    }

    @Test
    void rejectedDuplicateOfAnAcceptedPaymentCountsAsSuccess() {
        var rejection = new PaymentReply("r1", "p1", PaymentReply.REJECTED_DUPLICATE, 1, true,
                "requestId already processed", PaymentReply.ACCEPTED, "x", 0);
        assertThat(UpstreamResponseHandler.settlementOf(rejection))
                .isEqualTo(UpstreamResponseHandler.Settlement.SUCCEEDED);
    }

    private PaymentReply readResponse(Consumer<String, String> consumer, String requestId) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(200))) {
                if (requestId.equals(r.key())) {
                    return mapper.readValue(r.value(), PaymentReply.class);
                }
            }
        }
        throw new AssertionError("no response for " + requestId);
    }
}
