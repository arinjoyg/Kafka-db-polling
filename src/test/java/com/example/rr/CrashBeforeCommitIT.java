package com.example.rr;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.rr.model.Outcome;
import com.example.rr.model.PaymentCommand;
import com.example.rr.processor.ReplyFaultInjector;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;

/**
 * The processor "crashes" after the response has been sent but before the Kafka transaction (response + offset)
 * commits. The record is redelivered, the ledger recognises the same record (REDELIVERY), and the response is
 * sent again. Two responses physically reach the topic, but the first one is aborted:
 * a read_committed consumer (the caller) sees exactly ONE.
 */
class CrashBeforeCommitIT extends AbstractIntegrationTest {

    @Autowired
    ReplyFaultInjector faults;

    @Autowired
    ConsumerFactory<String, String> consumerFactory;

    @Test
    void crashAfterResponseSentBeforeCommitYieldsExactlyOneVisibleResponse() throws Exception {
        TopicPartition replyPartition = new TopicPartition(appProperties.topics().reply(), appProperties.replyPartition());
        Properties uncommitted = new Properties();
        uncommitted.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted");

        try (Consumer<String, String> committedView = consumerFactory.createConsumer("crash-rc", "rc");
             Consumer<String, String> rawView = consumerFactory.createConsumer("crash-ru", "crash", "ru", uncommitted)) {
            for (Consumer<String, String> c : List.of(committedView, rawView)) {
                c.assign(List.of(replyPartition));
                c.seekToEnd(List.of(replyPartition));
                c.position(replyPartition);
            }

            PaymentCommand cmd = newCommand();
            faults.failOnceAfterReply(cmd.requestId());
            int firedBefore = faults.fired();

            Outcome outcome = client.submit(cmd).get(5, TimeUnit.SECONDS);

            assertThat(faults.fired()).as("the crash was injected").isEqualTo(firedBefore + 1);
            assertThat(outcome.kind()).as("caller still gets its answer within the SLA").isEqualTo(Outcome.Kind.OK);

            long visible = countFor(committedView, cmd.requestId(), Duration.ofSeconds(3));
            long physical = countFor(rawView, cmd.requestId(), Duration.ofSeconds(3));
            System.out.printf("%n[crash] responses on topic=%d (incl. aborted), visible to read_committed=%d%n%n",
                    physical, visible);

            assertThat(physical).as("the first response really was sent before the crash").isEqualTo(2);
            assertThat(visible).as("read_committed caller sees exactly one response").isEqualTo(1);
            assertThat(jdbc.queryForObject("SELECT version FROM payments WHERE payment_id = ?", Long.class,
                    cmd.paymentId())).as("applied exactly once").isEqualTo(1L);
        }
    }

    private static long countFor(Consumer<String, String> consumer, String requestId, Duration window) {
        long n = 0;
        long end = System.nanoTime() + window.toNanos();
        while (System.nanoTime() < end) {
            for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(200))) {
                if (requestId.equals(r.key())) {
                    n++;
                }
            }
        }
        return n;
    }
}
