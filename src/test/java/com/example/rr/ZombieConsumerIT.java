package com.example.rr;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.rr.model.PaymentCommand;
import com.example.rr.processor.PaymentRequestListener;
import com.example.rr.processor.ReplyFaultInjector;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Rebalance while a request is in flight, worst case: processor A has committed the DB write but stalls before
 * sending the response, longer than max.poll.interval.ms. The group evicts A and gives the partition to B.
 * B processes the same record (ledger: REDELIVERY) and responds. A then wakes up and sends its response too.
 *
 * Expected: A's transaction carries a stale group generation and is rejected (KIP-447). Depending on timing A's
 * response is either written and then aborted, or failed client-side before it is written; either way the caller
 * (read_committed) sees exactly ONE response, B's. One DB write.
 */
class ZombieConsumerIT extends AbstractIntegrationTest {

    private static final String REQUEST_TOPIC = "zombie.request";
    private static final String REPLY_TOPIC = "zombie.reply";
    private static final Duration STALL = Duration.ofSeconds(12);

    @Autowired
    KafkaTemplate<String, String> kafka;

    @Autowired
    ConsumerFactory<String, String> consumerFactory;

    @Autowired
    ObjectMapper mapper;

    @Test
    void consumerEvictedMidRequestStillYieldsOneVisibleResponse() throws Exception {
        ConfigurableApplicationContext a = startProcessor("it-zombie-a");
        ConfigurableApplicationContext b = null;
        Properties uncommitted = new Properties();
        uncommitted.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted");

        try (Consumer<String, String> committedView = consumerFactory.createConsumer("zombie-rc", "rc");
             Consumer<String, String> rawView = consumerFactory.createConsumer("zombie-ru", "zombie", "ru", uncommitted)) {

            // A owns every partition before the request is sent.
            await().atMost(Duration.ofSeconds(60)).until(() -> assignedCount(a) == appProperties.topics().requestPartitions());
            List<TopicPartition> replyPartitions = committedView.partitionsFor(REPLY_TOPIC).stream()
                    .map(p -> new TopicPartition(p.topic(), p.partition())).toList();
            for (Consumer<String, String> c : List.of(committedView, rawView)) {
                c.assign(replyPartitions);
                c.seekToEnd(replyPartitions);
                replyPartitions.forEach(c::position);
            }

            PaymentCommand cmd = newCommand();
            ReplyFaultInjector aFaults = a.getBean(ReplyFaultInjector.class);
            aFaults.stallOnceBeforeReply(cmd.requestId(), STALL);   // >> max.poll.interval.ms (4s)
            kafka.send(REQUEST_TOPIC, cmd.paymentId(), mapper.writeValueAsString(cmd)).get();

            // A has written to the DB and is now stuck before responding.
            await().atMost(Duration.ofSeconds(30)).until(() -> aFaults.stallsStarted() == 1);
            long aWakesAt = System.nanoTime() + STALL.toNanos();

            // B joins; A can't take part in the rebalance, gets evicted, and B takes over A's partitions.
            b = startProcessor("it-zombie-b");

            long[] counts = new long[2];   // [visible to read_committed, physically on the topic]
            // B takes over and answers...
            await().atMost(Duration.ofSeconds(45)).pollInterval(Duration.ofMillis(200)).until(() -> {
                counts[0] += count(committedView, cmd.requestId());
                counts[1] += count(rawView, cmd.requestId());
                return counts[0] >= 1;
            });
            // ...then A wakes up and tries to send and commit its own response. Keep watching well past that.
            long watchUntil = aWakesAt + Duration.ofSeconds(5).toNanos();
            while (System.nanoTime() < watchUntil) {
                counts[0] += count(committedView, cmd.requestId());
                counts[1] += count(rawView, cmd.requestId());
            }
            System.out.printf("%n[zombie] responses on topic=%d (incl. aborted), visible to read_committed=%d%n%n",
                    counts[1], counts[0]);

            assertThat(counts[0]).as("caller sees exactly one response: A's commit was fenced").isEqualTo(1);
            assertThat(counts[1]).as("A's response, if written at all, was aborted").isBetween(1L, 2L);
            assertThat(jdbc.queryForObject("SELECT version FROM payments WHERE payment_id = ?", Long.class,
                    cmd.paymentId())).as("applied exactly once").isEqualTo(1L);
        } finally {
            if (b != null) {
                b.close();
            }
            a.close();
        }
    }

    private static int assignedCount(ConfigurableApplicationContext ctx) {
        var container = ctx.getBean(KafkaListenerEndpointRegistry.class)
                .getListenerContainer(PaymentRequestListener.LISTENER_ID);
        var assigned = container == null ? null : container.getAssignedPartitions();
        return assigned == null ? 0 : assigned.size();
    }

    private static long count(Consumer<String, String> consumer, String requestId) {
        long n = 0;
        for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(100))) {
            if (requestId.equals(r.key())) {
                n++;
            }
        }
        return n;
    }

    /** A processor-only instance (like a pod) in its own consumer group on dedicated topics. */
    private static ConfigurableApplicationContext startProcessor(String instanceId) {
        return new SpringApplicationBuilder(RequestReplyApplication.class)
                .profiles("processor")
                .run(
                        "--server.port=0",
                        "--app.instance-id=" + instanceId,
                        "--app.topics.request=" + REQUEST_TOPIC,
                        "--app.topics.reply=" + REPLY_TOPIC,
                        "--app.topics.replication-factor=1",
                        "--app.processor.group-id=zombie-processor",
                        "--app.processor.concurrency=1",
                        "--spring.kafka.consumer.properties[max.poll.interval.ms]=4000",
                        // Fresh topic: "latest" is resolved lazily per partition and can skip a record sent just
                        // after assignment. Earliest is exact here (the topic only holds this test's records).
                        "--spring.kafka.consumer.auto-offset-reset=earliest",
                        "--spring.flyway.enabled=false",
                        "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                        "--spring.datasource.url=" + jdbcUrlViaProxy(),
                        "--spring.datasource.username=" + ORACLE.getUsername(),
                        "--spring.datasource.password=" + ORACLE.getPassword());
    }
}
