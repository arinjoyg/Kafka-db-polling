package com.example.rr;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.rr.config.AppProperties;
import com.example.rr.config.RrHeaders;
import com.example.rr.model.Outcome;
import com.example.rr.model.PaymentCommand;
import com.example.rr.model.PaymentReply;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.TopicPartitionOffset;

/**
 * Scenario 8 (multi-instance requestors): two requestor instances on the same reply topic, each pinned
 * to its own reply partition, must each receive only their own replies – no cross-talk, no timeouts.
 */
class ReplyRoutingIT extends AbstractIntegrationTest {

    @Autowired
    ProducerFactory<String, String> producerFactory;

    @Autowired
    ConsumerFactory<String, String> consumerFactory;

    @Autowired
    AppProperties props;

    @Autowired
    ObjectMapper mapper;

    @Test
    void eachRequestorInstanceReceivesOnlyItsOwnReplies() throws Exception {
        // Second "instance": reply partition 1 (the application context uses partition 0).
        var containerProps = new ContainerProperties(new TopicPartitionOffset(
                props.topics().reply(), 1, TopicPartitionOffset.SeekPosition.END));
        containerProps.setGroupId("requestor-it-second");
        var container = new KafkaMessageListenerContainer<>(consumerFactory, containerProps);
        var second = new ReplyingKafkaTemplate<>(producerFactory, container);
        second.setDefaultReplyTimeout(Duration.ofSeconds(5));
        second.afterPropertiesSet();
        second.start();

        try {
            // Same race as a real pod start: the reply consumer seeks to END on its first poll, so replies
            // produced before that are skipped. Probe until one round-trip succeeds (= readiness check).
            await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500)).ignoreExceptions()
                    .until(() -> {
                        PaymentCommand probe = newCommand("probe-" + UUID.randomUUID());
                        var rec = new ProducerRecord<String, String>(
                                props.topics().request(), probe.paymentId(), mapper.writeValueAsString(probe));
                        RrHeaders.putDeadline(rec.headers(), System.currentTimeMillis() + 2_000);
                        return second.sendAndReceive(rec, Duration.ofSeconds(2)).get(3, TimeUnit.SECONDS) != null;
                    });

            List<CompletableFuture<Outcome>> fromMain = new ArrayList<>();
            List<PaymentCommand> sentBySecond = new ArrayList<>();
            List<RequestReplyFuture<String, String, String>> fromSecond = new ArrayList<>();

            for (int i = 0; i < 25; i++) {
                fromMain.add(client.submit(newCommand()));

                PaymentCommand cmd = newCommand();
                var record = new ProducerRecord<String, String>(
                        props.topics().request(), cmd.paymentId(), mapper.writeValueAsString(cmd));
                RrHeaders.putDeadline(record.headers(), System.currentTimeMillis() + 5_000);
                sentBySecond.add(cmd);
                fromSecond.add(second.sendAndReceive(record));
            }

            for (CompletableFuture<Outcome> f : fromMain) {
                assertThat(f.get(10, TimeUnit.SECONDS).kind()).isEqualTo(Outcome.Kind.OK);
            }
            for (int i = 0; i < fromSecond.size(); i++) {
                ConsumerRecord<String, String> reply = fromSecond.get(i).get(10, TimeUnit.SECONDS);
                assertThat(reply.partition()).isEqualTo(1);
                PaymentReply parsed = mapper.readValue(reply.value(), PaymentReply.class);
                assertThat(parsed.requestId()).isEqualTo(sentBySecond.get(i).requestId());
            }
        } finally {
            second.stop();
        }
    }
}
