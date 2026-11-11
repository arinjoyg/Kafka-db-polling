package com.example.rr.processor;

import com.example.rr.config.RrHeaders;
import com.example.rr.model.PaymentCommand;
import com.example.rr.model.PaymentReply;
import com.example.rr.model.RecordPosition;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

@Component
@Profile("processor")
public class PaymentRequestListener extends AbstractConsumerSeekAware {

    public static final String LISTENER_ID = "payment-processor";

    private static final Logger log = LoggerFactory.getLogger(PaymentRequestListener.class);

    private final PaymentService service;
    private final ObjectMapper mapper;
    private final ReplayWindow replayWindow;
    private final OffsetResync offsetResync;
    private final Counter expired;
    private final Counter replayed;
    private final Timer queueDelay;
    private final Timer processing;

    public PaymentRequestListener(PaymentService service, ObjectMapper mapper, ReplayWindow replayWindow,
                                  OffsetResync offsetResync, MeterRegistry registry) {
        this.service = service;
        this.mapper = mapper;
        this.replayWindow = replayWindow;
        this.offsetResync = offsetResync;
        this.expired = Counter.builder("rr.processor.expired")
                .description("Requests dropped because their deadline had passed before processing")
                .register(registry);
        this.replayed = Counter.builder("rr.processor.replayed")
                .description("Records re-applied inside a replay window after the DB was found behind Kafka")
                .register(registry);
        this.queueDelay = Timer.builder("rr.processor.queue.delay")
                .publishPercentiles(0.5, 0.99, 0.999).publishPercentileHistogram()
                .register(registry);
        this.processing = Timer.builder("rr.processor.processing")
                .publishPercentiles(0.5, 0.99, 0.999).publishPercentileHistogram()
                .register(registry);
    }

    /**
     * {@code id} is used for registry lookups (pause/resume); the consumer group comes from config so test
     * contexts can be isolated. {@code @SendTo} replies to REPLY_TOPIC/REPLY_PARTITION; null = no reply.
     */
    @KafkaListener(id = LISTENER_ID, groupId = "${app.processor.group-id}",
            topics = "${app.topics.request}", containerFactory = "processorFactory")
    @SendTo
    public String onRequest(ConsumerRecord<String, String> record) throws JsonProcessingException {
        long now = System.currentTimeMillis();
        queueDelay.record(Math.max(0, now - record.timestamp()), TimeUnit.MILLISECONDS);

        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        RecordPosition pos = new RecordPosition(record.topic(), record.partition(), record.offset() + 1);
        boolean replay = replayWindow.isReplay(tp, record.offset());

        long deadline = RrHeaders.deadline(record.headers());
        if (!replay && deadline > 0 && now > deadline) {
            // Caller already timed out; don't create a late write. Still record progress in the DB so a later
            // resync doesn't mistake this drop for lost data and replay it.
            expired.increment();
            service.recordDropped(pos);
            log.debug("Dropping expired request {} late-by={}ms", pos, now - deadline);
            return null;
        }
        if (replay) {
            replayed.increment();
        }

        PaymentCommand cmd = mapper.readValue(record.value(), PaymentCommand.class);
        cmd.validate();
        long start = System.nanoTime();
        PaymentReply reply;
        try {
            reply = service.process(cmd, pos);
        } finally {
            processing.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
        return mapper.writeValueAsString(reply);
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        super.onPartitionsAssigned(assignments, callback);
        try {
            offsetResync.onAssigned(assignments, callback);
        } catch (RuntimeException e) {
            // DB unreachable during a rebalance: positions stay at Kafka's committed offsets.
            // Run POST /admin/processing/resync once the DB is back.
            log.error("Could not compare DB offsets on assignment {}; run /admin/processing/resync", assignments.keySet(), e);
        }
    }

    /** Operator resync: seek all partitions this instance owns to the DB's stored offsets. */
    @SuppressWarnings("deprecation")
    public Map<String, String> resyncFromDatabase() {
        return offsetResync.resyncAll(getSeekCallbacks());
    }
}
