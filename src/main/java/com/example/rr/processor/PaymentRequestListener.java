package com.example.rr.processor;

import com.example.rr.config.AppProperties;
import com.example.rr.config.ProcessorKafkaConfig;
import com.example.rr.config.RrHeaders;
import com.example.rr.model.PaymentCommand;
import com.example.rr.model.PaymentReply;
import com.example.rr.model.RecordPosition;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
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
    private final AppProperties props;
    private final KafkaTemplate<String, String> replyTemplate;
    private final ReplyFaultInjector faults;
    private final Counter expired;
    private final Counter replayed;
    private final Timer queueDelay;
    private final Timer processing;

    public PaymentRequestListener(PaymentService service, ObjectMapper mapper, ReplayWindow replayWindow,
                                  OffsetResync offsetResync, AppProperties props,
                                  @Qualifier(ProcessorKafkaConfig.REPLY_TEMPLATE) KafkaTemplate<String, String> replyTemplate,
                                  ReplyFaultInjector faults, MeterRegistry registry) {
        this.service = service;
        this.props = props;
        this.replyTemplate = replyTemplate;
        this.faults = faults;
        this.mapper = mapper;
        this.replayWindow = replayWindow;
        this.offsetResync = offsetResync;
        this.expired = Counter.builder("rr.processor.expired")
                .description("Requests dropped because their deadline had passed before processing")
                .register(registry);
        this.replayed = Counter.builder("rr.processor.replayed")
                .description("Records re-applied SILENTLY (no response) inside a replay window after a DB switch")
                .register(registry);
        this.queueDelay = Timer.builder("rr.processor.queue.delay")
                .publishPercentiles(0.5, 0.99, 0.999).publishPercentileHistogram()
                .register(registry);
        this.processing = Timer.builder("rr.processor.processing")
                .publishPercentiles(0.5, 0.99, 0.999).publishPercentileHistogram()
                .register(registry);
    }

    /**
     * Runs inside a Kafka transaction (one per record) that also carries this record's consumer offset.
     * Exactly one VISIBLE response per request record:
     * <ul>
     *   <li>normal: DB commit, then response sent in the Kafka transaction, then commit (response + offset);</li>
     *   <li>crash after send, before commit: the transaction aborts, the response is invisible to read_committed
     *       consumers, the record is redelivered, the ledger says REDELIVERY, and the response is sent once more;</li>
     *   <li>replay after a DB switch: the write is re-applied silently, with NO response. The old database already
     *       answered (or the caller timed out long ago and will retry); a second answer would be a duplicate.</li>
     * </ul>
     * {@code id} is used for registry lookups (pause/resume); the group comes from config so test contexts stay isolated.
     */
    @KafkaListener(id = LISTENER_ID, groupId = "${app.processor.group-id}",
            topics = "${app.topics.request}", containerFactory = "processorFactory")
    public void onRequest(ConsumerRecord<String, String> record) throws JsonProcessingException {
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
            return;
        }

        PaymentCommand cmd = mapper.readValue(record.value(), PaymentCommand.class);
        cmd.validate();

        if (replay) {
            service.process(cmd, pos);   // restore the data on this DB; no response
            replayed.increment();
            return;
        }

        long start = System.nanoTime();
        PaymentReply reply;
        try {
            reply = service.process(cmd, pos);
        } finally {
            processing.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
        faults.beforeReplySent(reply.requestId());
        replyTemplate.send(respond(record, reply));
        faults.afterReplySent(reply.requestId());
    }

    /**
     * Response routing that works for both kinds of caller:
     * <ul>
     *   <li>Callers that send REPLY_TOPIC / REPLY_PARTITION headers (Spring ReplyingKafkaTemplate) get them honoured.</li>
     *   <li>Callers that just produce to the request topic and poll the response topic get it on
     *       {@code app.topics.reply}.</li>
     * </ul>
     * The key is the requestId in both cases, so a caller can match ANY attempt's response (including a late
     * response to an attempt it already gave up on) by requestId, not only by per-attempt correlation id.
     */
    private Message<String> respond(ConsumerRecord<String, String> request, PaymentReply reply)
            throws JsonProcessingException {
        Header replyTopic = request.headers().lastHeader(KafkaHeaders.REPLY_TOPIC);
        MessageBuilder<String> response = MessageBuilder.withPayload(mapper.writeValueAsString(reply))
                .setHeader(KafkaHeaders.TOPIC, replyTopic != null
                        ? new String(replyTopic.value(), StandardCharsets.UTF_8)
                        : props.topics().reply())
                .setHeader(KafkaHeaders.KEY, reply.requestId());
        Header replyPartition = request.headers().lastHeader(KafkaHeaders.REPLY_PARTITION);
        if (replyPartition != null) {
            response.setHeader(KafkaHeaders.PARTITION, ByteBuffer.wrap(replyPartition.value()).getInt());
        }
        Header correlation = request.headers().lastHeader(KafkaHeaders.CORRELATION_ID);
        if (correlation != null) {
            response.setHeader(KafkaHeaders.CORRELATION_ID, correlation.value());
        }
        return response.build();
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
