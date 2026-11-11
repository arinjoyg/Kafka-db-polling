package com.example.rr.processor;

import com.example.rr.config.RrHeaders;
import com.example.rr.model.PaymentCommand;
import com.example.rr.model.PaymentReply;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

@Component
@Profile("processor")
public class PaymentRequestListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentRequestListener.class);

    private final PaymentService service;
    private final ObjectMapper mapper;
    private final Counter expired;
    private final Timer queueDelay;
    private final Timer processing;

    public PaymentRequestListener(PaymentService service, ObjectMapper mapper, MeterRegistry registry) {
        this.service = service;
        this.mapper = mapper;
        this.expired = Counter.builder("rr.processor.expired")
                .description("Requests dropped because their deadline had passed before processing")
                .register(registry);
        // Time between produce and our pick-up = lag + poll delay. This is where rebalances show up.
        this.queueDelay = Timer.builder("rr.processor.queue.delay")
                .publishPercentiles(0.5, 0.99, 0.999).publishPercentileHistogram()
                .register(registry);
        this.processing = Timer.builder("rr.processor.processing")
                .publishPercentiles(0.5, 0.99, 0.999).publishPercentileHistogram()
                .register(registry);
    }

    /**
     * The listener id doubles as the consumer group id ("payment-processor").
     * {@code @SendTo} with no value replies to the REPLY_TOPIC/REPLY_PARTITION headers set by the
     * requestor; the correlation id header is copied automatically. Returning null sends no reply.
     */
    @KafkaListener(id = "payment-processor", topics = "${app.topics.request}", containerFactory = "processorFactory")
    @SendTo
    public String onRequest(ConsumerRecord<String, String> record) throws JsonProcessingException {
        long now = System.currentTimeMillis();
        queueDelay.record(Math.max(0, now - record.timestamp()), TimeUnit.MILLISECONDS);

        long deadline = RrHeaders.deadline(record.headers());
        if (deadline > 0 && now > deadline) {
            // Caller has already timed out. Writing now would create an unreconciled "late write"
            // and the reply would be discarded anyway. Drop it.
            expired.increment();
            log.debug("Dropping expired request partition={} offset={} late-by={}ms",
                    record.partition(), record.offset(), now - deadline);
            return null;
        }

        PaymentCommand cmd = mapper.readValue(record.value(), PaymentCommand.class);
        cmd.validate();
        long start = System.nanoTime();
        PaymentReply reply;
        try {
            reply = service.process(cmd);
        } finally {
            processing.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
        return mapper.writeValueAsString(reply);
    }
}
