package com.example.rr.requestor;

import com.example.rr.config.AppProperties;
import com.example.rr.config.RrHeaders;
import com.example.rr.model.Outcome;
import com.example.rr.model.PaymentCommand;
import com.example.rr.model.PaymentReply;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.requestreply.KafkaReplyTimeoutException;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.stereotype.Component;

/** Non-blocking request-reply client. The future always completes normally with an {@link Outcome}. */
@Component
@Profile("requestor")
public class ReplyClient {

    private final ReplyingKafkaTemplate<String, String, String> template;
    private final ObjectMapper mapper;
    private final AppProperties props;
    private final AuditRepository audit;
    private final Map<Outcome.Kind, Timer> timers = new EnumMap<>(Outcome.Kind.class);

    public ReplyClient(ReplyingKafkaTemplate<String, String, String> template, ObjectMapper mapper,
                       AppProperties props, AuditRepository audit, MeterRegistry registry) {
        this.template = template;
        this.mapper = mapper;
        this.props = props;
        this.audit = audit;
        for (Outcome.Kind kind : Outcome.Kind.values()) {
            timers.put(kind, Timer.builder("rr.requestor.roundtrip")
                    .description("Send-to-reply latency measured at the requestor")
                    .tag("outcome", kind.name())
                    .publishPercentiles(0.5, 0.95, 0.99, 0.999)
                    .publishPercentileHistogram()
                    .register(registry));
        }
    }

    public CompletableFuture<Outcome> submit(PaymentCommand cmd) {
        return submit(cmd, System.currentTimeMillis() + props.slaMs());
    }

    /** Visible for tests that need to send an already-expired or custom deadline. */
    public CompletableFuture<Outcome> submit(PaymentCommand cmd, long deadlineEpochMs) {
        long start = System.nanoTime();
        String json;
        try {
            json = mapper.writeValueAsString(cmd);
        } catch (JsonProcessingException e) {
            return CompletableFuture.completedFuture(finish(Outcome.error(cmd.requestId(), 0, e), cmd));
        }

        // Key = paymentId => per-payment ordering on one partition (and the hot-key risk that comes with it).
        var record = new ProducerRecord<String, String>(props.topics().request(), cmd.paymentId(), json);
        RrHeaders.putDeadline(record.headers(), deadlineEpochMs);

        CompletableFuture<Outcome> result;
        try {
            result = template.sendAndReceive(record).handle((reply, ex) -> {
                long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                if (ex == null) {
                    try {
                        return Outcome.ok(cmd.requestId(), mapper.readValue(reply.value(), PaymentReply.class), ms);
                    } catch (JsonProcessingException e) {
                        return Outcome.error(cmd.requestId(), ms, e);
                    }
                }
                return isTimeout(ex) ? Outcome.timeout(cmd.requestId(), ms) : Outcome.error(cmd.requestId(), ms, ex);
            });
        } catch (RuntimeException e) {
            // e.g. producer buffer full / metadata unavailable thrown synchronously
            long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            result = CompletableFuture.completedFuture(Outcome.error(cmd.requestId(), ms, e));
        }
        return result.thenApply(o -> finish(o, cmd));
    }

    private Outcome finish(Outcome outcome, PaymentCommand cmd) {
        timers.get(outcome.kind()).record(outcome.latencyMs(), TimeUnit.MILLISECONDS);
        audit.record(outcome, cmd.paymentId());
        return outcome;
    }

    private static boolean isTimeout(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof KafkaReplyTimeoutException) {
                return true;
            }
        }
        return false;
    }
}
