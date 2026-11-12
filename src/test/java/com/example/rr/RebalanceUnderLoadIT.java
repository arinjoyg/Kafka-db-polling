package com.example.rr;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.rr.model.Outcome;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.time.Duration;
import java.util.HashMap;
import java.util.HexFormat;
import org.HdrHistogram.Histogram;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Scenario 5: consumer rebalance under load. Runs an open-model load (fixed arrival rate, latency
 * measured from the INTENDED send time to avoid coordinated omission), then scales the processor
 * group out (+1 instance) and back in mid-run – exactly what an OpenShift HPA or rolling deploy does.
 *
 * Hard assertions are on correctness (no errors, every OK is durably written). Latency and timeout
 * rate are printed; the timeout-rate gate is configurable because it depends on the machine:
 *   -Drr.rebalance.maxTimeoutRate=0.02
 */
class RebalanceUnderLoadIT extends AbstractIntegrationTest {

    private static final int RATE_PER_SEC = Integer.getInteger("rr.rebalance.rate", 50);
    private static final int DURATION_SEC = Integer.getInteger("rr.rebalance.durationSec", 24);
    private static final double MAX_TIMEOUT_RATE =
            Double.parseDouble(System.getProperty("rr.rebalance.maxTimeoutRate", "0.10"));

    @Autowired
    ConsumerFactory<String, String> consumerFactory;

    @Test
    void scaleOutAndInDuringLoadLosesNothing() throws Exception {
        // Caller's view of its reply partition (read_committed), to count responses per request attempt.
        TopicPartition replyPartition = new TopicPartition(appProperties.topics().reply(), appProperties.replyPartition());
        Consumer<String, String> responses = consumerFactory.createConsumer("rebalance-recorder", "recorder");
        responses.assign(List.of(replyPartition));
        responses.seekToEnd(List.of(replyPartition));
        responses.position(replyPartition);

        List<CompletableFuture<Outcome>> futures = Collections.synchronizedList(new ArrayList<>());
        Histogram latencyMs = new Histogram(60_000, 3);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        long periodNanos = TimeUnit.SECONDS.toNanos(1) / RATE_PER_SEC;
        long t0 = System.nanoTime();
        long[] sent = {0};

        // scheduleAtFixedRate catches up after stalls, so the arrival schedule is preserved (open model).
        ScheduledFuture<?> load = scheduler.scheduleAtFixedRate(() -> {
            long intended = t0 + sent[0]++ * periodNanos;
            futures.add(client.submit(newCommand()).whenComplete((o, ex) -> {
                long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - intended);
                synchronized (latencyMs) {
                    latencyMs.recordValue(Math.min(ms, 59_999));
                }
            }));
        }, 0, periodNanos, TimeUnit.NANOSECONDS);

        TimeUnit.SECONDS.sleep(DURATION_SEC / 4);
        ConfigurableApplicationContext extraProcessor = startExtraProcessor();   // scale out => rebalance
        TimeUnit.SECONDS.sleep(DURATION_SEC / 3);
        extraProcessor.close();                                                    // scale in  => rebalance
        TimeUnit.SECONDS.sleep(DURATION_SEC - DURATION_SEC / 4 - DURATION_SEC / 3);

        load.cancel(false);
        scheduler.shutdown();
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get(30, TimeUnit.SECONDS);

        Map<Outcome.Kind, Integer> counts = new EnumMap<>(Outcome.Kind.class);
        List<String> okIds = new ArrayList<>();
        for (CompletableFuture<Outcome> f : futures) {
            Outcome o = f.join();
            counts.merge(o.kind(), 1, Integer::sum);
            if (o.kind() == Outcome.Kind.OK) {
                okIds.add(o.requestId());
            }
        }
        int total = futures.size();
        int timeouts = counts.getOrDefault(Outcome.Kind.TIMEOUT, 0);
        double timeoutRate = total == 0 ? 0 : (double) timeouts / total;

        System.out.printf("%n[rebalance] sent=%d outcomes=%s timeoutRate=%.4f%n", total, counts, timeoutRate);
        System.out.printf("[rebalance] latency ms (from intended send): p50=%d p99=%d p99.9=%d max=%d%n%n",
                latencyMs.getValueAtPercentile(50), latencyMs.getValueAtPercentile(99),
                latencyMs.getValueAtPercentile(99.9), latencyMs.getMaxValue());

        assertThat(counts.getOrDefault(Outcome.Kind.ERROR, 0)).as("errors").isZero();
        for (String id : okIds) {
            assertThat(processedCount(id)).as("OK reply implies committed write for " + id).isEqualTo(1L);
        }
        assertThat(timeoutRate).as("timeout rate during scale-out/in").isLessThanOrEqualTo(MAX_TIMEOUT_RATE);

        Map<String, Integer> perAttempt = new HashMap<>();
        long quietSince = System.nanoTime();
        try (responses) {
            while (System.nanoTime() - quietSince < TimeUnit.SECONDS.toNanos(3)) {
                var batch = responses.poll(Duration.ofMillis(200));
                if (!batch.isEmpty()) {
                    quietSince = System.nanoTime();
                }
                for (ConsumerRecord<String, String> r : batch) {
                    Header corr = r.headers().lastHeader(KafkaHeaders.CORRELATION_ID);
                    if (corr != null) {
                        perAttempt.merge(HexFormat.of().formatHex(corr.value()), 1, Integer::sum);
                    }
                }
            }
        }
        long duplicated = perAttempt.values().stream().filter(n -> n > 1).count();
        System.out.printf("[rebalance] responses=%d attempts-with-duplicate-responses=%d%n",
                perAttempt.values().stream().mapToInt(Integer::intValue).sum(), duplicated);
        assertThat(duplicated).as("request attempts that received more than one response").isZero();
    }

    /** A second processor-only instance joining the same consumer group, like another pod. */
    private static ConfigurableApplicationContext startExtraProcessor() {
        return new SpringApplicationBuilder(RequestReplyApplication.class)
                .profiles("processor")
                .run(
                        "--server.port=0",
                        "--app.instance-id=it-processor-2",
                        "--app.topics.replication-factor=1",
                        "--spring.flyway.enabled=false",
                        "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                        "--spring.datasource.url=" + jdbcUrlViaProxy(),
                        "--spring.datasource.username=" + ORACLE.getUsername(),
                        "--spring.datasource.password=" + ORACLE.getPassword());
    }
}
