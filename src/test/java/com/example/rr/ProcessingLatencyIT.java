package com.example.rr;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.rr.model.Outcome;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Benchmark, not a gate: injects a fixed latency on every Postgres round trip (simulating a DB in another
 * zone) and reports how much of the per-request time is DB round trips. Use it to compare processor
 * implementations. Knob: -Drr.bench.rttMs=3 -Drr.bench.requests=200
 */
class ProcessingLatencyIT extends AbstractIntegrationTest {

    private static final int RTT_MS = Integer.getInteger("rr.bench.rttMs", 3);
    private static final int REQUESTS = Integer.getInteger("rr.bench.requests", 200);

    @Autowired
    MeterRegistry registry;

    @Test
    void reportsDbRoundTripCostPerRequest() throws Exception {
        Timer processing = registry.get("rr.processor.processing").timer();
        PG_PROXY.toxics().latency("pg-rtt", ToxicDirection.DOWNSTREAM, RTT_MS);

        for (int i = 0; i < 20; i++) {   // warm the toxic path and JIT
            client.submit(newCommand()).get(5, TimeUnit.SECONDS);
        }

        long count0 = processing.count();
        double total0 = processing.totalTime(TimeUnit.MILLISECONDS);
        Histogram roundTrip = new Histogram(10_000, 3);

        for (int i = 0; i < REQUESTS; i++) {
            Outcome o = client.submit(newCommand()).get(5, TimeUnit.SECONDS);
            assertThat(o.kind()).isEqualTo(Outcome.Kind.OK);
            roundTrip.recordValue(Math.min(o.latencyMs(), 9_999));
        }

        double meanProcessingMs =
                (processing.totalTime(TimeUnit.MILLISECONDS) - total0) / (processing.count() - count0);
        System.out.printf("%n[db-rtt] injected=%dms/round-trip requests=%d processing.mean=%.1fms "
                        + "(~%.1f round trips) end-to-end p50=%dms p99=%dms%n%n",
                RTT_MS, REQUESTS, meanProcessingMs, meanProcessingMs / RTT_MS,
                roundTrip.getValueAtPercentile(50), roundTrip.getValueAtPercentile(99));
    }
}
