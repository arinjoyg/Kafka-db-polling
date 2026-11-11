package com.example.rr.requestor;

import com.example.rr.config.AppProperties;
import com.example.rr.model.Outcome;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Test-harness concern: records what the caller was told, so sql/reconcile.sql can find
 * timeouts that later committed ("late writes"). Written asynchronously, off the latency path.
 * Disabled by default; enable in perf environments with app.audit.enabled=true.
 */
@Repository
@Profile("requestor")
public class AuditRepository implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(AuditRepository.class);

    private final JdbcTemplate jdbc;
    private final AppProperties props;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public AuditRepository(JdbcTemplate jdbc, AppProperties props) {
        this.jdbc = jdbc;
        this.props = props;
    }

    public void record(Outcome outcome, String paymentId) {
        if (!props.audit().enabled()) {
            return;
        }
        executor.execute(() -> {
            try {
                jdbc.update("""
                        MERGE INTO requestor_audit a
                        USING (SELECT ? AS request_id, ? AS payment_id, ? AS outcome, ? AS latency_ms,
                                      ? AS instance_id FROM dual) s
                           ON (a.request_id = s.request_id)
                        WHEN MATCHED THEN UPDATE
                             SET a.outcome = s.outcome, a.latency_ms = s.latency_ms, a.recorded_at = SYSTIMESTAMP
                        WHEN NOT MATCHED THEN INSERT (request_id, payment_id, outcome, latency_ms, instance_id, recorded_at)
                             VALUES (s.request_id, s.payment_id, s.outcome, s.latency_ms, s.instance_id, SYSTIMESTAMP)
                        """, outcome.requestId(), paymentId, outcome.kind().name(), outcome.latencyMs(),
                        props.instanceId());
            } catch (RuntimeException e) {
                log.warn("Audit write failed for {}: {}", outcome.requestId(), e.toString());
            }
        });
    }

    @Override
    public void destroy() {
        executor.close();
    }
}
