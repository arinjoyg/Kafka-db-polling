package com.example.rr;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.rr.model.Outcome;
import com.example.rr.model.PaymentCommand;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Scenarios 7 + 13: DB degradation and timeout reconciliation.
 *
 * With +700ms per DB round trip the transaction (PL/SQL apply + commit) needs ~1.4s, so the caller times out
 * at 900ms while the processor – which passed the deadline check on arrival – still commits. That is a "late write": caller told PENDING, data actually written.
 * The test proves the reconciliation query in sql/reconcile.sql catches it.
 */
class DbLatencyLateWriteIT extends AbstractIntegrationTest {

    @Test
    void slowDatabaseProducesTimeoutThatReconciliationDetectsAsLateWrite() throws Exception {
        DB_PROXY.toxics().latency("db-latency", ToxicDirection.DOWNSTREAM, 700);

        PaymentCommand cmd = newCommand();
        Outcome outcome = client.submit(cmd).get(5, TimeUnit.SECONDS);
        assertThat(outcome.kind()).isEqualTo(Outcome.Kind.TIMEOUT);

        DB_PROXY.toxics().get("db-latency").remove();

        String lateWritesSql = reconcileQuery("late_writes");
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            List<String> late = jdbc.queryForList(lateWritesSql, String.class);
            assertThat(late).contains(cmd.requestId());
        });
    }

    @Test
    void recoversWithinSlaOnceDbLatencyIsRemoved() throws Exception {
        DB_PROXY.toxics().latency("db-latency", ToxicDirection.DOWNSTREAM, 700);
        client.submit(newCommand()).get(5, TimeUnit.SECONDS);
        DB_PROXY.toxics().get("db-latency").remove();

        await().atMost(Duration.ofSeconds(20)).until(() ->
                client.submit(newCommand()).get(5, TimeUnit.SECONDS).kind() == Outcome.Kind.OK);
    }

}
