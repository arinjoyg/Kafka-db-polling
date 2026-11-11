package com.example.rr;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.rr.model.Outcome;
import com.example.rr.model.PaymentCommand;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Scenarios 7 + 13: DB degradation and timeout reconciliation.
 *
 * With +400ms per DB round trip the transaction (SET LOCAL x2, claim, upsert, update, commit) needs
 * ~2.4s, so the caller times out at 900ms while the processor – which passed the deadline check on
 * arrival – still commits. That is a "late write": caller told PENDING, data actually written.
 * The test proves the reconciliation query in sql/reconcile.sql catches it.
 */
class DbLatencyLateWriteIT extends AbstractIntegrationTest {

    @Test
    void slowDatabaseProducesTimeoutThatReconciliationDetectsAsLateWrite() throws Exception {
        PG_PROXY.toxics().latency("pg-latency", ToxicDirection.DOWNSTREAM, 400);

        PaymentCommand cmd = newCommand();
        Outcome outcome = client.submit(cmd).get(5, TimeUnit.SECONDS);
        assertThat(outcome.kind()).isEqualTo(Outcome.Kind.TIMEOUT);

        PG_PROXY.toxics().get("pg-latency").remove();

        String lateWritesSql = extractQuery("late_writes");
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            List<String> late = jdbc.queryForList(lateWritesSql, String.class);
            assertThat(late).contains(cmd.requestId());
        });
    }

    @Test
    void recoversWithinSlaOnceDbLatencyIsRemoved() throws Exception {
        PG_PROXY.toxics().latency("pg-latency", ToxicDirection.DOWNSTREAM, 400);
        client.submit(newCommand()).get(5, TimeUnit.SECONDS);
        PG_PROXY.toxics().get("pg-latency").remove();

        await().atMost(Duration.ofSeconds(20)).until(() ->
                client.submit(newCommand()).get(5, TimeUnit.SECONDS).kind() == Outcome.Kind.OK);
    }

    /** Pulls the query tagged "-- name: <tag>" out of sql/reconcile.sql so test and runbook can't drift. */
    private static String extractQuery(String tag) throws Exception {
        String all = Files.readString(Path.of("sql/reconcile.sql"));
        String marker = "-- name: " + tag;
        int start = all.indexOf(marker);
        if (start < 0) {
            throw new IllegalStateException("query " + tag + " not found in sql/reconcile.sql");
        }
        int bodyStart = all.indexOf('\n', start) + 1;
        int end = all.indexOf(';', bodyStart);
        return all.substring(bodyStart, end);
    }
}
