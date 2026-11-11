package com.example.rr;

import static com.example.rr.AbstractIntegrationTest.KAFKA;
import static com.example.rr.AbstractIntegrationTest.NETWORK;
import static com.example.rr.AbstractIntegrationTest.ORACLE;
import static com.example.rr.AbstractIntegrationTest.ORACLE_IMAGE;
import static com.example.rr.AbstractIntegrationTest.ORACLE_PASSWORD;
import static com.example.rr.AbstractIntegrationTest.TOXI_CLIENT;
import static com.example.rr.AbstractIntegrationTest.newCommand;
import static com.example.rr.AbstractIntegrationTest.oracleUrlViaProxy;
import static com.example.rr.AbstractIntegrationTest.reconcileQuery;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.rr.model.Outcome;
import com.example.rr.requestor.ReplyClient;
import eu.rekawek.toxiproxy.Proxy;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Scenario 15: DB switchover main -> shadow with GoldenGate-style asynchronous replication.
 *
 * <p>Topology: the app reaches Oracle through a Toxiproxy endpoint (stands in for the DNS / TNS alias that is
 * repointed at switchover). {@link SnapshotReplicator} stands in for GoldenGate: it applies transactionally
 * consistent snapshots of main (flashback query AS OF SCN) to the shadow with a configurable lag, i.e. the
 * shadow is always a consistent but stale copy of main, which is what async GoldenGate gives you.
 *
 * <p>Runs in its own Spring context, schema (SWAPP), topics and consumer group, so fencing main can't affect
 * other tests. Real GoldenGate behaviour (LOGEND, Replicat EOF, parallel apply) must still be proven in the
 * perf environment; this test proves the APPLICATION's guarantees: nothing lost, nothing applied twice.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"requestor", "processor"})
@DirtiesContext
class DbSwitchoverIT {

    private static final String APP_USER = "swapp";
    private static final String APP_PASSWORD = "swapp";
    private static final int SWITCH_PROXY_PORT = 8667;
    private static final String[] REPLICATED_TABLES = {"payments", "processed_requests", "consumer_offsets"};

    static final OracleContainer SHADOW = new OracleContainer(DockerImageName.parse(ORACLE_IMAGE))
            .withPassword(ORACLE_PASSWORD)
            .withEnv("ORACLE_PASSWORD", ORACLE_PASSWORD)
            .withNetwork(NETWORK)
            .withNetworkAliases("oracle-shadow");

    static final Proxy SWITCH_PROXY;

    static {
        KAFKA.getBootstrapServers();   // shared containers are started by AbstractIntegrationTest's initializer
        SHADOW.start();
        createAppUser(ORACLE);
        createAppUser(SHADOW);
        // Shadow gets the same schema up front (as it would in a GoldenGate target).
        Flyway.configure().dataSource(SHADOW.getJdbcUrl(), APP_USER, APP_PASSWORD).load().migrate();
        try {
            SWITCH_PROXY = TOXI_CLIENT.createProxy("oracle-switch", "0.0.0.0:" + SWITCH_PROXY_PORT, "oracle-main:1521");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.datasource.url", () -> oracleUrlViaProxy(SWITCH_PROXY_PORT));
        registry.add("spring.datasource.username", () -> APP_USER);
        registry.add("spring.datasource.password", () -> APP_PASSWORD);
        registry.add("app.topics.request", () -> "switch.request");
        registry.add("app.topics.reply", () -> "switch.reply");
        registry.add("app.topics.replication-factor", () -> "1");
        registry.add("app.processor.group-id", () -> "switch-processor");
        registry.add("app.instance-id", () -> "it-switch");
        registry.add("app.audit.enabled", () -> "false");
    }

    @Autowired
    TestRestTemplate rest;

    @Autowired
    ReplyClient client;

    @Autowired
    MeterRegistry meters;

    private final JdbcTemplate mainSys = new JdbcTemplate(
            new DriverManagerDataSource(ORACLE.getJdbcUrl(), "system", ORACLE_PASSWORD));
    private final JdbcTemplate shadowApp = new JdbcTemplate(
            new DriverManagerDataSource(SHADOW.getJdbcUrl(), APP_USER, APP_PASSWORD));

    private SnapshotReplicator replicator;

    /** Every test starts on main, unfenced, with empty tables, processing running and intake open. */
    @BeforeEach
    void resetToMain() throws Exception {
        mainSys.execute("ALTER USER " + APP_USER + " ACCOUNT UNLOCK");
        SWITCH_PROXY.setUpstream("oracle-main:1521");
        for (String table : REPLICATED_TABLES) {
            mainSys.update("DELETE FROM " + APP_USER + "." + table);
            shadowApp.update("DELETE FROM " + table);
        }
        post("/admin/db/evict-pool");
        post("/admin/processing/resume");
        post("/admin/intake/open");
        await().atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofMillis(500)).ignoreExceptions().until(() ->
                client.submit(newCommand("warmup-" + UUID.randomUUID())).get(5, TimeUnit.SECONDS).kind()
                        == Outcome.Kind.OK);
    }

    @AfterEach
    void stopReplicator() {
        if (replicator != null) {
            replicator.stop();
        }
    }

    /** Runbook: intake closed -> pause -> fence -> wait for replication to drain -> repoint -> resume. */
    @Test
    void plannedSwitchoverLosesNothingAndAppliesNothingTwice() throws Exception {
        double replayedBefore = replayed();
        replicator = new SnapshotReplicator(Duration.ofMillis(500));
        replicator.start();

        OpenLoad before = OpenLoad.start(client, 30);
        TimeUnit.SECONDS.sleep(6);

        // 1. Close intake: callers get 503 + Retry-After instead of a request queued past its deadline.
        post("/admin/intake/close");
        assertThat(rest.postForEntity("/payments", newCommand(), String.class).getStatusCode())
                .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        List<Outcome> beforeOutcomes = before.stopAndCollect();   // the generator bypasses the gate; stop it too

        // 2. Pause processors and wait until idle.
        post("/admin/processing/pause");
        awaitPaused();

        // 3. Fence main so no straggler can write there after the switch.
        fenceMain();

        // 4-5. Wait for replication to drain, verified at the application level: offsets identical on both sides.
        await().atMost(Duration.ofSeconds(30)).until(() -> offsets(mainSys, APP_USER + ".").equals(offsets(shadowApp, "")));
        replicator.stop();

        // 7. Repoint the endpoint and drop pooled connections to the old primary.
        SWITCH_PROXY.setUpstream("oracle-shadow:1521");
        post("/admin/db/evict-pool");

        // 8. Resume and reopen.
        post("/admin/processing/resume");
        post("/admin/intake/open");
        OpenLoad after = OpenLoad.start(client, 30);
        TimeUnit.SECONDS.sleep(4);
        List<Outcome> afterOutcomes = after.stopAndCollect();

        Set<String> ok = okIds(beforeOutcomes, afterOutcomes);
        print("planned", beforeOutcomes, afterOutcomes);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(shadowRequestIds()).as("every OK is durable on the shadow").containsAll(ok));
        assertThat(shadowApp.queryForList(reconcileQuery("version_drift"), String.class))
                .as("no request applied twice").isEmpty();
        assertThat(errors(beforeOutcomes, afterOutcomes)).isZero();
        assertThat(replayed() - replayedBefore).as("zero lag at switch => nothing to replay").isZero();
    }

    /**
     * Main dies with ~2s of commits not yet replicated. Those writes (some already answered OK) exist only in
     * Kafka. Resync seeks to the shadow's stored offsets and replays them; the ledger skips what the shadow has.
     */
    @Test
    void unplannedFailoverReplaysWritesLostInReplicationLag() throws Exception {
        double replayedBefore = replayed();
        replicator = new SnapshotReplicator(Duration.ofSeconds(2));
        replicator.start();

        OpenLoad load = OpenLoad.start(client, 30);
        TimeUnit.SECONDS.sleep(8);

        // Main dies: replication freezes where it is, and the app can no longer write there.
        replicator.stop();
        fenceMain();

        // Operator: pause, repoint, evict, resync from the shadow's offsets, resume. Load keeps arriving throughout.
        post("/admin/processing/pause");
        awaitPaused();
        SWITCH_PROXY.setUpstream("oracle-shadow:1521");
        post("/admin/db/evict-pool");
        Map<?, ?> resync = post("/admin/processing/resync");
        System.out.printf("[failover] resync: %s%n", resync);
        post("/admin/processing/resume");

        TimeUnit.SECONDS.sleep(4);
        List<Outcome> outcomes = load.stopAndCollect();

        Set<String> ok = okIds(outcomes);
        print("unplanned", outcomes);
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                assertThat(shadowRequestIds()).as("every OK is durable on the shadow after replay").containsAll(ok));
        assertThat(shadowApp.queryForList(reconcileQuery("version_drift"), String.class))
                .as("no request applied twice").isEmpty();
        assertThat(errors(outcomes)).isZero();
        assertThat(replayed() - replayedBefore).as("records lost in the lag window were replayed").isPositive();
    }

    // ---------------------------------------------------------------- helpers

    private static void createAppUser(OracleContainer db) {
        JdbcTemplate sys = new JdbcTemplate(new DriverManagerDataSource(db.getJdbcUrl(), "system", ORACLE_PASSWORD));
        sys.execute("CREATE USER " + APP_USER + " IDENTIFIED BY " + APP_PASSWORD
                + " DEFAULT TABLESPACE users QUOTA UNLIMITED ON users");
        sys.execute("GRANT CREATE SESSION, CREATE TABLE TO " + APP_USER);
    }

    /** Lock the app account and kill its sessions on main (GoldenGate Extract is unaffected: it reads redo). */
    private void fenceMain() {
        mainSys.execute("ALTER USER " + APP_USER + " ACCOUNT LOCK");
        List<long[]> sessions = mainSys.query("SELECT sid, serial# FROM v$session WHERE username = ?",
                (rs, n) -> new long[] {rs.getLong(1), rs.getLong(2)}, APP_USER.toUpperCase());
        for (long[] s : sessions) {
            try {
                mainSys.execute("ALTER SYSTEM KILL SESSION '" + s[0] + "," + s[1] + "' IMMEDIATE");
            } catch (RuntimeException alreadyGone) {
                // ORA-00030/00031: session already ended or marked for kill
            }
        }
    }

    private Map<String, Long> offsets(JdbcTemplate jdbc, String schemaPrefix) {
        Map<String, Long> out = new HashMap<>();
        jdbc.query("SELECT topic, partition_id, next_offset FROM " + schemaPrefix + "consumer_offsets",
                rs -> {
                    out.put(rs.getString(1) + "-" + rs.getInt(2), rs.getLong(3));
                });
        return out;
    }

    private Set<String> shadowRequestIds() {
        return new HashSet<>(shadowApp.queryForList("SELECT request_id FROM processed_requests", String.class));
    }

    private void awaitPaused() {
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(250)).until(() ->
                Boolean.TRUE.equals(rest.getForObject("/admin/processing/status", Map.class).get("paused")));
    }

    private Map<?, ?> post(String path) {
        return rest.postForObject(path, null, Map.class);
    }

    private double replayed() {
        return meters.get("rr.processor.replayed").counter().count();
    }

    @SafeVarargs
    private static Set<String> okIds(List<Outcome>... runs) {
        Set<String> ids = new HashSet<>();
        for (List<Outcome> run : runs) {
            run.stream().filter(o -> o.kind() == Outcome.Kind.OK || o.kind() == Outcome.Kind.DUPLICATE)
                    .forEach(o -> ids.add(o.requestId()));
        }
        return ids;
    }

    @SafeVarargs
    private static long errors(List<Outcome>... runs) {
        long n = 0;
        for (List<Outcome> run : runs) {
            n += run.stream().filter(o -> o.kind() == Outcome.Kind.ERROR).count();
        }
        return n;
    }

    @SafeVarargs
    private static void print(String label, List<Outcome>... runs) {
        Map<Outcome.Kind, Integer> counts = new HashMap<>();
        for (List<Outcome> run : runs) {
            run.forEach(o -> counts.merge(o.kind(), 1, Integer::sum));
        }
        System.out.printf("%n[%s] outcomes=%s%n%n", label, counts);
    }

    /** Fixed-arrival-rate load through the requestor's ReplyClient (open model). */
    static final class OpenLoad {
        private final ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        private final List<CompletableFuture<Outcome>> futures = Collections.synchronizedList(new ArrayList<>());

        static OpenLoad start(ReplyClient client, int perSecond) {
            OpenLoad load = new OpenLoad();
            load.exec.scheduleAtFixedRate(() -> load.futures.add(client.submit(newCommand())),
                    0, TimeUnit.SECONDS.toNanos(1) / perSecond, TimeUnit.NANOSECONDS);
            return load;
        }

        List<Outcome> stopAndCollect() throws Exception {
            exec.shutdownNow();
            exec.awaitTermination(5, TimeUnit.SECONDS);
            List<CompletableFuture<Outcome>> all;
            synchronized (futures) {
                all = new ArrayList<>(futures);
            }
            CompletableFuture.allOf(all.toArray(CompletableFuture[]::new)).get(30, TimeUnit.SECONDS);
            return all.stream().map(CompletableFuture::join).toList();
        }
    }

    /**
     * Stand-in for asynchronous GoldenGate: every 100ms, take main's SCN; apply to the shadow the newest SCN that
     * is at least {@code lag} old, as ONE transaction containing a flashback snapshot (AS OF SCN) of all
     * replicated tables. The shadow is therefore always a consistent prefix of main, lagging by {@code lag}.
     */
    final class SnapshotReplicator {
        private final Duration lag;
        private final SingleConnectionDataSource mainDs =
                new SingleConnectionDataSource(ORACLE.getJdbcUrl(), "system", ORACLE_PASSWORD, true);
        private final SingleConnectionDataSource shadowDs =
                new SingleConnectionDataSource(SHADOW.getJdbcUrl(), APP_USER, APP_PASSWORD, true);
        private final JdbcTemplate main = new JdbcTemplate(mainDs);
        private final JdbcTemplate shadow = new JdbcTemplate(shadowDs);
        private final TransactionTemplate shadowTx = new TransactionTemplate(new DataSourceTransactionManager(shadowDs));
        private final ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        private final Deque<long[]> scnHistory = new ArrayDeque<>();
        private long appliedScn = -1;
        private volatile boolean stopped;

        SnapshotReplicator(Duration lag) {
            this.lag = lag;
        }

        void start() {
            exec.scheduleWithFixedDelay(() -> {
                try {
                    tick();
                } catch (RuntimeException e) {
                    System.out.println("[replicator] tick failed: " + e);
                }
            }, 0, 100, TimeUnit.MILLISECONDS);
        }

        void stop() {
            if (stopped) {
                return;
            }
            stopped = true;
            exec.shutdown();
            try {
                exec.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            mainDs.destroy();
            shadowDs.destroy();
        }

        private void tick() {
            long now = System.currentTimeMillis();
            Long scn = main.queryForObject("SELECT current_scn FROM v$database", Long.class);
            scnHistory.addLast(new long[] {now, scn});
            Long target = null;
            while (!scnHistory.isEmpty() && scnHistory.peekFirst()[0] <= now - lag.toMillis()) {
                target = scnHistory.pollFirst()[1];
            }
            if (target != null && target > appliedScn) {
                applySnapshot(target);
                appliedScn = target;
            }
        }

        private void applySnapshot(long scn) {
            String asOf = " AS OF SCN " + scn;
            List<Object[]> payments = main.query(
                    "SELECT payment_id, amount, currency, status, version, last_request_id, updated_at FROM "
                            + APP_USER + ".payments" + asOf,
                    (rs, n) -> new Object[] {rs.getString(1), rs.getBigDecimal(2), rs.getString(3), rs.getString(4),
                            rs.getLong(5), rs.getString(6), rs.getTimestamp(7)});
            List<Object[]> ledger = main.query(
                    "SELECT request_id, payment_id, result_version, processed_at FROM "
                            + APP_USER + ".processed_requests" + asOf,
                    (rs, n) -> new Object[] {rs.getString(1), rs.getString(2), rs.getLong(3), rs.getTimestamp(4)});
            List<Object[]> offsets = main.query(
                    "SELECT topic, partition_id, next_offset, updated_at FROM " + APP_USER + ".consumer_offsets" + asOf,
                    (rs, n) -> new Object[] {rs.getString(1), rs.getInt(2), rs.getLong(3), rs.getTimestamp(4)});

            shadowTx.executeWithoutResult(status -> {
                for (String table : REPLICATED_TABLES) {
                    shadow.update("DELETE FROM " + table);
                }
                if (!payments.isEmpty()) {
                    shadow.batchUpdate("INSERT INTO payments (payment_id, amount, currency, status, version, "
                            + "last_request_id, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)", payments);
                }
                if (!ledger.isEmpty()) {
                    shadow.batchUpdate("INSERT INTO processed_requests (request_id, payment_id, result_version, "
                            + "processed_at) VALUES (?, ?, ?, ?)", ledger);
                }
                if (!offsets.isEmpty()) {
                    shadow.batchUpdate("INSERT INTO consumer_offsets (topic, partition_id, next_offset, updated_at) "
                            + "VALUES (?, ?, ?, ?)", offsets);
                }
            });
        }
    }
}
