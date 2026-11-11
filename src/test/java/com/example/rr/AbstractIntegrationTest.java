package com.example.rr;

import static org.awaitility.Awaitility.await;

import com.example.rr.config.AppProperties;
import com.example.rr.model.Outcome;
import com.example.rr.model.PaymentCommand;
import com.example.rr.requestor.ReplyClient;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.Toxic;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Real Kafka (KRaft) + real Oracle (23ai Free), with Oracle reached THROUGH Toxiproxy so tests can inject
 * latency, cut connections, or repoint the endpoint (DB switchover). Containers are shared across all ITs.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"requestor", "processor"})
public abstract class AbstractIntegrationTest {

    protected static final Network NETWORK = Network.newNetwork();

    protected static final String ORACLE_IMAGE = "gvenzl/oracle-free:23-slim-faststart";

    /** Same password for the app user and SYS/SYSTEM, so tests can also connect as SYSTEM. */
    protected static final String ORACLE_PASSWORD = "test";

    protected static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"));

    protected static final OracleContainer ORACLE =
            new OracleContainer(DockerImageName.parse(ORACLE_IMAGE))
                    .withPassword(ORACLE_PASSWORD)
                    .withEnv("ORACLE_PASSWORD", ORACLE_PASSWORD)
                    .withNetwork(NETWORK)
                    .withNetworkAliases("oracle-main");

    protected static final ToxiproxyContainer TOXIPROXY =
            new ToxiproxyContainer(DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.5.0"))
                    .withNetwork(NETWORK);

    static final ToxiproxyClient TOXI_CLIENT;

    /** Toxiproxy exposes 8666-8697 for proxies; 8666 = the shared DB proxy. */
    private static final int DB_PROXY_PORT = 8666;

    protected static final Proxy DB_PROXY;

    static {
        Startables.deepStart(KAFKA, ORACLE, TOXIPROXY).join();
        try {
            TOXI_CLIENT = new ToxiproxyClient(TOXIPROXY.getHost(), TOXIPROXY.getControlPort());
            DB_PROXY = TOXI_CLIENT.createProxy("oracle", "0.0.0.0:" + DB_PROXY_PORT, "oracle-main:1521");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static final Set<String> WARMED_UP = ConcurrentHashMap.newKeySet();

    @Autowired
    protected TestRestTemplate rest;

    @Autowired
    protected JdbcTemplate jdbc;

    @Autowired
    protected ReplyClient client;

    @Autowired
    protected AppProperties appProperties;

    protected static String oracleUrlViaProxy(int proxyPort) {
        return "jdbc:oracle:thin:@" + TOXIPROXY.getHost() + ":" + TOXIPROXY.getMappedPort(proxyPort)
                + "/" + ORACLE.getDatabaseName();
    }

    protected static String jdbcUrlViaProxy() {
        return oracleUrlViaProxy(DB_PROXY_PORT);
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.datasource.url", AbstractIntegrationTest::jdbcUrlViaProxy);
        registry.add("spring.datasource.username", ORACLE::getUsername);
        registry.add("spring.datasource.password", ORACLE::getPassword);
        registry.add("app.topics.replication-factor", () -> "1");
        registry.add("app.audit.enabled", () -> "true");
        registry.add("app.instance-id", () -> "it-main");
    }

    /**
     * Cold start (topic creation, partition assignment, first metadata fetch) routinely exceeds 1s.
     * That is itself a finding: readiness probes must not pass until a probe round-trip succeeds.
     */
    @BeforeEach
    void warmUpOncePerContext() {
        if (WARMED_UP.contains(appProperties.instanceId())) {
            return;
        }
        await().atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofMillis(500)).until(() ->
                client.submit(newCommand("warmup-" + UUID.randomUUID())).get(5, TimeUnit.SECONDS).kind()
                        == Outcome.Kind.OK);
        WARMED_UP.add(appProperties.instanceId());
    }

    @AfterEach
    void removeToxics() throws IOException {
        for (Toxic toxic : DB_PROXY.toxics().getAll()) {
            toxic.remove();
        }
    }

    protected static PaymentCommand newCommand(String paymentId) {
        return new PaymentCommand(UUID.randomUUID().toString(), paymentId, new BigDecimal("100.00"), "USD", "it");
    }

    protected static PaymentCommand newCommand() {
        return newCommand("pay-" + UUID.randomUUID());
    }

    protected long processedCount(String requestId) {
        Long n = jdbc.queryForObject(
                "SELECT count(*) FROM processed_requests WHERE request_id = ?", Long.class, requestId);
        return n == null ? 0 : n;
    }

    /** Pulls the query tagged "-- name: <tag>" out of sql/reconcile.sql so tests and the runbook can't drift. */
    protected static String reconcileQuery(String tag) {
        try {
            String all = Files.readString(Path.of("sql/reconcile.sql"));
            int start = all.indexOf("-- name: " + tag);
            if (start < 0) {
                throw new IllegalStateException("query " + tag + " not found in sql/reconcile.sql");
            }
            StringBuilder sql = new StringBuilder();
            for (String line : all.substring(all.indexOf('\n', start) + 1).split("\n")) {
                if (line.stripLeading().startsWith("--")) {
                    continue;   // comments may contain ';'
                }
                int semi = line.indexOf(';');
                if (semi >= 0) {
                    return sql.append(line, 0, semi).toString();
                }
                sql.append(line).append('\n');
            }
            throw new IllegalStateException("query " + tag + " has no terminating ';'");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
