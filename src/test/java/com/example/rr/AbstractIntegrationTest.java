package com.example.rr;

import static org.awaitility.Awaitility.await;

import com.example.rr.model.Outcome;
import com.example.rr.model.PaymentCommand;
import com.example.rr.requestor.ReplyClient;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.Toxic;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;
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
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

/**
 * Real Kafka (KRaft) + real Postgres, with Postgres reached THROUGH Toxiproxy so tests can inject
 * latency / connection cuts on the DB path. Containers are shared across all ITs (singleton pattern).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"requestor", "processor"})
public abstract class AbstractIntegrationTest {

    protected static final Network NETWORK = Network.newNetwork();

    protected static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"));

    protected static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withNetwork(NETWORK)
                    .withNetworkAliases("postgres");

    protected static final ToxiproxyContainer TOXIPROXY =
            new ToxiproxyContainer(DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.5.0"))
                    .withNetwork(NETWORK);

    /** Toxiproxy exposes 8666-8697 for proxies; we use the first one for Postgres. */
    private static final int PG_PROXY_PORT = 8666;

    protected static final Proxy PG_PROXY;

    static {
        Startables.deepStart(KAFKA, POSTGRES, TOXIPROXY).join();
        try {
            var client = new ToxiproxyClient(TOXIPROXY.getHost(), TOXIPROXY.getControlPort());
            PG_PROXY = client.createProxy("postgres", "0.0.0.0:" + PG_PROXY_PORT, "postgres:5432");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static volatile boolean warmedUp;

    @Autowired
    protected TestRestTemplate rest;

    @Autowired
    protected JdbcTemplate jdbc;

    @Autowired
    protected ReplyClient client;

    protected static String jdbcUrlViaProxy() {
        return "jdbc:postgresql://" + TOXIPROXY.getHost() + ":" + TOXIPROXY.getMappedPort(PG_PROXY_PORT)
                + "/" + POSTGRES.getDatabaseName();
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.datasource.url", AbstractIntegrationTest::jdbcUrlViaProxy);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("app.topics.replication-factor", () -> "1");
        registry.add("app.audit.enabled", () -> "true");
        registry.add("app.instance-id", () -> "it-main");
    }

    /**
     * Cold start (topic creation, partition assignment, first metadata fetch) routinely exceeds 1s.
     * That is itself a finding: readiness probes must not pass until a probe round-trip succeeds.
     */
    @BeforeEach
    void warmUpOnce() {
        if (warmedUp) {
            return;
        }
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(500)).until(() ->
                client.submit(newCommand("warmup-" + UUID.randomUUID())).get(5, TimeUnit.SECONDS).kind()
                        == Outcome.Kind.OK);
        warmedUp = true;
    }

    @AfterEach
    void removeToxics() throws IOException {
        for (Toxic toxic : PG_PROXY.toxics().getAll()) {
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
}
