# kafka-rr-sla

A reference implementation and test harness for a **synchronous-over-Kafka** flow. A caller sends a request and the processor applies an idempotent **Oracle upsert**. The caller needs an answer within **1000 ms**.

```
HTTP caller ──► requestor ──(payments.request, key=paymentId, deadline header)──► processor ──► Oracle upsert
       ▲             │                                                              │
       └──200/202────┴◄──────(payments.reply, own partition, correlation id)────────┘
```

## Design choices (and why)

| Concern | Choice |
|---|---|
| Request-reply | Spring Kafka `ReplyingKafkaTemplate` + `@SendTo`. Correlation ID handled by the framework. |
| Multiple requestor pods | Each pod is **pinned to its own reply partition** (`REPLY_PARTITION`, e.g. StatefulSet ordinal). There are no rebalances on the reply side and no cross-talk. |
| Exactly-once *effect* | At-least-once delivery + `processed_requests` PK on `requestId`. One PL/SQL block (ledger claim, `MERGE` payment, consumer offset) = 1 round trip + commit. The DB commits before the reply is sent, so a reply always means a durable write. |
| DB switchover / failover | Consumer offsets stored **in the DB, in the same transaction**. After a switch, consumers resume from what the new DB actually has; the ledger skips what it already has. Runbook: [docs/db-switchover.md](docs/db-switchover.md). |
| DB outage | The error handler **never skips** on DB-unavailable errors (capped backoff, retries forever). Bad records go to `<topic>.DLT`. |
| Not wasting work | An absolute **deadline header** is set. The processor drops expired requests and doesn't write them. |
| Timeout semantics | 202 PENDING, **not** failure. The write may still land ("late write"). `sql/reconcile.sql` finds these. |
| Bounded DB time | `oracle.jdbc.ReadTimeout` (5 s backstop), `oracle.net.CONNECT_TIMEOUT` (2 s), Hikari `connection-timeout=500ms`. |
| Rebalance impact | `CooperativeStickyAssignor`. On Kafka 4.x, switch to `group.protocol=consumer` (KIP-848). |
| Fail-fast producer | `request.timeout.ms=1000`, `delivery.timeout.ms=1500`, `max.block.ms=500`, `linger.ms=0`, `acks=all`, idempotence on. |

**Caveat:** if the caller really needs a synchronous answer, the simpler architecture is a sync API that writes to the DB and publishes to Kafka via an outbox. This repo is for when Kafka in the request path is a given.

## Layout

```
src/main/java/.../requestor   REST edge, ReplyClient (non-blocking), audit
src/main/java/.../processor   @KafkaListener, deadline check, idempotent upsert
src/test/java/...             Testcontainers ITs: Kafka (KRaft) + Oracle 23ai Free behind Toxiproxy
k6/scenarios.js               Open-model load: baseline, ramp, spike, soak, hot-key, duplicates, payload size
chaos/                        Chaos Mesh manifests: processor kill, broker kill, DB network delay
scripts/                      run-matrix.sh (load scenarios), chaos-during-load.sh (fault + reconcile)
sql/reconcile.sql             late_writes, unresolved_timeouts, ok_without_write, version_drift, summary
sql/switchover-check.sql      Run on main and shadow before repointing; output must match
docs/db-switchover.md         GoldenGate switchover / failover runbook
scripts/db-switchover.sh      Orchestrates the app-side switchover steps across pods
docs/test-plan.md             Scenario matrix (14 scenarios) with pass criteria and run rules
```

## Running it

**Integration tests** (needs Docker). On Docker Engine 29+, first run
`echo "api.version=1.44" > ~/.docker-java.properties` (Testcontainers 1.21.x otherwise sends an API version Docker rejects).
```bash
mvn verify
# rebalance test knobs
mvn verify -Dit.test=RebalanceUnderLoadIT -Drr.rebalance.rate=100 -Drr.rebalance.maxTimeoutRate=0.02
```

| IT | Scenario(s) |
|---|---|
| `IdempotencyIT` | 12 (duplicates), 10 (ordered updates to the same row) |
| `ExpiredRequestIT` | 14 (stale request dropped, not written) |
| `DbLatencyLateWriteIT` | 7 (DB slowness via Toxiproxy), 13 (reconciliation catches late writes) |
| `ReplyRoutingIT` | 8 (two requestor instances, isolated reply partitions) |
| `RebalanceUnderLoadIT` | 5 (scale processor group out and in under open-model load) |
| `ProcessingLatencyIT` | Benchmark: DB round trips per request under injected latency |
| `DbSwitchoverIT` | 15 (planned switchover and unplanned failover, main → shadow, simulated async GoldenGate) |

**Local stack + load smoke test:**
```bash
mvn -DskipTests package
docker compose up --build
k6 run -e SCENARIO=baseline -e RATE=100 -e DURATION=2m k6/scenarios.js
sql -S payments/payments@localhost:1521/FREEPDB1 @sql/reconcile.sql   # SQLcl or sqlplus
```

**Perf environment (OpenShift):**
```bash
BASE_URL=http://<requestor-route> RATE=<peak> MAX_RATE=<5x peak> ./scripts/run-matrix.sh
ACTION=rollout     BASE_URL=... PSQL=... ./scripts/chaos-during-load.sh
ACTION=broker-kill BASE_URL=... PSQL=... ./scripts/chaos-during-load.sh
```

Deploy requestor and processor as separate Deployments (`SPRING_PROFILES_ACTIVE=requestor|processor`). For requestors, use a StatefulSet and derive `REPLY_PARTITION` from the pod ordinal. Keep `reply-partitions` ≥ requestor replicas.

## Key settings

| Property | Default | Notes |
|---|---|---|
| `app.sla-ms` | 1000 | Deadline stamped on each request. |
| `app.reply-timeout-ms` | 900 | Leaves about 100 ms for HTTP and serialization. |
| `app.reply-partition` | 0 | **Unique per requestor pod.** |
| `app.processor.concurrency` | 6 | ≤ request partitions (12). |
| `app.processor.group-id` | payment-processor | Consumer group of the request listener. |
| `DB_READ_TIMEOUT_MS` / `DB_CONNECT_TIMEOUT_MS` | 5000 / 2000 | Oracle driver socket timeouts. |
| `app.audit.enabled` | false | Turn on in perf runs for reconciliation. |

## Known limitations

- **`processed_requests` grows without bound.** Add a purge job for rows older than your max redelivery window; the soak test will show it if you don't.
- **The deadline check relies on NTP-synced clocks.** Clock skew eats into the budget directly.
- **The chaos manifests assume Chaos Mesh and Strimzi labels.** `chaos-during-load.sh` has `delete-pod`, `rollout` and `scale` fallbacks that need only `oc`.
