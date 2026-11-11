# kafka-rr-sla

A reference implementation and test harness for a **synchronous-over-Kafka** flow. A caller sends a request and the processor applies an idempotent **DB upsert**. The caller needs an answer within **1000 ms**.

```
HTTP caller ──► requestor ──(payments.request, key=paymentId, deadline header)──► processor ──► Postgres upsert
       ▲             │                                                              │
       └──200/202────┴◄──────(payments.reply, own partition, correlation id)────────┘
```

## Design choices (and why)

| Concern | Choice |
|---|---|
| Request-reply | Spring Kafka `ReplyingKafkaTemplate` + `@SendTo`. Correlation ID handled by the framework. |
| Multiple requestor pods | Each pod is **pinned to its own reply partition** (`REPLY_PARTITION`, e.g. StatefulSet ordinal). There are no rebalances on the reply side and no cross-talk. |
| Exactly-once *effect* | At-least-once delivery + `processed_requests` PK on `requestId` + `INSERT … ON CONFLICT DO UPDATE`. The DB commits before the reply is sent, so a reply always means a durable write. |
| Not wasting work | An absolute **deadline header** is set. The processor drops expired requests and doesn't write them. |
| Timeout semantics | 202 PENDING, **not** failure. The write may still land ("late write"). `sql/reconcile.sql` finds these. |
| Bounded DB time | `SET LOCAL lock_timeout/statement_timeout`, Hikari `connection-timeout=250ms`. |
| Rebalance impact | `CooperativeStickyAssignor`. On Kafka 4.x, switch to `group.protocol=consumer` (KIP-848). |
| Fail-fast producer | `request.timeout.ms=1000`, `delivery.timeout.ms=1500`, `max.block.ms=500`, `linger.ms=0`, `acks=all`, idempotence on. |

**Caveat:** if the caller really needs a synchronous answer, the simpler architecture is a sync API that writes to the DB and publishes to Kafka via an outbox. This repo is for when Kafka in the request path is a given.

## Layout

```
src/main/java/.../requestor   REST edge, ReplyClient (non-blocking), audit
src/main/java/.../processor   @KafkaListener, deadline check, idempotent upsert
src/test/java/...             Testcontainers ITs: Kafka (KRaft) + Postgres behind Toxiproxy
k6/scenarios.js               Open-model load: baseline, ramp, spike, soak, hot-key, duplicates, payload size
chaos/                        Chaos Mesh manifests: processor kill, broker kill, DB network delay
scripts/                      run-matrix.sh (load scenarios), chaos-during-load.sh (fault + reconcile)
sql/reconcile.sql             late_writes, unresolved_timeouts, ok_without_write, version_drift, summary
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

**Local stack + load smoke test:**
```bash
mvn -DskipTests package
docker compose up --build
k6 run -e SCENARIO=baseline -e RATE=100 -e DURATION=2m k6/scenarios.js
psql postgresql://payments:payments@localhost:5432/payments -f sql/reconcile.sql
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
| `app.db.lock-timeout-ms` / `statement-timeout-ms` | 300 / 500 | |
| `app.audit.enabled` | false | Turn on in perf runs for reconciliation. |

## Known limitations

- **`processed_requests` grows without bound.** Add a purge job for rows older than your max redelivery window; the soak test will show it if you don't.
- **The deadline check relies on NTP-synced clocks.** Clock skew eats into the budget directly.
- **The chaos manifests assume Chaos Mesh and Strimzi labels.** `chaos-during-load.sh` has `delete-pod`, `rollout` and `scale` fallbacks that need only `oc`.
