# Test plan: Kafka request-reply → DB upsert, 1000 ms SLA

## What is measured, and where

| Metric | Source | Why |
|---|---|---|
| End-to-end latency p50/p95/p99/p99.9/max | k6 `http_req_duration` (client side), `rr.requestor.roundtrip` | The SLA lives here. Consumer-side timings hide queueing. |
| Timeout (PENDING/202) rate | k6 `rr_pending`, `rr.requestor.roundtrip{outcome=TIMEOUT}` | A timeout is a broken promise even if the write lands later. |
| Queue delay (produce → pick-up) | `rr.processor.queue.delay` | Lag and rebalance pauses show up here first. |
| Processing time (DB tx) | `rr.processor.processing` | Separates DB time from Kafka time. |
| Expired drops | `rr.processor.expired` | Must explain every `unresolved_timeouts` row. |
| Consumer lag per partition | Kafka exporter / `kafka.consumer.fetch.manager.records.lag` | Hot partitions and backlog. |
| Hikari pool wait | `hikaricp.connections.acquire`, `hikaricp.connections.pending` | Pool exhaustion looks like "slow DB" without this. |
| GC pause | `jvm.gc.pause` | Long pauses cause both latency spikes and false consumer evictions. |
| Late writes / lost / OK-without-write | `sql/reconcile.sql` | Correctness under stress. |

Calculate latency only on one host's clock (the client's). Don't subtract timestamps taken on different hosts.

## Scenario matrix

| # | Scenario | How it's run | Pass criteria (starting points, so tune them to the contract) |
|---|---|---|---|
| 1 | Baseline at peak TPS | `k6 SCENARIO=baseline RATE=<peak>` | p99 < 800 ms, p99.9 < 1000 ms, pending < 0.1%, 0 failed, 0 dropped_iterations |
| 2 | Ramp to the knee | `k6 SCENARIO=ramp RATE=<peak/4> MAX_RATE=<5×peak>` | Knee (first plateau with p99 ≥ 1000 ms) ≥ peak ÷ 0.7. Record the knee TPS. |
| 3 | Spike 3–5× | `k6 SCENARIO=spike SPIKE_MULT=5` | Lag drains and p99 returns under 800 ms within N s after the spike (agree on N). Count expired drops. |
| 4 | Soak 8–24 h | `k6 SCENARIO=soak DURATION=8h` | Flat p99 trend. No heap, pool, or pending-reply growth. `processed_requests` purge keeps up. |
| 5 | Rebalance under load | `RebalanceUnderLoadIT`; `chaos-during-load.sh ACTION=pod-kill/rollout/scale` | 0 errors. Timeouts per event within budget. Every OK has a committed write. |
| 6 | Broker failure | `ACTION=broker-kill` (RF=3, min.insync=2, acks=all) | No lost acknowledged writes. Measure the latency spike during leader election. Timeouts within budget. |
| 7 | DB degradation | `DbLatencyLateWriteIT`; `ACTION=db-delay` | Timeouts rise and recover once latency is removed. Every late write shows up in reconciliation. |
| 8 | Requestor instance loss / multi-instance | `ReplyRoutingIT`; kill a requestor pod | No reply cross-talk. In-flight requests on the dead pod end as late writes or expired drops, never lost. |
| 9 | Hot key / partition skew | `k6 HOT_KEY_FRACTION=0.4 HOT_KEYS=3` | The p99 hit is quantified. If it breaches the SLA, revisit the partition key. |
| 10 | Concurrent updates to the same row | Covered by 9 (same paymentId), plus `lock_timeout` | No deadlocks. Lock waits stay bounded by `lock_timeout`. Versions strictly sequential (`version_drift` = 0). |
| 11 | Payload size variance | `k6 MAX_PADDING=200000` | p99 within SLA up to the agreed max size. Nothing rejected under `max.message.bytes`. |
| 12 | Duplicate injection | `IdempotencyIT`; `k6 DUP_FRACTION=0.05` | `version_drift` = 0. Duplicates answered as DUPLICATE with the original version. |
| 13 | Timeout reconciliation | `sql/reconcile.sql` after every run | `ok_without_write` = 0. `unresolved_timeouts` = `rr.processor.expired`. Late writes explained. |
| 14 | Backlog / cold start | `ExpiredRequestIT`; stop processors 5 min under load, then restart | Expired requests dropped, not written. Fresh requests meet the SLA once the backlog of expired ones is skipped. |

## Rules of the run

1. **Use open-model load only.** Closed-loop thread pools (default JMeter thread groups, plain for-loops) suffer from coordinated omission and underreport p99. k6 arrival-rate executors are used here. A run with `dropped_iterations > 0` is invalid.
2. **Use a production-like environment.** That means the same partition counts, RF, `min.insync.replicas`, broker count, DB engine, DB size, and indexes. A DB with 1,000 rows will flatter you.
3. **Warm up before measuring.** The first round-trips after start include metadata fetches and partition assignment, and routinely exceed 1 s. Readiness probes should require a successful round-trip.
4. **Inject one fault at a time,** under steady baseline load, at a known timestamp. That's the only way to attribute timeouts to a cause.
5. **Reconcile after every run.** A green latency report with a non-zero `ok_without_write` is a failed run.

## Exit criteria for sign-off

- Scenarios 1, 3, 4, 12 and 13 pass.
- Scenarios 5–8 meet the agreed per-event timeout budget.
- The knee from scenario 2 is documented, along with headroom vs. forecast peak.
- Hot-key exposure from scenario 9 is quantified, with a decision recorded.
