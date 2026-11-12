# Upstream integration contract: payments request/response over Kafka

**Audience:** the team whose service writes payment requests to Kafka and waits for responses.

The payments processor guarantees that **each request is applied at most once** and that **each request record gets exactly one committed response**. Those guarantees only hold end to end if the upstream service does the four things in section 1.

## 1. Required settings (non-negotiable)

| # | Setting | Why it's required | What breaks without it |
|---|---|---|---|
| 1 | **Producer `enable.idempotence=true`** | A send that times out but actually succeeded (for example, a broker leader change mid-request) is retried by the client. Idempotence de-duplicates that at the broker. | The request lands on the topic twice. You get `ACCEPTED` and then `REJECTED_DUPLICATE` for one attempt. |
| 2 | **A retry reuses the same `requestId`** | `requestId` is the idempotency key. | A retry with a new ID is a **new payment**: the customer is charged twice. |
| 3 | **Response consumer `isolation.level=read_committed`** | Responses are written in Kafka transactions. A processor crash or eviction leaves an *aborted* response on the topic. | You read aborted responses, typically a second `ACCEPTED`. |
| 4 | **Settle each `requestId` on its first response; ignore later ones** | Kafka delivers to consumers at least once. After *your* consumer rebalances or crashes, uncommitted responses are delivered again. Retries also produce a second, legitimate response. | Double-processing of responses: notifications, ledger postings and so on. |

About #1: idempotence is the default in Kafka clients 3.0+, but it is **silently disabled** by conflicting settings: `acks` other than `all`, `retries=0`, or `max.in.flight.requests.per.connection` > 5. Verify the *effective* config, which the client logs at startup as `enable.idempotence = true`. Don't rely on the client version alone.

## 2. Request

| Item | Value |
|---|---|
| Topic | `payments.request` |
| Key | `paymentId`. All updates to one payment go to one partition, in order. |
| Value | JSON: `{"requestId", "paymentId", "amount", "currency", "memo"}` |
| `requestId` | Unique per logical request. **The same value on every retry of that request.** |
| Header `x-deadline-epoch-ms` (recommended) | Absolute deadline (epoch ms). Requests already past their deadline when picked up are **not applied** and get no response. Without it, a request queued during an outage is applied whenever it's processed, possibly minutes later. |
| Reply headers | Not needed. Responses go to `payments.reply`. (Spring `ReplyingKafkaTemplate` callers may send `kafka_replyTopic`/`kafka_replyPartition`; those are honoured.) |

## 3. Response

| Item | Value |
|---|---|
| Topic | `payments.reply` (or your `kafka_replyTopic` header) |
| Key | `requestId`. Match on this, not only on a per-attempt correlation ID, so a late response to an earlier attempt is still recognised. |
| `status` | `ACCEPTED`, or `REJECTED_DUPLICATE` |
| `originalStatus` | On `REJECTED_DUPLICATE`: how the **original** request ended |
| `version` | Payment version produced by this `requestId` (the original's, for a duplicate) |

**How to interpret a response:**

| Response | Meaning | Action |
|---|---|---|
| `ACCEPTED` | Payment applied | Settle as **succeeded** |
| `REJECTED_DUPLICATE` with `originalStatus: ACCEPTED` | This `requestId` was already applied by an earlier attempt | Settle as **succeeded**. **Never** treat it as a failure, and never retry with a new `requestId`. |
| No response within your timeout | Unknown: it may still be applied | Retry with the **same** `requestId` (section 4) |

Reference implementation: [`UpstreamResponseHandler`](../src/test/java/com/example/rr/UpstreamResponseHandler.java). In production, use a conditional update on your own request table:
```sql
UPDATE upstream_requests SET status = :settlement, settled_at = SYSTIMESTAMP
 WHERE request_id = :requestId AND status = 'PENDING'
-- 0 rows updated => already settled => ignore this response
```

## 4. Timeouts and retries

- **Timeout:** 300 ms is reasonable. Steady state is about 10 ms p50 and about 20 ms p99. During a processor rebalance, expect stalls of about 1 s.
- **Retry with exponential backoff and jitter,** for example 300 ms, then 600 ms, then 1.2 s ±20%, and **cap the number of attempts**. Fixed immediate retries turn every rebalance into a retry storm: everything in the stall window is resent at once, doubling load at the worst moment.
- **After the last attempt,** mark the request `UNKNOWN` (not failed) and resolve it later: either by a late response, which section 1 #4 handles, or by reconciliation. A timed-out request may still have been applied.

## 5. During a payments DB switchover

The processor pauses for up to about 5 minutes. Requests you send keep queueing in Kafka:
- **Without a deadline header,** they are applied after the resume. Your retries for the same `requestId` then get `REJECTED_DUPLICATE` with `originalStatus: ACCEPTED`. Correct, and settled as success.
- **With a deadline header,** requests that expired while queued are dropped, so your next retry after the resume is the one that gets applied.

Either way each `requestId` is applied once. Writes restored during a failover replay are **not** answered again, so don't wait for them. Your retries get the answer.

## 6. Proof

The processor's CI runs these against real Kafka and Oracle:

| Scenario | Test |
|---|---|
| Retry after timeout → `REJECTED_DUPLICATE` with `originalStatus` | `UpstreamRetryIT` |
| Processor crash after sending, before commit → 1 visible response | `CrashBeforeCommitIT` |
| Processor evicted mid-request (rebalance) → 1 visible response | `ZombieConsumerIT` |
| Processor scale-out/in under load → 0 duplicate responses | `RebalanceUnderLoadIT` |
| *Your* consumer rebalances → same response redelivered, absorbed by `requestId` dedupe | `UpstreamResponseRereadIT` |
| DB switchover / failover → 0 duplicate responses, 0 double writes | `DbSwitchoverIT` |
