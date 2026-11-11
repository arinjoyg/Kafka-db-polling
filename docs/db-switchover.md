# DB switchover runbook: Oracle main → shadow (GoldenGate)

The target is **zero lost writes and zero double-applied writes**, whether the switch is planned or a crash. During the roughly 5-minute window the 1000 ms SLA is suspended, and callers are told so (HTTP 503 with `Retry-After`).

## Why this works

| Mechanism | What it guarantees |
|---|---|
| Ledger (`processed_requests`), payment write and `consumer_offsets` committed in **one transaction** | GoldenGate preserves transaction boundaries, so the shadow has all three rows for a request or none of them. |
| `consumer_offsets` stored in the DB | After a switch, consumers resume from **what the new DB actually has**, not from Kafka's committed offset, which may be ahead of it. |
| Ledger primary key on `request_id` | Replaying a request the shadow already has is a no-op that replies DUPLICATE with the original version. |
| Replay window | Records that existed in Kafka before the resync are re-applied even if past their deadline, because the old DB may already have answered them OK. |
| Error handler never skips on DB-unavailable errors | During the outage, records wait with backoff. Before this change they were skipped after about 100 ms, and processed in neither DB. |

## GoldenGate prerequisites (verify before the first switch)

- [ ] `PAYMENTS`, `PROCESSED_REQUESTS` and `CONSUMER_OFFSETS` are in the **same Extract → trail → Replicat path**, with no filters on any of them.
- [ ] Replicat does not split source transactions. Grouping several together (`GROUPTRANSOPS`/`BATCHSQL`) is fine, because that only merges whole transactions. Parallel or Integrated Replicat is fine too: every transaction updates its partition's `CONSUMER_OFFSETS` row, which forces per-partition ordering.
- [ ] The reverse path (shadow → main) is configured but **stopped**, with loop prevention (tagging or `EXCLUDEUSER`), so you can fall back.
- [ ] Kafka retention on the request topic is longer than the worst replication lag plus detection time. The default is 7 days.
- [ ] The app connects through a **switchable endpoint** (DNS CNAME, TNS alias or service) and never by host name.
- [ ] No sequences or identity columns in these tables (natural keys only). Keep it that way.

## Planned switchover

Script: `scripts/db-switchover.sh`. Run each step in order; each one must pass before the next.

| # | Step | Command / check | Done when |
|---|---|---|---|
| 1 | Close intake on every requestor pod | `POST /admin/intake/close` | `POST /payments` returns 503 |
| 2 | Pause every processor pod | `POST /admin/processing/pause` | `GET /admin/processing/status` shows `paused: true` on all pods |
| 3 | Fence main | `ALTER USER <app> ACCOUNT LOCK;` then kill the remaining `<app>` sessions | `v$session` has no `<app>` sessions |
| 4 | Drain GoldenGate | `SEND EXTRACT <ext>, LOGEND`, then `SEND REPLICAT <rep>, STATUS` / `LAG REPLICAT <rep>` | LOGEND = YES, Replicat at EOF, lag 0 |
| 5 | **Application-level check** | Run `sql/switchover-check.sql` on both DBs | `CONSUMER_OFFSETS` identical on main and shadow, partition by partition |
| 6 | Stop the forward Replicat; start the reverse path if you want fallback | GGSCI / Admin Service | |
| 7 | Repoint the endpoint to the shadow, then evict pools on every pod | DNS/TNS change; `POST /admin/db/evict-pool` | New connections reach the shadow (`SELECT sys_context('USERENV','DB_NAME') FROM dual`) |
| 8 | Resume processors, then reopen intake | `POST /admin/processing/resume`; `POST /admin/intake/open` | Probe round trip OK, then `sql/reconcile.sql` is clean |

Resync isn't needed in a planned switch, because step 5 proved the offsets are equal. Running it anyway is harmless.

## Unplanned failover (main is gone)

1. Close intake and pause processors. Records already wait instead of being skipped.
2. Make sure main can't come back as a writer: fence it if it's reachable, otherwise keep the old endpoint unreachable.
3. Repoint the endpoint to the shadow and evict pools.
4. **`POST /admin/processing/resync` on every processor pod.** Each partition seeks to the shadow's stored offset and opens a replay window up to the current log end. If it returns 500 with ORA-28000 (account locked), a connection that was mid-connect during the repoint reached the fenced main. Evict again and retry; `scripts/db-switchover.sh` does this for you.
5. Resume processors and reopen intake.
6. Run `sql/reconcile.sql` and check `GET /admin/processing/status` until the replay windows are empty.

What callers see: some requests were answered OK by main and then lost in the replication lag. They are re-applied from Kafka within seconds of the resume, with the same request ID and the same resulting version. Until then they're briefly missing from the shadow. If that window is unacceptable, the only fix is synchronous replication, which GoldenGate doesn't provide: Data Guard in Maximum Availability mode does, at the cost of commit latency.

## Known limits

- **Admin endpoints act per pod and are unauthenticated.** Secure them with a network policy or auth before using them outside test.
- **A partition with no row in `CONSUMER_OFFSETS` on the new DB is left at Kafka's position,** with a warning. In practice every active partition has a row after its first message.
- **Records sent to the DLT don't advance the DB offset,** so a resync can resend them to the DLT. That's harmless, but expect duplicate DLT entries.
- **`DbSwitchoverIT` simulates GoldenGate,** using flashback snapshots applied with a lag. It proves the application's guarantees. GoldenGate's own behaviour (LOGEND, EOF, parallel apply) must be proven in the perf environment.
