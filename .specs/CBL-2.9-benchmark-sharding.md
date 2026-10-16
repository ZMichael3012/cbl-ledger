# CBL-2.9. Write benchmark and sharding strategy by account_id

| Field | Value |
| :-- | :-- |
| Epic | E2, Ledger |
| Repository | `cbl-ledger` |
| Size | M |
| Wave and parallel group | W1-LEDGER |
| Depends on | 2.3 |
| Contract version | `cbl-protos@cf-1` (`billing/ledger/v1`) |
| Status | done (review closed, commit CBL-2.9) |

## 1. Context and goal

The task starts early (CBL-004, principle 5), since its result may change the ledger
architecture: the target profile from `CBL-002` is 500 TPS of writes and reads, p99 of
`PostTransaction` under 500 ms without balance errors. Hot accounts (T-06) and the risk of
TPS degradation are the main risk of epic E2.
Goal: measure the actual throughput of the 2.2/2.3 mechanism on real PostgreSQL,
verify that the target is achievable and fix the sharding strategy by `account_id`
(implementation or a justified deferral with revision criteria).

## 2. Boundaries

In scope: Go benchmarks of the hot and distributed profile on testcontainers PostgreSQL;
the k6 gRPC `PostTransaction` scenario for the compose stand (`test/load/`) with an account
seed; a throughput and bottleneck report; an ADR on sharding and an update of the
AGENTS.md trade-offs.

Out of scope: implementation of sharding/batching (only based on the results and, if needed,
in a separate task 14.2/14.3); the global 3000 RPS profile (14.2); chaos tests (14.3);
reserves (2.4), the consumer (2.7).

## 3. Functional requirements

FR-1. Go benchmarks (build tag integration, real PostgreSQL):
- `BenchmarkPostTransactionHotAccount` (sequential, 2.3) - the baseline;
- `BenchmarkPostTransactionHotAccountParallel` - the parallel profile via `RunParallel`,
  unique idempotency keys, the metrics `retries_per_op` (retry events per operation)
  and `conflicts_%` (share of operations that exhausted the retry budget) from the 2.3 observer;
- `BenchmarkPostTransactionDistributedAccounts` - the parallel profile over 64 account pairs
  without a shared hot account: the ceiling of the storage layer and PostgreSQL without
  contention (the gRPC transport is measured by k6);
- `BenchmarkPostTransactionManyEntries` and `...NoTrigger` - an A/B of the cost of the
  deferred trigger 00006 on a transaction with 100 entries (the 2.2 limit).

FR-2. The k6 scenario `test/load/post_transaction.js`:
- the gRPC call `billing.ledger.v1.LedgerService/PostTransaction` using the proto from `cbl-protos`;
- modes via env: `RATE` (arrival rate, 100 by default), `DURATION` (30s), `PAIRS`
  (the number of merchant debit/credit account pairs from `test/load/seed.sql`, 64 by default,
  256 maximum);
- transport exceptions are recorded by the separate metric `transport_failures` (the check is
  not executed when the invoke is thrown), thresholds: `p(99) < 500ms`, `checks rate > 0.999`,
  `transport_failures rate < 0.001`, `dropped_iterations count < 500` (the CBL-002 criterion);
- running without installing k6: the `grafana/k6:2.3.0` image (pinned), the proto is mounted
  from `services/cbl-protos`; the commands are in the header of the scenario (cmd.exe).

FR-3. Throughput report: a measurement table (profile, concurrency, TPS,
p99/average, retry share) and a bottleneck analysis: the number of round trips per posting,
serialization of the hot balance row, the cost of the deferred trigger 00006, the limits of a
single PostgreSQL instance.

FR-4. Sharding strategy: an ADR with options (keep as is; batching of deltas over a short
window; sharded clearing sub-accounts with aggregation; time partitioning of `entries`),
selection criteria and revision thresholds at 14.2/14.3. The decision is fixed
in AGENTS.md (T-LED-01, T-LED-02) and the specification.

FR-5. Acceptance criterion: the write throughput confirmed on the local stand is
at least 500 TPS (distributed profile) with p99 under 500 ms; for a hot single
account the measured limit and the protection plan (batching/sharding on breach) are
recorded. Acceptance of 2.9 covers writes only: `GetBalance`/`ListEntries` reads appear in 2.5,
the mixed profile and read thresholds are input of task 14.1.

FR-6. Based on the load run results, the identified configuration bottlenecks were fixed:
the service PostgreSQL connection pool is limited (`DB_MAX_OPEN_CONNS=20`, `DB_MAX_IDLE_CONNS=10`,
lifetime 30m, configuration ceiling 100); without the ceiling a burst of 500 rps exhausted
the PostgreSQL `max_connections` (`Internal` errors in 2.57% of calls). After the fix - 0 errors
among executed calls at 500 and 1000 rps (at 1000 rps about 1% of iterations were dropped by the
k6 queue upon tail saturation, the share of successful ones among executed 100%).

## 3.1. Measurement results

Stand: Windows 10, Intel Core i7-9750H (6 cores / 12 threads), Docker Desktop (WSL2),
PostgreSQL 16 in a container; Go benchmarks via testcontainers, k6 via the compose stand
(ledger + PostgreSQL + Kafka in one compose), k6 `grafana/k6` v2.3.0 in a container.

| Profile | Concurrency | Achieved | Latency | Errors/retries |
| :-- | :-- | :-- | :-- | :-- |
| Go bench: hot pair, sequential | 1 | 283 writes/s | 3.53 ms/op | none |
| Go bench: hot pair, parallel | 24 | 168 writes/s total (successful ~145/s) | 5.95 ms/op (aggregate time) | 3.07 retry events/op; 13.9% of operations exhausted retries |
| Go bench: 64 pairs, parallel | 24 | 2070 writes/s | 0.48 ms/op (aggregate time) | none |
| Go bench: single row UPDATE (lock ceiling) | 24 | 7374 updates/s | 0.14 ms/op (aggregate time) | none |
| Go bench: 100 entries, trigger 00006 enabled | 1 | ~9 transactions/s | 111.5 ms/op | none |
| Go bench: 100 entries, trigger 00006 disabled | 1 | ~10 transactions/s | 100.6 ms/op | none |
| k6 end-to-end, target 500 rps | 5 - 7 VU | 10000 iterations in 20 s (exactly at the rate, no drops) | p95 15.4 ms, p99 24.2 ms | 0 |
| k6 end-to-end, target 1000 rps | up to 178 VU | 19809 iterations in 20 s, 192 dropped (0.97%) | p95 193 ms, p99 369 ms | 0 among executed |

Conclusions:
1. The target of 500 TPS of writes with p99 under 500 ms is reached. The throughput margin of
   the storage layer is ~4x (2070 writes/s against 500); the e2e margin by p99 is more modest:
   24 ms at 500 rps, 369 ms at 1000 rps (1.35x to the 500 ms threshold) - the main degradation
   at peak comes from queuing (the connection pool and retries), not from the transactional
   model as a whole.
2. The number of round trips per posting (about 9 statements: key lookup, transaction and
   entry inserts, account and balance selects, updates; estimated from the code, isolation
   in 14.2 if needed) determines the baseline latency of 3.5 ms; the serialization ceiling
   of a single row is much higher (7374 updates/s), that is, the balance row is not the limit.
3. A single hot account degrades under contention: the sequential limit is 283/s,
   with 24 writers the total throughput drops to 168/s because of the CAS conflict
   storm (3.07 retry events per operation and 13.9% failures). The limit is created
   not by row locking but by the optimistic retry model under high contention.
4. The cost of the deferred trigger 00006 was measured in an A/B: on a transaction with 100
   entries +10.9 ms (111.5 vs 100.6 ms, about 11% of the operation); on typical postings with
   2 - 4 entries the contribution is within noise (two index aggregates). The 2.3 open question
   about the many-entry cost is closed; if isolation is needed in 14.2 - pg_stat_statements.
5. The load run identified and allowed closing a configuration defect: the unlimited connection
   pool exhausted PostgreSQL under load (FR-6).

## 3.2. Sharding strategy (ADR-2.9)

Decision: the current model (optimistic CAS + the 2.3 retry policy) is kept; no
sharding or batching implementation is introduced in W1. Rationale: the 500 TPS target is
reached as long as operations are distributed across accounts; a single hot account
structurally arises at the clearing account (every completed payment posts
Dr clearing / Cr merchant), and the payment flow reaches its design share only by wave
W3 - that is when protection is needed.

Revision threshold: 200 writes/s to a single account - about 70% of the sequential limit
of 283/s, with margin for bursts; the primary triggers are also p99 of operations with this
account above 500 ms and an error share above 0.1%. On trigger, the following is applied
in order:

1. Reduction of the number of posting statements (single selects, batch inserts): expected
   effect 2 - 3x to the sequential limit; diagnostics - 14.2, implementation - 14.6.
2. Batch committer for the hot account: accumulation of deltas over a window of 5 - 10 ms
   and one transaction per batch; applicable to internal flows (clearing), does not change
   the merchant API; 14.6.
3. Account sharding by `account_id`: N sub-accounts (clearing#0..N-1) selected by a hash of
   the business key, periodic consolidation and an aggregated balance for reporting;
   double entry is preserved at the sub-account level. Estimate: 8 sub-accounts at the
   concurrent limit of 168/s give about 1340/s, at the sequential 283/s - about 2260/s
   (above the platform target clearing flow); 14.6.
4. Time partitioning of `entries`: after confirmed growth (the growth estimate
   and threshold - 14.2), interlinks with open question 2.1.

Rejected alternative: serialization of the hot account with a pessimistic row lock until
commit. The transaction holds the lock for about its duration (3.5 ms at the current layout),
which gives no more than the sequential limit (~283/s) and queues at peaks; the gain is
possible only together with batching (option 2), where it is equivalent to it. CAS is kept
for non-hot accounts, where there are almost no conflicts (the distributed measurement).

Implementation owner: profile diagnostics - 14.2, implementation of measures 1 - 3 on
threshold breach - the new task 14.6 (E14, depends on 14.2). Threshold monitoring: the
retry metric (2.3 observer) and the posting rate per account, alerts - 14.4; under load
the retry warn logs are sampled by the zap production configuration (100 entries/s
per level).

## 4. Contracts

- gRPC: `LedgerService/PostTransaction` (`cbl-protos@cf-1`), unchanged.
- Kafka: unchanged.
- Storage: schema unchanged; the seed `test/load/seed.sql` creates up to 256 account pairs
  with deterministic UUIDs and initial balances.

## 5. Data model and migrations

No new migrations. The load seed data is not part of the production migrations: a separate file
`test/load/seed.sql`, executed only on the load stand.

## 6. State machines and rules

Not applicable. Load profile: the k6 arrival-rate scenario; the idempotency key is unique
per iteration (`<RUN_ID>-<vu>-<iter>`, the run prefix is length-limited and fits
the 128-character key limit from 2.2), repeated calls do not create double effects.

## 7. Edge cases

Hot account (all transactions to one pair) - the measured limit and retries; the distributed
profile - low contention; load growth to the peak 1.5x - 2x (14.2); reuse of
one k6 idempotency key within vu/iter; PG availability in testcontainers/compose.

## 8. Non-functional requirements

- Money scenarios only on real PostgreSQL (mocks are forbidden, CBL-001).
- Reproducibility: one command for benchmarks (`make bench`), fixed seed and k6
  commands in the header of `test/load/post_transaction.js`; k6 version 2.3.0 was used
  (the `grafana/k6` image, the PIN is refined in 0.2).
- The report contains the stand characteristics (CPU, OS, PostgreSQL version, Docker network).
- Em dash and float are not used; metrics do not contain user money amounts.

## 9. Test plan

| Level | Scenarios |
| :-- | :-- |
| Smoke | build, migrations, healthz (compose) |
| Unit | not applicable to load; the repository unit suite does not change |
| Integration | benchmarks on testcontainers PostgreSQL (hot, hot-parallel, distributed) |
| Local e2e | compose stand + k6: distributed profile (RATE=100..500), CBL-002 thresholds |
| Load | k6 official local task profile (performed: 500 and 1000 rps, see 3.1); the global profile - 14.2 |

Unit+integration coverage does not decrease (the new code is test and load only).

## 10. Work plan and commit

1. Spec, Go benchmarks, the k6 scenario and the seed.
2. Runs: benchmarks (hot, hot-parallel, distributed), k6 on compose.
3. Report and ADR in the spec; AGENTS.md update (trade-offs, KI-ledger-03, journal).
4. Four review passes, linters, commit `CBL-2.9: write benchmark and sharding strategy`.

## 11. Open questions and decisions

- Time partitioning of `entries` (open question 2.1): the decision follows the results of the
  growth estimate in 14.2; criteria in 3.2 (item 4).
- Batching and sharding of the hot account: the strategy is fixed in 3.2; implementation - task
  14.6 (E14 of the plan) on exceeding the threshold of 200 writes/s to a single account based on the results of 14.2.
- The cost of the deferred trigger 00006 on many-entry transactions: closed by the A/B measurement
  (conclusion 4 of section 3.1).
- The synthetic k6 profile does not reproduce the read share in the mix; the global mixed
  profile - 14.1/14.2.
- The uniqueness test of the idempotency key under load is covered by the integration suite
  (a repeat with the same body), not by k6: every run uses its own `RUN_ID` prefix.

## 12. Review (CBL-001, step 6)

Four independent passes (14.10.2026): no-context, architecture, with-context,
security (blocking). No blockers; the security verdict is PASS (with KI-ledger-06);
all major and minor findings are closed or recorded.

| Role | Key findings | Closure |
| :-- | :-- | :-- |
| No-context | the trigger conclusion was not confirmed by a measurement; the k6 thresholds do not catch transport failures and drops; a discrepancy in the k6 methodology (542/1021 calls/s against the rates); the key in section 6 did not match the script; the `retries_%` metric is ambiguous; distributed was called the system ceiling | A/B benchmarks on 100 entries added (conclusion 4); `transport_failures` and dropped thresholds added to k6; table 3.1 converted to iterations per run; section 6 synchronized; the metric renamed to `retries_per_op`; the docstring clarified |
| Architecture | no owner for the implementation of protection measures (14.2 - diagnostics); the trigger cost on many entries was not measured; the 200/s threshold without a derivation; no rejected serialization option; the "4x margin" mixes TPS and p99 | task 14.6 added to CBL-004; the A/B trigger measurement; the threshold derived from 283/s (~70%); the rejected alternative added with rationale; conclusion 1 separates the TPS and p99 margins |
| With-context | status/journal ran ahead of the review; `make bench` did not cover all benchmarks; ambiguous `retries_%` units; a typo and Cyrillic in the Latin text; no `make bench`/k6 in AGENTS; PAIRS without a lower bound; the `.js` was not checked for em dashes | status and journal brought in order; the `-bench "."` filter; the metric renamed; typos fixed; commands added to AGENTS.md; `Math.max(1, ...)` added; the `.js` included in `check-no-emdash.ps1` |
| Security | the pool ceiling without a server deadline/queue limit (infinite wait on saturation); `DB_MAX_OPEN_CONNS` not bounded from above; retry warn logs under load; k6 without an image pin; the build without `.dockerignore` | KI-ledger-06 recorded (timeout and queue before prod; prod is forbidden by KI-ledger-02); the clamp of 100 and logging of pool values; zap production sampling confirmed; the image pinned to `grafana/k6:2.3.0`; `.dockerignore` of the services context - in 0.2 |