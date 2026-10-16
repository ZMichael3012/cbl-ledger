# AGENTS.md - cbl-ledger

Double entry, reserves and balances: the source of truth about money.

Mandatory global rules: `../../AGENTS.md`, `../../.specs/CBL-001-development-process.md`,
`../../.specs/CBL-002-repos-and-docker.md`, contracts `../../.specs/CBL-003-contracts.md`.
The em dash (U+2014) is forbidden, only the hyphen is used.

## 1. Boundaries

In scope: PostTransaction, reserves, balances, statements, idempotent postings.

Out of scope: other platform services; interaction only through `cbl-protos` contracts
(gRPC), Kafka and the perimeter REST (see `CBL-000`, section 2).

## 2. Contracts

- REST: administrative /healthz, /readyz
- gRPC: LedgerService server (PostTransaction, Reserve, CommitReserve, ReleaseReserve,
  GetBalance, ListEntries with merchant and period filters, ListMerchantAccounts - 2.10)
- Kafka produces: `ledger.events` (transaction.posted, reserve.created/committed/released;
  envelopes via outbox, key - the first posting in sort order; CBL-003 section 6)
- Kafka consumes: `payment.events` (payment.completed -> Dr clearing / Cr merchant / Cr fee;
  group `cbl-ledger-payments`, retry topics and DLQ via the kit mechanics)
- Storage: PostgreSQL ledger (accounts, transactions, entries, balances, reserves,
  reserve_operations, transaction_balances, outbox, inbox)
- Chart of accounts (2.6): `account_id` is derived deterministically from the business key
  (`CBL-003`, section 9, golden vectors); provisioning - `EnsureAccount` (usecase/storage)

Contracts are frozen in `cbl-protos` (CF-1 gRPC, CF-2/CF-3 events; additive
BFF statement fields - `cbl-protos@cf-3.2`, CBL-2.10), see `CBL-004`.

## 3. Architecture

Layers: `internal/transport` -> `internal/usecase` -> `internal/domain` -> `internal/storage`.
Libraries: the standard `CBL-002` set (Gin, GORM + pgx driver, franz-go + pkg/sr, go-redis,
goose, zap). `AutoMigrate` is forbidden, migrations are goose only, forward only.

## 4. Commands

```powershell
make build        # go build ./...
make test         # go test ./...
make test-integration  # integration (testcontainers), including property-based invariant tests (2.8)
make test-e2e     # runs the Go e2e tests against an already started compose stand; the run instruction is `test/e2e/E2E.md`
make bench        # write benchmarks on testcontainers (CBL-2.9)
make vet          # go vet ./...
make lint         # golangci-lint in Docker
make up           # docker compose -f deploy/docker-compose.yml up -d
make down         # docker compose -f deploy/docker-compose.yml down -v
```

The k6 load run (compose stand): the seed and commands are in the header of
`test/load/post_transaction.js`, the seed data is `test/load/seed.sql` (load stand only).

## 5. Ports and environment

HTTP `8040`, gRPC `9040`, metrics `9240`.
Variables (applicable to the service): `HTTP_ADDR`, `GRPC_ADDR`, `METRICS_ADDR`, `PG_DSN`,
`KAFKA_BROKERS`, `KAFKA_CONSUMER_GROUP` (default `cbl-ledger-payments`),
`SCHEMA_REGISTRY_URL`, `REDIS_ADDR`, `DB_MAX_OPEN_CONNS` (default 20),
`DB_MAX_IDLE_CONNS` (default 10).

## 6. Development rules

- Pipeline: SDD spec in `.specs/` -> architecture -> contracts -> tests first -> code ->
  review by 4 agents (no-context, architecture, with-context, security - blocking) ->
  AGENTS.md -> final runs -> linters -> commit.
- Unit+integration coverage is at least 80%; money scenarios only on real PostgreSQL and
  Kafka (testcontainers); local e2e in docker-compose is mandatory.
- Money is int64 in minor units plus decimals only; float is forbidden.
- Godoc docstrings are mandatory in English: the first line starts with the entity name
  and ends with a period; comments explain why; errors are wrapped with `%w`; panics in
  business code are forbidden.
- Commit: `CBL-<epic>.<number>: <short message>`, the spec with the same prefix in the same commit.

## 7. SLO and metrics

SLO is refined in task 14.4. Base metrics: latency p95/p99, error rate, outbox lag,
consumer lag; dashboards after task 0.7.

## 8. Trade-offs

| ID | Decision | Pros | Cons and compensation |
| :-- | :-- | :-- | :-- |
| T-LED-01 | Validation, idempotency and optimistic retries live in `storage.Repository` | transactional context and atomicity in one layer | the usecase is thin, layer boundaries are blurred | since 2.3 the retry policy is extracted into `RetryPolicy` and `RetryObserver`; benchmark 2.9: distributed 2070 writes/s, the 500 TPS target is reached, hot account decisions - section 3.2 of spec 2.9 |
| T-LED-02 | The sum of debits and credits is checked by the application and a DB barrier (deferred constraint trigger 00006) | application flexibility plus a second barrier at commit | barrier limitations: direct SQL bypassing the application can append a balanced pair to someone else's transaction and mix assets (outside the threat model; balances do not change) | A/B measurement 2.9: +10.9 ms per 100 entries (about 11%); within noise at the typical 2 - 4 entries |
| T-LED-03 | A negative `available` is allowed for clearing, fee and suspense | supports the Dr clearing / Cr merchant flow without prefunding | a policy mistake is possible for transit accounts | control by account types, suspense monitoring in 2.6 |
| T-LED-04 | The PostgreSQL connection pool is limited (`DB_MAX_OPEN_CONNS=20`, configuration ceiling 100) | protects `max_connections` from spikes, predictable degradation | when saturated, requests wait for a connection | found by load test 2.9 (Internal errors at 500 rps); the values are tuned via env, a 4x margin per the 2070 writes/s measurement |
| T-LED-05 | A hot account stays on optimistic CAS up to the threshold of 200 writes/s | simple model, no row locks on common paths | with 24 writers to one account throughput drops (168 writes/s, 14% failures) | protection plan on threshold breach: operator reduction, batch committer, account sharding (2.9, section 3.2) |
| T-LED-06 | A reserve is a hold: Reserve/Release change only available/reserved without postings, Commit writes a balanced pair Dr source / Cr payout account | the hold creates no fictitious movement in the journal, compensation is cheap (release) | balances are not derived from entries alone, reserves accounting is needed (CBL-000, section 6); the commit recipient is not in the CF-1 contract | the recipient is the payout account of the same merchant_ref and asset (ADR-2.4, spec 2.4); reserve.* events - in 2.7; fail-closed for merchant accounts without merchant_ref |
| T-LED-07 | Chart of accounts rules (2.6) are enforced by validation and a single provisioning path, without DB CHECK constraints | the migration does not break already applied dev stands with legacy accounts | direct SQL can create an off-plan account (lowercase asset, ref on a platform type) | money invariants are not affected (reserves fail-closed by type/ref/asset, postings are balanced); balance healing is limited to accounts without postings |
| T-LED-08 | The consumer handler (2.7) does not use the Guard transaction: the posting is idempotent by the business key `payment:{id}:completed`, inbox is the second barrier | no revision of the 2.3 retry loop; one compact commit per event | a failure between the posting and inbox commits is safe (replay), but requires `payment_id` canonicalization (implemented) | concurrent duplicates are confirmed by an integration test; events are written to the outbox atomically with the posting |
| T-LED-09 | The `ledger.events` partition key is the lexicographically minimal account_id of the posting | stable event partitioning, dedup by event_id in the header | events of one account may land in different partitions (order is not promised); the min key pulls a share of events to platform accounts (clearing/fee) | consumers correlate by `ref_id`/`transaction_id`, not by order; if needed, the key is revisited in CF-2 |
| T-LED-10 | Property tests without new dependencies: `testing/quick` for the domain, a deterministic generator on `math/rand/v2` for storage | zero dependencies, reproducibility via `PROPERTY_SEED`, a check after every operation | manual generator, no shrinking, invariants duplicate the SQL schema | check independence is confirmed by mutations; failure localization - stage and seed; `pgregory.net/rapid` extension - only on a revision in 14.x |
| T-LED-11 | Merchant reads (2.10) are filtered by `merchant_ref` in the query instead of a separate token owner: the BFF passes the token subject, the Ledger does not authenticate the caller (KI-ledger-02) | ownership and merchant isolation without cross-service tables; foreign/platform accounts under the filter answer NotFound without revealing existence | protection against `merchant_ref` spoofing appears only with mTLS/authN (0.6); until then the port is not published outside the trusted network | the unfiltered statement and the one by account_id remain internal calls; isolation integration tests are mandatory; access to platform accounts through the merchant filter is impossible |

## 9. Known issues

| ID | Problem | Impact | Plan | Status |
| :-- | :-- | :-- | :-- | :-- |
| KI-ledger-01 | CI and pre-commit are not configured | linters run manually | task 0.2 | open |
| KI-ledger-02 | No mTLS and authN on the gRPC LedgerService: writing, reserves, reading balances and the full statement (with ref_id) of any account; `account_id` is derived from public business keys (2.6), i.e. it is not a capability; since 2.10 reads by `merchant_ref` are added - with access to the port, spoofing the reference gives statements and balances of any merchant (T-LED-11); an equivalent unauthenticated write path appeared via Kafka: writing `payment.completed` into `payment.events` creates accounts and credits an arbitrary `merchant_id` | do not publish port 9040 and the Kafka topics outside the trusted network; prod deployment is forbidden; bind authorization to the merchant, not to knowledge of the id | mTLS and authorization before prod (0.6, 1.4); broker ACL/mTLS for writing to `payment.events` - task 0.5; namespacing of idempotency keys by merchant - together with authN | open |
| KI-ledger-03 | e2e scenarios in `test/e2e/` are missing | step 8 is partially done: integration, smoke compose and k6 load (2.9, `test/load/`) exist; there are no e2e scenarios | CLOSED 14.10.2026: e2e compose stand run per the test/e2e/E2E.md instruction (Reserve/Commit/Release via real gRPC and PostgreSQL) | closed |
| KI-ledger-04 | The race detector is unavailable locally (no CGO on Windows) | concurrency tests run without -race | enable -race in CI (0.2) | open |
| KI-ledger-05 | The `balances_non_negative()` function (migration 00005) does not pin `search_path` | low: spoofing requires CREATE rights in a schema earlier than public; 00006 is hardened (`public.entries`, search_path) | apply hardening with a separate migration during a planned change of 00005 | open |
| KI-ledger-06 | No server deadline interceptor and no RPC queue limit: when the pool is saturated, a request without a client deadline can wait for a long time | availability risk outside the money path; load test 2.9 degrades by latency growth, not by errors | timeout interceptor and MaxConcurrentStreams before prod (0.6/14.4); prod is forbidden by KI-ledger-02 | open |
| KI-ledger-07 | `transaction_balances` grows about 1:1 to `entries` (a row per affected account), retention and partitioning are not defined | DB growth, more expensive backups; reads by PK are not affected by the growth | partitioning/retention together with `entries` - task 14.2; `created_at` is already written (2.5) | open |
| KI-ledger-08 | The derivation of `account_id` (chart of accounts UUIDv5) is implemented in the ledger `internal/domain`; consumers (Billing API 7.4, Payout 10.2) will reproduce the algorithm from the golden vectors (CBL-003, section 9) | risk of implementation drift between services | move the derivation into the shared `cbl-kit` library before 7.4/10.2 starts; until then the vectors are mandatory in every consumer's tests | open |
| KI-ledger-09 | The ledger outbox grows on every mutation (postings, reserves) and is not cleaned: the relay only sets `published_at` | DB and backup growth; the 2.9 measurement did not include outbox row inserts | retention/archival of published rows together with `entries` partitioning - task 14.2; throughput re-measurement there as well | open |

## 10. Task journal

| CBL | Date | Status | Spec | Commit |
| :-- | :-- | :-- | :-- | :-- |
| CBL-2.1 | 2026-10-14 | done, review closed (migrations 00001 - 00005, append-only trigger, non-negative) | `.specs/CBL-2.1-ledger-schema.md` | fb47705 |
| CBL-2.2 | 2026-10-14 | done, review closed (idempotency by body hash, coverage 90.7%) | `.specs/CBL-2.2-post-transaction.md` | a2d0606 |
| CBL-2.3 | 2026-10-14 | done, review closed (RetryPolicy + full jitter, observer, migration 00006, coverage 91.7%) | `.specs/CBL-2.3-concurrency.md` | 0b623af |
| CBL-2.9 | 2026-10-14 | done, review closed (benchmarks, k6 500/1000 rps, DB pool ceiling, strategy in ADR-2.9) | `.specs/CBL-2.9-benchmark-sharding.md` | b663447 |
| CBL-2.4 | 2026-10-14 | done, review closed (reserves with a finite state machine, migration 00007, e2e compose, coverage 86.6%) | `.specs/CBL-2.4-reserves.md` | f3d4b9e |
| CBL-2.5 | 2026-10-14 | done, review closed (GetBalance/ListEntries, migration 00008, balances_after snapshots, coverage 87.3%) | `.specs/CBL-2.5-balance-queries.md` | 19ff6f5 |
| CBL-2.6 | 2026-10-14 | done, review closed (deterministic accounts, EnsureAccount, golden vectors, coverage 87.6%) | `.specs/CBL-2.6-account-plan.md` | df71640 |
| CBL-2.7 | 2026-10-14 | done, review closed (consumer payment.completed, ledger.events via outbox, kit DLQ fix, coverage 87.3%) | `.specs/CBL-2.7-payment-consumer.md` | 594afeb |
| CBL-2.8 | 2026-10-14 | done, review closed (property-based invariant tests with mutation check, coverage 87.7%) | `.specs/CBL-2.8-property-tests.md` | 2dba4a5 |
| CBL-2.10 | 2026-10-15 | done, review closed (BFF statements: ListMerchantAccounts, merchant and period filters for ListEntries, ownership fail-closed; contract cf-3.2) | `.specs/CBL-2.10-ledger-bff-extension.md` | fbe0c45 |
| CBL-16.5 | 2026-10-16 | done (English comments and documentation across the repository: code, tests, migrations, Dockerfile, load scripts, all ten specs, AGENTS.md, README; e2e runs via `test/e2e/E2E.md` with scenario `E2E_Ledger_ReserveFlow`, `reserve_flow.ps1` and `make smoke` removed; validation run PASS; coverage unchanged, 87.1% unit+integration) | `.specs/CBL-16.5-conventions.md` | this commit |

## 11. Runbook

Filled in by the first product task of the service. Typical incidents: dependency unavailability,
error growth, processing lag; actions: check health/metrics, logs with request_id, the state of
the Kafka consumer group and outbox lag, restart the service if needed.

Reserves (2.4):

1. Stuck `active` reserves outside a saga: `SELECT ref_id, account_id, amount FROM reserves
   WHERE status = 'active'` - verify against Payout (10.x); release via `ReleaseReserve`
   with the same ref, not by editing rows.
2. `conflict` (resource_exhausted) on Reserve/Commit: contention on the account; check retry
   metrics (cause `lock_conflict`, `reserve_state_race`), if systematic - the 2.9/14.6 plan.
3. Look for capture postings with `ref_type = 'reserve'`; the summary is the payout account of the
   same `merchant_ref`; balance discrepancies without postings (available/reserved) are holds,
   not lost money (T-LED-06).

Chart of accounts (2.6):

1. `account_conflict` (failed_precondition) on reserve operations: an account with a different
   decimals scale already exists for the business key; find it with `SELECT id, decimals FROM accounts WHERE
   type = ... AND asset = ... AND COALESCE(merchant_ref,'') = ...`; the fix is only via the
   ops procedure (reversal and account recreation), not by editing decimals.
2. `invalid_account_plan`: the account key does not pass the rules (asset/merchant_ref); the source is
   dirty legacy data or a consumer bug; verify against the golden vectors (CBL-003, section 9).
3. The consumer-derived `account_id` differs from the ledger one: verify the implementation against the
   vectors; moving the derivation to `cbl-kit` - KI-ledger-08.

Payment consumer and events (2.7):

1. `payment.events.dlq`: the reason is in the `x-error` header; typical reasons - an invalid event
   (payment_id/asset/amounts), a repeated completed with a changed body (`idempotency_key_reuse`);
   correcting an erroneous posting - reversal only (10.x), not repeating the event.
2. Consumer lag: metrics/logs `payment completed posted` (replay - `payment completed
   replayed`); if stalled, check the DLQ and `x-original-event-id` (the kit sanitizes broken
   event_id since the CBL-1.3 fix).
3. Outbox lag: kit relay logs; unpublished rows lie in the `outbox` table
   (retention - KI-ledger-09, 14.2); there is no loss, duplicates are possible (at-least-once).

BFF statements (2.10):

1. `invalid_query` (invalid_argument) on ListEntries: neither
   `account_id` nor `merchant_ref` is filled in, or `merchant_ref` does not follow the chart of accounts.
2. `account_not_found` under the merchant filter: the account is foreign or platform
   (clearing/fee/suspense); this is the expected fail-closed response, not a failure.
3. `invalid_period` (invalid_argument): `period_to` is not later than `period_from` or the
   timestamp is invalid; check the caller's clock (UTC).
4. An empty ListMerchantAccounts list: the merchant has no chart accounts yet (no
   postings and reserves) - not an error; verify the expected `account_id` against the
   golden vectors (KI-ledger-08).
