# CBL-2.2. PostTransaction with idempotency and balance checks

| Field | Value |
| :-- | :-- |
| Epic | E2, Ledger |
| Repository | `cbl-ledger` |
| Size | L |
| Wave and parallel group | W1-LEDGER |
| Depends on | 2.1, CF-1 |
| Contract version | `cbl-protos@cf-1` (`billing/ledger/v1`, `billing/common/v1`) |
| Status | done (locally, awaiting commit) |

## 1. Context and goal

The main ledger operation: a balanced double-entry transaction with key idempotency and protection
of balances from concurrent changes (`CBL-000`, invariants 2 - 5).

## 2. Boundaries

In scope: `PostTransaction` (gRPC and the storage layer), idempotent replay, insufficient funds
check, optimistic retries on conflict.
Out of scope: reserves (2.4), event consumer (2.7), `ledger.events` publication (2.7;
done in 2.7).

## 3. Functional requirements

FR-1. Validation: at least two entries, positive amounts (int64), valid directions,
debits equal credits, overflow protection for amounts (`domain.ValidateEntries`).

FR-2. Idempotency: a unique `idempotency_key` in `transactions`; the request body is captured
by the canonical hash `request_hash` (migration 00004, length-prefixed string encoding).
A replay with the same body returns the original `transaction_id` and balances (`Replayed=true`),
there is no repeated effect; a replay with a different body is rejected with `idempotency_key_reuse`
(already_exists, 409). A concurrent insert of the same key is handled via code 23505
and a repeated lookup.

FR-3. Balances: a debit decreases `available`, a credit increases it; a negative `available`
is forbidden for merchant and payout (`insufficient_funds`), transit accounts (clearing, fee,
suspense) may go negative (T-LED-03); int64 overflow is rejected
(`invalid_transaction`); the update happens only on a `lock_version` match, otherwise a retry with an
exponential pause and jitter (up to 8 attempts, a 5 ms start), then `conflict`
(`resource_exhausted`).

FR-4. All entries, the transaction and the balance updates are performed in a single PostgreSQL
transaction; all accounts in one transaction must have the same asset and decimals (`asset_mismatch`).
Balances are updated in a deterministic order (sorted by account_id); a PostgreSQL deadlock
(40P01) is retried on par with a version conflict.

FR-5. Error mapping to gRPC: `idempotency_key_reuse` (already_exists), `account_not_found`
(not_found), `insufficient_funds` (failed_precondition), `unbalanced_transaction` and
`invalid_transaction` (invalid_argument), `conflict` (resource_exhausted), everything else (internal
without details).

## 4. Contracts

`LedgerService.PostTransaction` from `billing/ledger/v1`: `EntryInput{account_id, direction,
amount: Money}` -> `transaction_id`, `balances_after[]`. The response is assembled by the transport
from balance snapshots.

## 5. Data model and migrations

Migrations 00004 (`request_hash`) and 00005 (the trigger forbidding a negative balance) from task
2.1; there are no new tables.

## 6. State machines

Not applicable; the transaction is atomic.

## 7. Edge cases

A replay with the same key after success, a concurrent replay, insufficient funds, a negative
balance forbidden, a nonexistent account, different assets, an empty key, invalid directions,
amount overflow, a large number of entries (one transaction).

## 8. Non-functional requirements

Hot accounts: optimistic retries without global locks; `SELECT ... FOR UPDATE` is not used. The
target latency and TPS are confirmed by benchmark 2.9.

## 9. Test plan

| Level | Scenarios |
| :-- | :-- |
| Smoke | `go build ./...`, service startup with PostgreSQL, healthz=ok, migrations applied |
| Unit | `domain.ValidateEntries`: boundaries, imbalance, overflow, directions |
| Integration | posting and balances; key replay; rejections (imbalance, shortage, foreign account, assets); append-only trigger; 10 concurrent debits without a negative balance |
| E2E | local ledger compose (run manually: healthz, goose versions) |
| Load | task 2.9 |

## 10. Work plan and commit

Commit `CBL-2.2: PostTransaction with idempotency and balance checks`; specs 2.1 and 2.2
are part of the commits of the respective tasks (merging is allowed at the customer's decision).

## 11. Open questions

- Returning balances on a replay reflects the current balance state rather than a snapshot at the
  time of the first posting; strict response idempotency will require a snapshot table (2.5).

## 12. Review (CBL-001, step 6)

Four independent passes (13 - 14.10.2026). Result: 2 blockers, 7 major, a series of minor findings;
all blocker and major findings are closed.

| Role | Key findings | Closure |
| :-- | :-- | :-- |
| No-context | a panic on an account UUID in a different case; no tests for key conflicts | UUID normalization (`normalizeEntries`), tests for the conflict and the concurrent key |
| Architecture | a key replay with a different body silently returned a foreign result (a CBL-003 violation); deadlock under a nondeterministic update order; the minus prohibition broke the Dr clearing / Cr merchant flow; logic in storage | `request_hash` (migration 00004) + `idempotency_key_reuse` (409); account sorting and the 40P01 retry; an account type policy (merchant/payout only); trade-off T-LED-01 |
| With-context | coverage 67.96% (below 80%), the Docker build did not work (`replace` outside the context), middleware not wired, govulncheck red | coverage 90.7% (unit + bufconn integration); the Dockerfile and compose switched to the services/ context (the build verified); ginmw/grpcmw wired; Go 1.26 and dependencies updated |
| Security | BLOCK: idempotent result substitution and reachable grpc/pgx/x-net vulnerabilities | closed: body hash + 409, dependency updates; accepted KIs: no mTLS (0.6), input limits added (100 entries, field lengths, MaxRecvMsgSize) |
