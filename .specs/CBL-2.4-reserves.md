# CBL-2.4. Reserves: Reserve, CommitReserve, ReleaseReserve

| Field | Value |
| :-- | :-- |
| Epic | E2, Ledger |
| Repository | `cbl-ledger` |
| Size | M |
| Wave and parallel group | W1-LEDGER |
| Depends on | 2.1, CF-1 |
| Contract version | `cbl-protos@cf-1` (`billing/ledger/v1`, `billing/common/v1`) |
| Status | done, review closed |

## 1. Context and goal

Reserves are the money primitive of the payout and refund saga (`CBL-000`, section 5: `requested ->
reserved (Ledger) -> sent_to_gateway -> completed/failed`, with compensation on `failed`) and
task 10.2. A reserve holds merchant funds until the external operation is confirmed; commit
turns the reserve into a posting (the money leaves), release returns the funds on cancellation.

## 2. Boundaries

In scope: `Reserve`, `CommitReserve`, `ReleaseReserve` (gRPC, usecase, storage), the
`reserves` and `reserve_operations` tables, reserve idempotency and the reserve finite state
machine, the compose e2e scenario (closes KI-ledger-03).
Out of scope: `GetBalance`/`ListEntries` requests (2.5), the chart of accounts and account
provisioning (2.6), publication of `ledger.events` (2.7, same as in 2.2), the `payment.events`
consumer (2.7).

## 3. Functional requirements

FR-1. Reserve model (ADR-2.4): a reserve is a hold (encumbrance) on a merchant balance
account; `Reserve` moves the amount from `available` to `reserved` of the same account and
creates no postings: a hold is not a double-entry posting. `ReleaseReserve` returns the amount
from `reserved` to `available` (compensation, no postings). `CommitReserve` captures the
hold: the source `reserved` decreases, the amount goes to the payout account, and a balanced
posting is created in `transactions`/`entries` (debit of the source, credit of the payout
account).

FR-2. `Reserve(idempotency_key, account_id, amount, ref_id)`:
- the account must exist, be of type `merchant` and have a non-empty `merchant_ref`
  (otherwise `reserve_not_allowed`): an account without a merchant reference does not make it
  possible to select the commit recipient unambiguously, fail-closed against mixing funds of
  different merchants;
- asset and decimals must match the account (`asset_mismatch`);
- the amount is strictly positive (`invalid_reserve`), `reserved` overflow checks;
- with `available < amount` - rejection `insufficient_funds` without changes;
- success: status `RESERVE_STATUS_ACTIVE`, `reserve_id` (UUIDv7); `ref_id` is a business
  reference (for example, payout_id).

FR-3. `CommitReserve(idempotency_key, reserve_id)`:
- reserve in `active` status: capture. In a single PostgreSQL transaction: the source
  `reserved` decreases by the hold amount; the payout account (`type=payout`, asset and
  `merchant_ref` of the source) is created idempotently if absent, its `available` grows by
  the amount; a `transactions` row is written (`ref_type='reserve'`, the reserve `ref_id`,
  the commit key and hash) and a pair of `entries` (Dr source `amount`, Cr payout account
  `amount`); the reserve receives `status=committed`, `transaction_id`,
  `counterparty_account_id`;
- `committed` status: idempotent replay, `COMMITTED` is returned, no effect (including with
  a different key: the state is unambiguous);
- `released` status: `reserve_invalid_state` (failed_precondition);
- unknown `reserve_id`: `reserve_not_found` (not_found).

FR-4. `ReleaseReserve(idempotency_key, reserve_id, reason)`:
- reserve `active`: `reserved` decreases, `available` grows by the amount; status
  `released`, `reason` is stored; no postings;
- `released` status: idempotent replay, `RELEASED` is returned (the replay reason does not
  change the result and is not part of the key hash);
- `committed` status: `reserve_invalid_state`;
- unknown `reserve_id`: `reserve_not_found`.

FR-5. Key idempotency: the `Reserve` key is unique in `reserves`, the `CommitReserve` and
`ReleaseReserve` keys are unique in `reserve_operations` (they are recorded also on a replay
in a terminal status: a key used on one reserve rejects the same operation on another reserve
as `idempotency_key_reuse`); the request body is fixed by the canonical `request_hash`
(Reserve: account/amount/ref; commit/release: reserve_id, with reason excluded for release).
A replay of the key with the same body returns the operation result (`Replayed=true` in the
storage layer; for a replay in a terminal status, the current reserve status); a replay with
a different body is `idempotency_key_reuse` (already_exists, 409); a commit key already taken
by a `PostTransaction` posting in `transactions` is also rejected as `idempotency_key_reuse`
(without retries). A concurrent key insert (23505) is retried according to the 2.3 policy.

FR-6. Concurrency: `balances` updates only via CAS on `lock_version` with `RetryPolicy`
retries (2.3); when two commits of one reserve race, one wins (exactly one posting); the
loser, after the internal retry, obtains the `committed` state from the reserve state. Lost
effects are impossible: the reserve is finalized by a conditional UPDATE
`WHERE status='active'` in the same transaction as the balances and postings.

FR-7. Error mapping to gRPC: `insufficient_funds` and `reserve_invalid_state`
(failed_precondition), `reserve_not_allowed` (failed_precondition), `reserve_not_found`
(not_found), `idempotency_key_reuse` (already_exists), `invalid_reserve`, `asset_mismatch`
(invalid_argument), `account_not_found` (not_found), `conflict` (resource_exhausted),
everything else is internal without details.

## 4. Contracts

`LedgerService.Reserve/CommitReserve/ReleaseReserve` from `billing/ledger/v1` (CF-1):
`ReserveRequest{idempotency_key, account_id, amount: Money, ref_id}` and
`CommitReserveRequest`/`ReleaseReserveRequest{idempotency_key, reserve_id[, reason]}` ->
`{reserve_id, status}`; the `ReserveStatus` enum: ACTIVE/COMMITTED/RELEASED. Transport input
limits: key and identifiers up to 128 characters, reason up to 200, amount within int64.
Accepted with a limitation: `CommitReserveRequest` has no recipient account field, so the
commit recipient is unambiguously the payout account with the same `merchant_ref` and asset
(ADR-2.4); when a real provider appears and the contract is clarified, the recipient is moved
into the request (CF-2 does not affect gRPC).

## 5. Data model and migrations

`migrations/00007_reserves.sql`:

```sql
CREATE TABLE reserves (
    id                      uuid PRIMARY KEY,
    account_id              uuid NOT NULL REFERENCES accounts (id),
    amount                  bigint NOT NULL CHECK (amount > 0),
    ref_id                  text NOT NULL,
    status                  text NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'committed', 'released')),
    idempotency_key         text NOT NULL UNIQUE,
    request_hash            text NOT NULL,
    reason                  text NOT NULL DEFAULT '',
    transaction_id          uuid REFERENCES transactions (id),
    counterparty_account_id uuid REFERENCES accounts (id),
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now(),
    CHECK (status <> 'committed' OR transaction_id IS NOT NULL)
);
CREATE INDEX reserves_account_idx ON reserves (account_id, created_at, id);
CREATE INDEX reserves_ref_idx ON reserves (ref_id);
CREATE INDEX reserves_active_idx ON reserves (status) WHERE status = 'active';

CREATE TABLE reserve_operations (
    id              bigserial PRIMARY KEY,
    reserve_id      uuid NOT NULL REFERENCES reserves (id),
    op              text NOT NULL CHECK (op IN ('commit', 'release')),
    idempotency_key text NOT NULL UNIQUE,
    request_hash    text NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now()
);
```

The GORM models `Reserve` and `ReserveOperation` mirror the DDL; `AutoMigrate` is forbidden.
Amendment to `CBL-000` (section 6): `reserves` and `reserve_operations` are added to the
ledger table list. Reserve statuses are exported from `internal/domain`.

## 6. Finite state machines

```
(none) --Reserve--> active --CommitReserve--> committed   (terminal, has a posting)
                      |--ReleaseReserve-----> released    (terminal, no posting)
```

Transitions only from `active`; a replay of an operation in a terminal status is idempotent
(the current status is returned), the opposite operation is rejected with
`reserve_invalid_state`.

## 7. Edge cases

`Reserve` replay with the same key (same and different amount), concurrent `Reserve` on one
account (must not overspend), `Commit` after `Release` and vice versa, double concurrent
`Commit` with the same and with different keys, a replay of an operation in a terminal status
with a new key (the key is recorded, capturing another reserve with the same key is rejected),
`Commit`/`Release` of an unknown reserve, an empty `idempotency_key`, a zero and a negative
amount, accounts of type `clearing`/`fee`/`suspense`/`payout`, a merchant account without
`merchant_ref`, a foreign asset, a nonexistent account, `reserved` and `available` overflow,
a collision of the commit key with a `PostTransaction` key, a disabled payout account created
on the fly, a payout account with a different decimals scale is rejected, `Release` replay
with a different reason.

## 8. Non-functional requirements

The RPC deadline is 1 s (CBL-003); hot accounts use CAS without global locks (T-LED-05), the
number of retries is bounded by `RetryPolicy`; `SELECT ... FOR UPDATE` is not used.
Unit+integration coverage is at least 80%; money scenarios only on real PostgreSQL
(testcontainers).

## 9. Test plan

| Level | Scenarios |
| :-- | :-- |
| Smoke | `go build ./...`, compose stand: healthz, migrations up to version 7 |
| Unit | `domain`: amount/ref validation, status transitions; transport: error and status mapping |
| Integration (storage) | hold and balances; key replay and reuse; rejections (amount, account type, asset, shortage); commit (posting, payout account, idempotency, state); release (return, reason, state); concurrent reserve and commit; overflow |
| Integration (gRPC, bufconn) | end-to-end Reserve -> Commit -> Release through the service with real PostgreSQL; error codes |
| E2E (compose) | scenario: seed accounts, Reserve -> Commit (verification of the posting and balances in PG) and Reserve -> Release; compose stand signatures |
| Load | reserves are not part of profile 2.9 (PostTransaction); extended in 14.x if needed |

## 10. Work plan and commit

Commit `CBL-2.4: reserves Reserve, CommitReserve, ReleaseReserve`; the spec is part of the
commit.

## 11. Open questions and decisions

- ADR-2.4: the commit recipient is the payout account with the same asset and `merchant_ref`
  (contract CF-1 does not pass the recipient); the account is created idempotently on the
  first commit; if decimals do not match the source, the commit is rejected
  (`asset_mismatch`).
- ADR: `Reserve` is available only to merchant accounts with a non-empty `merchant_ref`
  (fail-closed): otherwise funds of different merchants could be mixed in the shared payout
  account.
- ADR: `ReleaseReserve` creates no reversal postings, since a hold is not a posting; the
  operation trace is in `reserves`/`reserve_operations`; `CBL-000` (section 5) and `CBL-002`
  (GE-3) are synchronized with the wording "release of the hold without postings"; the
  Reconciler and reporting must account for `reserves` (CBL-000, section 6).
- ADR: the commit key is also written to `transactions.idempotency_key`; a key taken there is
  a key body conflict (`idempotency_key_reuse`, 409), not a concurrent race; the Reserve,
  Commit and Release keys live in different namespaces and one key may occur in each of them.
- Open question: `reserve.created/committed/released` events in `ledger.events` will be
  published in 2.7 together with `transaction.posted` (the outbox record for all ledger
  operations is centralized there); backfill of already performed operations is not provided.
  (Closed in 2.7: events are written to the outbox in the mutation transaction.)
- Open question: provisioning of merchant accounts (currently tests create SQL) is task 2.6
  (closed in 2.6: deterministic accounts and `EnsureAccount`).

## 12. Review (CBL-001, step 6)

Four independent passes (14.10.2026). No blockers; one major (aligning the release semantics
with the system specs) and one major on idempotency (the replay key in a terminal status) are
closed, minors are closed or recorded.

| Role | Key findings | Closure |
| :-- | :-- | :-- |
| No context | a commit key collision with `transactions` gives 429 instead of 409; the payout account without a decimals reconciliation; `errReserveStateRace` in the observer as unknown; test gaps; `make smoke` without a build | the `findTransaction` check -> `idempotency_key_reuse`; recipient decimals reconciliation; cause `reserve_state_race`; tests added (account types, unknown release, key collision, replay key binding, available overflow, oversubscribe boundaries); `go build ./...` added to the script |
| Architecture | major: CBL-000/`CBL-002`/T-05 describe compensation as a reversal posting, while ADR-2.4 has release without postings; recipient decimals; the replay key is not written to `reserve_operations` (a cross-reserve effect) | system specs synchronized: CBL-000 (section 5 + the note about `reserves`), CBL-002 (GE-3), the global AGENTS (T-05); decimals reconciliation; the key is recorded on a terminal replay (`recordReserveOperation`) + a binding test |
| With context | major: the same cross-reserve replay key effect; duplication of the retry loop and the commit/release skeleton; `retryCause`; edge test gaps; step 7 not done | replay key recording; the duplication is left deliberately (T-LED-01, single policy 2.3, extraction into a generic helper when retries are revisited in 14.4); the cause is added; tests added; AGENTS/spec updated by this commit |
| Security | major: `merchant_ref IS NULL` routes the commit to the shared payout account (fail-open); leakage of the account asset/decimals into the gRPC error text; the commit key: 8 idle retries and 429; recipient decimals; no length limit for `asset` | fail-closed in `Reserve` (a non-empty `merchant_ref`) + a test; the error causes `asset_mismatch`/`reserve_not_allowed`/`invalid_reserve` are no longer included in the response; the key collision is checked before locks are acquired; decimals reconciliation; `asset` limit 32 |
