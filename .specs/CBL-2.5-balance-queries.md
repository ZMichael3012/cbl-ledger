# CBL-2.5. GetBalance, ListEntries (cursor) and balance snapshots

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

Ledger read requests: the account balance (for Billing API `/v1/balances`) and a posting
statement with cursor pagination (for the merchant statement and reconciliation).
Additionally, open question 2.2 is closed: a `PostTransaction` replay must return the balance
snapshot as of the posting, not the current state of the accounts
(`transactions.balances_after`).

## 2. Boundaries

In scope: `GetBalance`, `ListEntries` (gRPC, usecase, storage), the `transaction_balances`
snapshot table (migration 00008), snapshot writes in `PostTransaction`.
Out of scope: the chart of accounts and provisioning (2.6), the `payment.events` consumer
(2.7), `ledger.events` events (2.7), reporting extracts (ClickHouse, 11.x).

## 3. Functional requirements

FR-1. `GetBalance(account_id)`:
- the account must exist (`account_not_found`), the balance row must exist;
- response: `available` and `reserved` in `Money` (int64 minor units plus the account asset
  and decimals); a zero balance is returned as 0/0.

FR-2. `ListEntries(account_id, limit, cursor)`:
- the account must exist; the statement is sorted by `entry_id` ascending (append-only
  chronology); the `entry_id` order for committed postings matches the commit order per
  account: balances are updated via CAS before the postings are inserted, so the losing
  conflict rolls back the issued ids and retries with larger ones (ADR-2.5); the cursor does
  not skip records;
- the cursor is an opaque string (in the implementation, the decimal `entry_id` of the last
  record of the previous page); `next_cursor` is returned if there are newer records; an
  empty cursor means from the beginning; an unparsable or negative cursor is
  `invalid_cursor`;
- `limit`: 0 -> 50, greater than 200 -> 200 (cursor pagination without a total);
- response: `entry_id`, `transaction_id`, `account_id`, `direction`, `amount: Money`,
  `ref_type`, `ref_id`, `created_at` (from `transactions`), asset/decimals from the account.

FR-3. Balance snapshots: on the first `PostTransaction` posting, rows
`(transaction_id, account_id, available, reserved)` for all affected accounts are written to
the `transaction_balances` table in the same DB transaction. A replay by the idempotency key
returns `balances_after` from the snapshot, not the current state of the accounts (closes
open question CBL-2.2). For transactions created before migration 00008 (dev data), the
previous behavior is preserved (current balances) as a transitional fallback read.

FR-4. Error mapping: `account_not_found` (not_found), `invalid_cursor` (invalid_argument),
`invalid_account_id` (invalid_argument, transport), everything else is internal without
details. Error causes are not included in the status text (schema details do not leak).

## 4. Contracts

`LedgerService.GetBalance` and `LedgerService.ListEntries` from `billing/ledger/v1` (CF-1):
`GetBalanceRequest{account_id}` -> `AccountBalance`; `ListEntriesRequest{account_id, limit,
cursor}` -> `entries[]`, `next_cursor`. Transport input limits: `account_id` is a UUID,
`cursor` up to 64 characters, `limit` within uint32 (clamped in storage).

## 5. Data model and migrations

`migrations/00008_transaction_balances.sql`:

```sql
CREATE TABLE transaction_balances (
    transaction_id uuid        NOT NULL REFERENCES transactions (id),
    account_id     uuid        NOT NULL REFERENCES accounts (id),
    available      bigint      NOT NULL,
    reserved       bigint      NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (transaction_id, account_id)
);
CREATE INDEX entries_account_id_idx ON entries (account_id, id);
```

The table is append-only by design (written once together with the transaction); asset and
decimals are not duplicated: they are read by joining with `accounts`. `created_at` prepares
time-based partitioning (retention and maintenance in 14.2). The index `(account_id, id)`
serves the statement: `entries_account_idx (account_id, created_at, id)` does not give order
by `id`. `AutoMigrate` is forbidden.

## 6. Finite state machines

Not applicable: the requests do not change state.

## 7. Edge cases

An empty account balance (explicit zeros), a nonexistent account, an account without a
balance row (fail-closed `account_not_found`), an empty cursor, the cursor of the last page
(`next_cursor` is empty), a cursor from a statement of another account (accepted: the
`id > cursor` filter), limit 0/1/200/huge, a statement of an account without postings (an
empty page without a cursor), a `PostTransaction` replay after subsequent operations (the
snapshot), a transaction without a snapshot (legacy fallback), concurrent writes during
pagination (the cursor does not skip committed records), a garbage/negative/overflowing
cursor.

## 8. Non-functional requirements

The deadline is 500 ms (CBL-003); the statement is read via the index
`entries_account_id_idx (account_id, id)` (migration 00008), snapshots via the primary key
`(transaction_id, account_id)`; reads without an explicit DB transaction (autocommit, without
holding connections and write locks). Unit+integration coverage is at least 80%.

## 9. Test plan

| Level | Scenarios |
| :-- | :-- |
| Smoke | `go build ./...`; the compose stand migrates up to version 8 |
| Unit | transport: account_id/cursor validation; direction/snapshot mapping |
| Integration (storage) | GetBalance (balance, zeros, an account without a balance row, an unknown account); ListEntries (pages, order, ref fields, cursor, limits, a garbage cursor, a cursor from a foreign statement, an empty account); the replay snapshot after subsequent postings; legacy fallback; concurrent pagination completeness (3 writers, a reader following the cursor chain) |
| Integration (gRPC, bufconn) | GetBalance and ListEntries through the service on real PostgreSQL; error codes; a replay returns the snapshot |
| E2E (compose) | PostTransaction -> GetBalance -> ListEntries (pagination) and the replay snapshot on the compose stand |
| Load | not extended (2.9 covers writes) |

## 10. Work plan and commit

Commit `CBL-2.5: balances and statement with snapshots after a posting`; the spec is part of
the commit.

## 11. Open questions and decisions

- ADR-2.5: the cursor is the decimal `entry_id`; pagination completeness under concurrent
  writes is ensured by the order "id = commit order per account": balances are updated via CAS
  before the postings are inserted, the losing conflict is rolled back entirely and retried
  with new (larger) ids (implemented by moving the CAS before the insert in `postOnce`). When
  accounts are sharded (2.9, section 3.2; task 14.6), the cursor format is revisited; until
  then clients do not parse it.
- ADR: reads are performed without an explicit DB transaction (autocommit): a snapshot across
  `accounts`/`balances`/`entries` is not required, pool connections are not held (T-LED-04).
- Accepted with a limitation: an invalid UUID at the storage layer returns `ErrUnknownAccount`
  (unreachable from outside: the transport validates before the call); a dedicated error will
  appear when internal read calls appear (2.7).
- Accepted with a limitation: `transaction_balances` grows approximately like `entries`; the
  retention and partitioning strategy is task 14.2 (KI-ledger-07); `created_at` is already
  written.
- Open question: a statement with asset/operation type filters, as Billing API requirements
  appear (7.x); the current CF-1 contract contains no filters.
- Open question (from 2.2): closed by the `balances_after` snapshot for a replay.

## 12. Review (CBL-001, step 6)

Four independent passes (14.10.2026). No blockers; the majors (the statement index, the id and
commit order under concurrent writes, the snapshot lifecycle) are closed, minors are closed or
recorded.

| Role | Key findings | Closure |
| :-- | :-- | :-- |
| No context | the statement `created_at` is taken from `entries`, not from `transactions`; the legacy fallback branch without a test; edge test gaps (zeros, an account without a balance row, a cursor from a foreign statement); `ORDER BY e.id` is not served by `entries_account_idx`; a missing balance row is masked as `account_not_found`; outdated comments in e2e | SELECT `t.created_at`; a legacy fallback test; tests for zeros/a missing row/a foreign cursor; the index `entries_account_id_idx (account_id, id)` in 00008; the fallback is recorded in §11; the e2e comments are updated |
| Architecture | major: the statement index; major: possible record skipping when the id order and the commit order diverge (insert before CAS); major: `transaction_balances` has no `created_at` and no lifecycle; reads in an extra transaction; `invalid uuid` as not_found; the ADR about the cursor version is not implemented | the index in 00008; CAS moved before the posting insert (id order = commit order, a concurrent completeness test); `created_at` in 00008 + KI-ledger-07 (retention, 14.2); reads via autocommit; the limitation is recorded; the ADR is rewritten without the unimplemented version |
| With context | the first run was cancelled by the tool (KI-06), a repeated pass over the final tree was performed; major: step 7 not done (journal, KI-ledger-07, the KI-ledger-02 wording, contracts); minor: version 8 in the AGENTS commands, the "4/3 writers" discrepancy, the CBL-004 update | AGENTS updated (the CBL-2.5 journal entry, KI-ledger-07, KI-ledger-02 with reads, contracts, `make smoke` -> version 8); the spec is synchronized; CBL-004 is updated |
| Security | minor: the cause leaked into `account_not_found`; the statement index; the global idempotency key namespace (an oracle, KI-ledger-02); the KI-ledger-02 wording did not reflect balance/statement reads | `account_not_found` has cause `nil`; the index is added; key namespacing together with authN (KI-ledger-02); the KI wording is updated |
