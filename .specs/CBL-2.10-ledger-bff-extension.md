# CBL-2.10. Ledger gRPC extension for BFF statements

| Field | Value |
| :-- | :-- |
| Epic | E2 (Ledger) |
| Repository | `cbl-ledger` (+ contract `cbl-protos@cf-3.2`) |
| Size | S |
| Wave | W3-BILLING (prerequisite of 7.4, FR-8 of the CBL-7.1 spec) |
| Depends on | 2.5 (statement), CF-3, tag `cbl-protos@cf-3.2` |
| Status | done, review closed |

## 1. Context and goal

The 7.1 review (architecture BLOCKER) revealed: the Billing API `/balances` and
`/ledger/entries` are not implementable on the CF-1 contract - there is no list of
merchant accounts with balances and no ownership check, there is no statement filter
by merchant and period. 2.10 adds additive RPCs and fields to the Ledger gRPC (fixed
by the `cbl-protos` commit `CBL-2.10`, tag `cf-3.2`) and implements the server side,
after which 7.4 gets a data source without shared tables and independently of KI-01.

## 2. Boundaries

In scope: `ListMerchantAccounts` (merchant accounts with balances and ownership),
`ListEntries` with `merchant_ref` and a period, validation of untrusted input,
integration and e2e tests. Out of scope: the REST Billing API (7.4), `pending`
aggregation (Payment, 7.4), retention/partitioning (14.2); the DB schema does not
change (reads only).

## 3. Contract (cf-3.2)

- `ListMerchantAccountsRequest{merchant_ref}` ->
  `ListMerchantAccountsResponse{accounts[]}`; `MerchantAccount{account_id, type,
  asset, decimals, available, reserved}`; order `(asset, type)`, accounts without
  postings - zero.
- `ListEntriesRequest` + `merchant_ref = 4`, `period_from = 5`, `period_to = 6`;
  `account_id` or `merchant_ref` is filled in (at least one); the period is by the
  transaction `created_at`, `from` inclusive, `to` exclusive.

## 4. Functional requirements

FR-1. `ListMerchantAccounts`: mandatory `merchant_ref` per the chart of accounts
rules (`^[A-Za-z0-9_-]{1,128}$`); only accounts of the types `merchant` and
`payout` of this merchant are returned (platform accounts are unavailable); balances
are read via LEFT JOIN balances (no row - zeros).

FR-2. `ListEntries`: with `merchant_ref` the statement is limited to the merchant
accounts (`merchant`, `payout`); an `account_id` of a foreign merchant or of a
platform account under the merchant filter answers `NotFound` without revealing
existence (fail-closed); an `account_id` without a filter works as in 2.5 (internal
calls); an empty filter and an invalid `merchant_ref` - `InvalidArgument`
(`invalid_query`); inverted period bounds - `InvalidArgument`
(`invalid_period`).

FR-3. Period: `period_from` inclusive, `period_to` exclusive by
`t.created_at`; the 2.5 cursor is preserved (entry_id, string), pagination is
consistent under concurrent writes.

FR-4. The transport validates untrusted input before storage: the UUID `account_id`,
the `merchant_ref` format (via `domain.ValidMerchantRef`), the `Timestamp` validity
(CheckValid), the cursor length, the order of bounds.

FR-5. Tests: unit - transport validation (empty filter, broken ref/period,
empty merchant_ref); integration - merchant isolation (own and foreign accounts,
platform ones), the period (inclusive/exclusive bounds, back-date via an
append-only INSERT), the order and zero balances of `ListMerchantAccounts`,
pagination; gRPC bufconn - an end-to-end merchant scenario; e2e compose -
`ListMerchantAccounts` and the merchant statement on real gRPC+PostgreSQL.

## 5. Checks

| Check | Command | Result |
| :-- | :-- | :-- |
| build/vet | `go build ./...`, `go vet ./...`, `go vet -tags "integration e2e" ./...` | clean |
| unit | `go test ./internal/... -count=1` | green |
| integration | `go test -tags integration ./internal/storage/ ./internal/transport/grpcserver/` | green (including merchant isolation, period, list cap) |
| e2e | run instruction `test/e2e/E2E.md` (scenario E2E_Ledger_ReserveFlow, compose stand) | green |
| coverage | `-coverpkg` over internal | 88.2% (was 87.7%) |
| lint/em dash | golangci-lint in Docker; `check-no-emdash.ps1` | 0 issues; clean |

## 6. Non-functional requirements

Reads only: the data schema does not change; migration 00009 was added (a partial
index `accounts (merchant_ref, type, asset) WHERE merchant_ref IS NOT NULL`) -
the merchant account select and the statement join filter use it; the statement over
merchant accounts is a join to `entries` via `entries_account_idx` (2.5). A full
benchmark of the multi-account statement - profile 14.1/14.2 (as well as the 2.5
read load); the 500 ms deadline (CBL-000, section 3) holds at typical volumes, the
EXPLAIN review confirmed SEQSCAN only on the previous index - fixed by 00009.
Responses are bounded: the statement - a clamp of 200 (2.5), the account list - a
limit of 1000 (ErrTooManyResults - a chart of accounts anomaly); permissions: the
gRPC port is not published outside the trusted network (KI-ledger-02).

## 7. Edge cases

A merchant without accounts - an empty list (not 404); an account without a balance
row (a provisioning anomaly) - zeros in the list, the point GetBalance remains
fail-closed (2.5); foreign/platform accounts under the merchant filter -
NotFound; `to` earlier than or equal to `from` (an empty period) - InvalidArgument;
a cursor from another statement is accepted (filter by entry_id, 2.5); within
a single account the 2.5 guarantee "no skips of concurrent writes" holds
(CAS serialization of the balance); in the merchant multi-account statement there is
no such serialization: a write committed after the cursor was issued with a smaller
entry_id may not appear in the current pass (standard cursor semantics, a repeated
statement by period shows it) - recorded deliberately;
back-dating test rows - INSERT only (append-only journal).

## 8. Review (CBL-001, step 6)

Four independent passes (15.10.2026). No blockers: fail-closed isolation is
confirmed (a foreign, platform and nonexistent account_id under the merchant
filter give the same NotFound), there are no SQL injections or leaks, money is
int64+decimals only. MAJOR findings closed: error codes brought to the
spec/runbook catalog (`invalid_query`, `invalid_period`; `missing_merchant_ref` for an empty
list); the migration 00009 index added (the EXPLAIN review showed SEQSCAN accounts
on the previous index - the NFR statement corrected to the actual one); an account
list limit introduced (1000, `ErrTooManyResults` - resource_exhausted); the
pagination semantics of the multi-account statement documented (the race window, §7);
the statement filter moved into `usecase.ListEntriesFilter` (the transport no longer
imports storage). MINOR/NIT closed: tests of an invalid Timestamp (CheckValid), an
empty list and an account without a balance row, unit tests of `ValidMerchantRef`,
`mapError` cases (`invalid_query`, `too_many_results`), `merchantAccountTypes` -
an array, comment wording, the smoke script updated to version 9, journal
2.8 got a hash. Accepted deliberately: `COALESCE` zeros for an account without a row (2.5
is fail-closed for point reads only, there is no other source of the anomaly);
`account_id` without `merchant_ref` remains an internal path (it is 7.4's obligation to
always pass the token subject's `merchant_ref`, T-LED-11); no pagination of the
account list (the number of accounts - assets per merchant, bounded).

| Role | Key findings | Closure |
| :-- | :-- | :-- |
| No-context | MAJOR: codes invalid_merchant_ref/invalid_period_from|to against the spec; statuses/§5/§8 not filled in; MINOR: a double code for inverted bounds, no CheckValid test and §7 edge cases, the NFR about indexes is wrong, the transport imports storage, cf-3.2 not in CBL-003 §8; NIT: wording, the types array, an empty line | codes brought to invalid_query/invalid_period; the spec filled in; tests added; index 00009 and an honest §6; the usecase filter; entries in CBL-003; wording |
| Architecture | MAJOR: multi-account statement pagination without the 2.5 guarantee; the index and NFR are wrong, no EXPLAIN/benchmark; MINOR: codes, the /balances mapping not fixed, COALESCE against ADR 2.6, the BFF obligation to pass merchant_ref, KI text, CBL-003 §5 merchant_id, CBL-004, tests | the semantics documented; migration 00009 + §6; the code catalog; §9 mapping; the deliberate COALESCE (§7); T-LED-11 + KI; CBL-003/004 updated; tests |
| With-context | MAJOR: §5/status/§8 placeholder against the journal; CBL-004 without 2.10; the cbl-protos journal without CBL-2.10; MINOR: CBL-003 §8 cf-3.2, journal 2.8 "this commit", no LEFT JOIN and CheckValid tests, layers, the Truncate comment; NIT: the mapError case, ValidMerchantRef tests, an empty line | filled in; statuses updated; follow-up in cbl-protos; CBL-003 §8; the hash 2dba4a5; tests; the usecase filter; the comment; cases; tests; returned |
| Security | no BLOCKER; MAJOR: no limit on ListMerchantAccounts (resources), the index and NFR (EXPLAIN SEQSCAN), ownership is a filter only (a 7.4 condition); MINOR: the invalid_merchant_ref code, the COALESCE verdict, a trim nuance; NIT: logs with raw input | the cap of 1000 + ErrTooManyResults; 00009; T-LED-11/KI-ledger-02 + a 7.4 requirement; the code catalog; the §7 decision; accepted; logs are not client-side |

## 9. Decisions and open questions

- KI-ledger-08 (moving the `account_id` derivation to `cbl-kit`) does not block 2.10:
  the service works with already existing accounts; the BFF receives them as a list.
- KI-ledger-02: ownership is implemented via the `merchant_ref` filter, but
  non-authenticated gRPC retains its status until mTLS (0.6) - prod is forbidden;
  7.4's obligation is to always derive `merchant_ref` from the token subject and not
  proxy a client `account_id` without a filter (T-LED-11).
- `pending` aggregation for `/balances` - Payment data (7.4), not Ledger;
  the 7.4 mapping: `available`/`reserved` - the sum of merchant accounts by asset
  (reserved is accounted on the hold's source account, T-LED-06), payout accounts
  are not summed into balances (funds in the payout process).
- The account list without pagination (the number of merchant accounts - assets x 2, a limit
  of 1000); pagination - additive (cf-3.x) when a requirement appears.
- A full benchmark of the multi-account statement and retention - profile 14.1/14.2.

## 10. Work plan and commit

Implementation of storage/usecase/transport, tests, AGENTS/runbook, commit
`CBL-2.10: Ledger gRPC extension for BFF statements`; the contract commit has already
been released (`cbl-protos` `CBL-2.10`, tag `cf-3.2`).
