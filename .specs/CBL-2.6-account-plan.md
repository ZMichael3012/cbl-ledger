# CBL-2.6. Chart of accounts: merchant, clearing, fee, payout, suspense

| Field | Value |
| :-- | :-- |
| Epic | E2, Ledger |
| Repository | `cbl-ledger` |
| Size | M |
| Wave and parallel group | W1-LEDGER |
| Depends on | 2.1, 2.4 (internal provisioning), CF-1 |
| Contract version | `cbl-protos@cf-1` (gRPC unchanged; see ADR-2.6) |
| Status | done, review closed |

## 1. Context and goal

The ledger chart of accounts: five account types (`merchant`, `clearing`, `fee`, `payout`,
`suspense`), the rules for their existence and the way consumers obtain `account_id`
(consumer 2.7, Payout 10.2, Billing API 7.4). The task closes open question 2.4:
account provisioning was performed by an SQL seed in tests and pointwise (the payout
account at commit).

The main constraint: the CF-1 gRPC contract (`LedgerService`) contains no account
management methods, and the frozen `PostTransaction`/`Reserve`/`GetBalance` accept a
ready-made `account_id`. Therefore, `account_id` must be computable by a consumer from the
business key without an additional RPC (ADR-2.6).

## 2. Boundaries

In scope: the chart of accounts (types, mandatory `merchant_ref`, asset, decimals), the
canonical business key and the deterministic `account_id` (UUIDv5), idempotent provisioning
(`EnsureAccount`), moving the internal payout account provisioning (2.4) to the plan, golden
vectors for consumers.
Out of scope: posting templates for business events (2.7), new gRPC methods, changes to
migrations 00001 - 00008, partitioning.

## 3. Functional requirements

FR-1. Account business key: `(type, asset, decimals, merchant_ref)`. Plan rules:

| Type | `merchant_ref` | Purpose |
| :-- | :-- | :-- |
| `merchant` | mandatory | merchant balance per asset (Dr clearing / Cr merchant on `payment.completed`) |
| `payout` | mandatory | merchant payout account (recipient of `CommitReserve`) |
| `clearing` | forbidden | transit collection of platform payments per asset |
| `fee` | forbidden | platform fees per asset |
| `suspense` | forbidden | reconciliation discrepancies per asset |

Validation: `type.Valid()`, `asset` matching `^[A-Z0-9]{1,16}$`, `decimals` 0 - 18,
`merchant_ref` matching `^[A-Za-z0-9_-]{1,128}$` (no colons: the key is serialized with
colons), mandatory for `merchant`/`payout`, empty for the rest.

FR-2. Deterministic `account_id` (ADR-2.6): UUIDv5 (SHA-1) of the canonical name

```
cbl:ledger:account:v1:<type>:<asset>:<decimals>:<merchant_ref>
```

in the standard `uuid.NameSpaceURL` namespace. The same business key always yields the same
`account_id` in any service; the golden vectors (section 4) allow verifying an
implementation without a shared library. Determinism applies only to newly created
accounts: existing rows with the business key but a different `id` (legacy, dev data)
remain valid, and `EnsureAccount` returns them as is.

FR-3. `EnsureAccount(type, asset, decimals, merchant_ref)`: idempotent provisioning.
Algorithm: lookup by business key -> if present, verify `decimals` (`account_conflict` on
mismatch) and return the row; if absent, insert the account with the deterministic `id` and
a zero balance row in one DB transaction; an insert suppressed by the unique index (a race)
re-reads the canonical row by the key - the winner determines both the account and the
balance row. Balance row healing is allowed only for accounts without postings: an account
with `entries` but without `balances` is data corruption, and provisioning returns
`account_not_found` instead of masking it with zero. Errors: `invalid_account_plan`
(violation of the FR-1 rules), `account_conflict` (an existing account with other
parameters); at the `CommitReserve` boundary (2.4) a decimals conflict is mapped to
`asset_mismatch`. The account type check for reserves (2.4) and the fail-closed behavior on
`merchant_ref` are preserved.

FR-4. The internal provisioning of 2.4 is moved to the plan: `CommitReserve` obtains the
payout account via `EnsureAccount` (a deterministic `id` instead of a random UUIDv7);
existing payout accounts with a different `id` are reused without errors.

FR-5. The usecase exposes `EnsureAccount` (for consumer 2.7 and Payout requests inside the
service); the external gRPC contract does not change. Consumers that need `account_id`
(Billing API, Payout) compute it per FR-2; reading a missing merchant account is treated by
the consumer as a zero balance (the `GetBalance` contract does not change).

## 4. Contracts

gRPC/Protobuf do not change (CF-1). The cross-service chart of accounts contract is the
FR-2 algorithm plus the golden vectors; it is fixed in `CBL-003` (section 1, the note on
identifiers, and the new "Ledger chart of accounts" subsection). Vectors:

| Business key | account_id |
| :-- | :-- |
| `clearing/USDT/2/` | `dec27141-ec8f-55de-a325-04a35cd6b0f2` |
| `fee/USDT/2/` | `b138b712-5940-59bc-82bd-19f653642227` |
| `fee/USDT/0/` | `8ee4265f-342c-5dbc-8d80-2516a5641b59` |
| `suspense/USDT/2/` | `cb2ccbda-92f9-5889-ba9f-c7ec90b8cfa0` |
| `merchant/USDT/2/m-1` | `58686062-c133-5d4e-b08b-0d33a0db924f` |
| `merchant/BTC/8/m-1` | `e1a671a1-a662-5305-b144-6e623099ea46` |
| `merchant/USDT/2/M-1_a` | `00f2dc5b-a0c1-51c1-b2eb-f261e0ed538f` |
| `payout/USDT/2/m-1` | `0595b42d-8f10-5bb5-b98f-a349594bc3f8` |

## 5. Data model and migrations

The schema does not change (migrations 00001 - 00008 are untouched): `accounts.id` is
already `uuid PRIMARY KEY`, the business key is protected by the unique index
`accounts_type_asset_merchant_idx`. The new plan rules are enforced by validation and the
single provisioning path; direct SQL bypassing the plan is possible (trade-off T-LED-07),
but money invariants are not violated (reserves are fail-closed, transactions are
balanced).

## 6. Finite state machines

Not applicable: accounts do not change state; changing `decimals` or `merchant_ref` is
forbidden (checked during provisioning).

## 7. Edge cases

A repeated `EnsureAccount` (the same `id`), concurrent provisioning (an insert race,
including with different `decimals`: the winner occupies the key, the losers get
`account_conflict`, and no raw DB errors leak out), the same business key with other
`decimals` (`account_conflict`; the same asset with another scale but a free key is another
account), violation of the plan rules (asset, `merchant_ref` length/characters,
`merchant_ref` on a platform type, a missing `merchant_ref` on a merchant type), a legacy
row with a random `id`, an account with postings but without a balance row (healing
refused), zero balance of a new account, compatibility with `CommitReserve` and `Reserve`
(2.4).

## 8. Non-functional requirements

Provisioning is one short transaction (lookup + insert), called on the "cold" path of the
first operation; races are closed by the unique index without global locks. Unit+integration
coverage is at least 80%; the golden vectors freeze the contract.

## 9. Test plan

| Level | Scenarios |
| :-- | :-- |
| Smoke | `go build ./...`; compose stand without schema changes (version 8) |
| Unit | domain: key validation per type, determinism and golden vectors (including BTC/8, decimals 0, a ref with `_`), key helpers |
| Integration (storage) | EnsureAccount: creation, repeat, concurrent call, race with different decimals, decimals conflict, legacy row, plan rules, balance row healing and refusal in the presence of postings; CommitReserve uses the deterministic payout account |
| Integration (gRPC, bufconn) | bufconn seed via `EnsureAccount`; PostTransaction/GetBalance/Reserve on provisioned accounts; verification of the derived id against the plan |
| E2E (compose) | accounts via provisioning and funding by a Dr clearing / Cr merchant posting (no SQL); Reserve/Commit/Release and ListEntries |
| Load | not extended |

## 10. Work plan and commit

Commit `CBL-2.6: deterministic accounts and chart of accounts provisioning`; the spec is
part of the commit; `CBL-003` (the note on identifiers, section 9 and the vectors) and
`CBL-000` (section 6, the line about accounts) are synchronized.

## 11. Open questions and decisions

- ADR-2.6: deterministic `account_id` (UUIDv5 from the business key) instead of the
  `EnsureAccount` RPC: the CF-1 contract is frozen and contains no account management;
  computing on the consumer side removes the dependency and an extra round trip; the price
  is a deviation from "UUIDv7 for all entities" (recorded by a note in `CBL-003`, section 1)
  and the need for golden vectors against implementation drift. The implementation lives in
  the ledger `internal/domain`; consumers (Billing API 7.4, Payout 10.2) must reproduce the
  algorithm from the vectors - the move to the shared `cbl-kit` library is planned before
  those tasks start (KI-ledger-08).
- ADR: the canonical `decimals` of an asset is set by the first provisioning (a platform
  event); a unified asset registry is outside the scope of 2.6; a scale mismatch between
  services leads to different accounts and is diagnosed via `account_conflict` and alerts
  (runbook).
- Accepted: legacy accounts with the same business key but a different `id` are reused;
  consumer-side id derivation does not work for legacy data, the bridge is ops/SQL only;
  prod starts on an empty account registry, dev stands are recreated (`down -v`).
- ADR: balance row healing only for accounts without postings; an account with `entries`
  but without `balances` is corruption and returns `account_not_found` (not masked with
  zero).
- Note: sharding subaccounts (2.9, section 3.2; task 14.6) will require a key schema
  version (a `v2` prefix) or a separate field; the interaction is fixed in 14.6.
- ADR: the plan rules are checked by validation and the provisioning path, but not by a DB
  CHECK constraint: a migration with CHECK would break already applied dev stands, and
  money invariants are not affected (T-LED-07).
- Open question: posting templates for business events (`payment.completed`: Dr clearing /
  Cr merchant, fee) - task 2.7.
- Open question: mapping of "account missing -> zero balance" in the Billing API - task 7.4.

## 12. Review (CBL-001, step 6)

Four independent passes (14.10.2026). No blockers; the majors (the insert race with
different decimals, the placement and drift of the id derivation, the bufconn scenario,
T-LED-07) are closed, the minors are closed or recorded.

| Role | Key findings | Closure |
| :-- | :-- | :-- |
| No context | the suppressed-insert race inserts the balance by the derived rather than the canonical id (an FK error instead of `account_conflict`); a contradiction in FR-3 (`asset_mismatch` vs `account_conflict`); no race and boundary tests; a dead decimals check | the canonical row is re-read before the balance row, the order fixed; FR-3 synchronized; tests for the mixed-decimals race, corruption without balances, validation boundaries; the unreachable check removed |
| Architecture | major: the id derivation implementation is not frozen by a shared library, the vectors are narrow; major: erroneous decimals permanently occupy the key with no plan; major: legacy rows violate "one key - one id" for consumers; T-LED-07 missing; a conflict with the 2.9 subaccounts | the vectors extended (BTC/8, decimals 0, `M-1_a`); KI-ledger-08 (move to `cbl-kit` before 7.4/10.2) and the runbook `account_conflict` procedure; the legacy boundary recorded (prod on an empty registry, dev - recreation); T-LED-07 added; the interaction with 14.6 noted |
| With context | the bufconn line of the test plan was not fulfilled (SQL seed); T-LED-07; a dead check; SQL funding of e2e; ambiguity of §7 about decimals; spec dependencies | `seedGrpcAccount` moved to `EnsureAccount` + an id verification test; T-LED-07 added; the check removed; e2e is funded by a Dr clearing / Cr merchant posting; §7 and the dependencies clarified |
| Security | major: the canonical nature of decimals across services is not frozen; the insert race yields Internal; dirty legacy breaks CommitReserve (Internal); healing masks corruption; predictable ids are not a capability (KI-ledger-02); no legacy payout test | decimals/the registry fixed in §11 + CBL-003; the race fixed; `invalid_account_plan`/`account_conflict` mapped to failed_precondition; healing limited to accounts without postings; the KI-ledger-02 wording extended; the legacy payout test already exists (`TestIntegrationCommitReusesExistingPayoutAccount`) and is extended with an inheritance check |
