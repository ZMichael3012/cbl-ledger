# CBL-2.1. Ledger DB schema

| Field | Value |
| :-- | :-- |
| Epic | E2, Ledger |
| Repository | `cbl-ledger` |
| Size | M |
| Wave and parallel group | W1-LEDGER |
| Depends on | 1.2, 1.3 (cbl-kit packages), CF-1 |
| Contract version | `cbl-protos@cf-1` (`billing/ledger/v1`, `billing/events/v1`) |
| Status | done (locally, awaiting commit) |

## 1. Context and goal

The ledger schema is the foundation of tasks 2.2 - 2.9 and of all money flows of the platform
(`CBL-000`, invariants 2 - 5). Double entry, append-only entries, optimistic balance locking.

## 2. Boundaries

In scope: goose migrations, GORM models, direction and account type constants, reference copies
of outbox/inbox from `cbl-kit`.
Out of scope: `PostTransaction` logic (2.2), reserves (2.4), payment consumer (2.7).

## 3. Functional requirements

FR-1. Migrations are forward only, goose format (`-- +goose Up` / `-- +goose Down`):

| File | Content |
| :-- | :-- |
| `migrations/00001_ledger_init.sql` | `accounts`, `transactions`, `entries`, `balances`, append-only trigger |
| `migrations/00002_outbox.sql` | a copy of `cbl-kit/migrations/outbox.sql` |
| `migrations/00003_inbox.sql` | a copy of `cbl-kit/migrations/inbox.sql` |
| `migrations/00004_transaction_request_hash.sql` | `request_hash` column for key reuse control |
| `migrations/00005_balances_non_negative.sql` | trigger forbidding a negative `available` for merchant and payout |

FR-2. `accounts`: `id uuid PRIMARY KEY`, `type text NOT NULL` (merchant, clearing, fee, payout,
suspense), `asset text NOT NULL`, `decimals int NOT NULL CHECK (decimals BETWEEN 0 AND 18)`,
`merchant_ref text NULL`, `created_at timestamptz NOT NULL DEFAULT now()`; a unique index
`(type, asset, COALESCE(merchant_ref, ''))`: accounts without merchant_ref are also unique within
a type and asset.

FR-3. `transactions`: `id uuid PRIMARY KEY`, `idempotency_key text NOT NULL UNIQUE`,
`ref_type text NOT NULL`, `ref_id text NOT NULL`, `request_hash text NOT NULL` (canonical
body hash, migration 00004), `created_at timestamptz NOT NULL DEFAULT now()`.

FR-4. `entries` (append-only, without UPDATE and DELETE):
`id bigserial PRIMARY KEY`, `transaction_id uuid NOT NULL REFERENCES transactions(id)`,
`account_id uuid NOT NULL REFERENCES accounts(id)`, `direction text NOT NULL CHECK (direction IN
('debit','credit'))`, `amount bigint NOT NULL CHECK (amount > 0)`, `created_at timestamptz NOT
NULL DEFAULT now()`; indexes `(account_id, created_at, id)` and `(transaction_id)`.
The `entries_append_only` trigger raises an exception on UPDATE and DELETE.

FR-5. `balances`: `account_id uuid PRIMARY KEY REFERENCES accounts(id)`,
`available bigint NOT NULL DEFAULT 0`, `reserved bigint NOT NULL DEFAULT 0 CHECK (reserved >= 0)`,
`lock_version bigint NOT NULL DEFAULT 0`, `updated_at timestamptz NOT NULL DEFAULT now()`;
updates only through an optimistic lock on `lock_version` (2.3). A negative
`available` is forbidden by the `balances_non_negative` trigger for merchant and payout
accounts (migration 00005); transit accounts (clearing, fee, suspense) may go negative (T-LED-03).

FR-6. GORM models mirror the DDL; `AutoMigrate` is forbidden. Money fields are int64 only.
Domain constants of account types and directions are exported from `internal/domain`.

FR-7. Local dependencies: the service `go.mod` wires `github.com/cbl/cbl-kit` and
`github.com/cbl/cbl-protos` through temporary `replace` directives pointing to the sibling
directories `../cbl-kit` and `../cbl-protos`; the replace is removed after the modules are
published in CI (0.2).

## 4. Contracts

The table mapping GORM models to the `billing/ledger/v1` contract:
`EntryInput/Entry` <- `entries`, `AccountBalance` <- `balances`, `Direction` enum <- the
`direction` column. `ledger.events` events are built from `transactions` and `entries` (tasks 2.2, 2.7).

## 5. Data model and migrations

See FR-2 - FR-5. All amounts are in asset minor units; asset and decimals are taken from the account.

## 6. State machines

Not applicable; payment and payout states live in their own services.

## 7. Edge cases

Entry balance check at the application level (2.2), UPDATE/DELETE prohibition via the trigger,
concurrent balance updates (2.3), empty entries, negative amounts, a nonexistent account.

## 8. Non-functional requirements

Migration application is idempotently repeatable (goose versioning). Indexes for hot balance
and statement queries. Schema verification on real PostgreSQL.

## 9. Test plan

| Level | Scenarios |
| :-- | :-- |
| Smoke | `go build ./...`, migrations apply on a clean database |
| Unit | model constants and validators |
| Integration | testcontainers PostgreSQL: migrations up; the trigger blocks UPDATE/DELETE on entries; CHECK reserved >= 0; UNIQUE idempotency_key; FK to a nonexistent account |
| E2E | local ledger compose: health + migration application |
| Load | not applicable (2.9) |

## 10. Work plan and commit

Commit `CBL-2.1: ledger schema with append-only entries and balances`; the spec is in the same commit.

## 11. Open questions

- Time-based partitioning of `entries` is postponed until benchmark 2.9.

## 12. Review (CBL-001, step 6)

Four independent passes (13 - 14.10.2026): no-context, architecture, with-context,
security (blocking).

| Role | Key findings | Closure |
| :-- | :-- | :-- |
| No-context | the unique index of `accounts` is stricter than FR-2; `CHECK reserved >= 0` and FK constraints are not covered | FR-2 clarified (COALESCE), `reserved`/FK tests added |
| Architecture | there is no second DB barrier for balances; the sum of entries is controlled only in code | migration 00005 (a non-negative trigger for merchant/payout); the sum constraint trigger is a candidate for 2.3 (T-LED-02) |
| With-context | duplication of helper wiring between services | `storagex.Migrate` moved to cbl-kit (T-KK6) |
| Security | a negative `available` is not forbidden at the DB level | closed by migration 00005 and the `TestIntegrationBalancesNonNegativeTrigger` test |
