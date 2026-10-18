# CBL-17.10. Ledger fee split and top-up coverage

| Field | Value |
| :-- | :-- |
| Epic | E17 (real provider integration) |
| Repository | `cbl-ledger` |
| Size | S |
| Wave and parallel group | W5-PROVIDER |
| Depends on | 2.7 (payment consumer), 17.10 payment increment (`payment.events` fee split), CF-4 contracts |
| Contract version | none (CF-4 fields already frozen; no `.proto` change) |
| Status | done |

## 1. Context and goal

The payment service now emits the provider fee split (`gateway_fee`,
`network_fee`, `fx_spread`) in `payment.events` besides the aggregate `fee`
(CF-4, payment increment of 17.10), and credits repeated deposits as derived
completed child payments. The ledger posts `fee` as real money
(Dr clearing / Cr merchant net / Cr fee); therefore the split that produced the
total must be validated fail-closed before a posting, and the top-up child
payments must reach the ledger through the ordinary `payment.completed` path.
This increment realises both on the ledger side without changing the posting
scheme.

## 2. Scope

In scope: split validation in `buildCompletedPosting` (non-negative, same
asset/scale as the received amount, sum equals `fee` when components are
present, overflow-safe), the ambiguous legacy case documented (no components ->
`fee` accepted), unit/integration/e2e tests including the top-up child coverage.

Out of scope: postings by component (the ledger credits one fee account; the
split is provider evidence, not an accounting dimension); suspense routing of
unmatched deposits (17.13); payment-side rules (payment increment 17.10);
contracts (no `.proto` change).

## 3. Architecture sketch

`internal/usecase/payment_consumer.go` gains one validation step inside
`buildCompletedPosting` after the existing fee checks:

```
splitSum, present := feeSplitTotal(event)      // nil components = 0, only non-negative parts counted
if present:
    require splitSum == fee.minor_units        // cf-4.1: the total is the sum of the components
    require every part.asset/decimals == received.asset/decimals
    require no overflow while summing
else:
    fee accepted as-is (legacy producers without a split)
```

The posting entries are unchanged: `Dr clearing received`, `Cr merchant
received - fee` (if positive), `Cr fee fee` (if positive). A derived top-up
child is an ordinary completed payment event: the idempotency key
`payment:<child>:completed` makes it a separate, replay-safe posting with the
delta amount.

## 4. Functional requirements

FR-1. A completed event whose fee components are present and consistent with
`fee` (sum equal, same asset/scale, non-negative) is posted with the unchanged
posting scheme.

FR-2. An inconsistent split (sum mismatch, negative component, foreign asset or
scale, overflow) is an invalid event (`errInvalidPaymentEvent`): no posting, no
outbox event, the kit retry/DLQ path applies.

FR-3. A completed event without components keeps the current behavior: `fee`
(validated as before) is posted as-is.

FR-4. A top-up child payment (a separate `payment_id` with the delta amount)
posts exactly like any completed payment: Dr clearing / Cr merchant /
Cr fee with its own idempotency key; the parent posting is untouched.

## 5. Contracts

No `.proto` change: `payment_events.proto` fields 14-16 (`gateway_fee`,
`network_fee`, `fx_spread`) are consumed as evidence only.

## 6. Data model and migrations

None: no schema change, no migration, no new dependency.

## 7. State machines and rules

None: the consumer remains stateless per event; idempotency is the business key
`payment:{payment_id}:completed` plus the inbox barrier (T-LED-08).

## 8. Edge cases

E-1. `fee` absent, no components -> no fee posting (two entries), as before.
E-2. Components present, `fee` absent -> invalid (sum differs from zero).
E-3. All components zero (explicit Money with 0) -> treated as absent; the
validation of `fee` is unchanged.
E-4. A component in a foreign asset/scale -> invalid, even when the numeric sum
matches.
E-5. Sum overflow of two non-negative int64 components -> invalid, no wrap.

## 9. Non-functional requirements

- NFR-1. Coverage stays at least 80% unit+integration.
- NFR-2. No new dependencies, no schema change.
- NFR-3. Money stays int64 minor units; no float.
- NFR-4. English only, no em dash, godoc on exported entities.

## 10. Test plan

### 10.1. Unit matrix

| ID | Case | Input | Expected |
| :-- | :-- | :-- | :-- |
| P-01 | consistent split | fee 1500, parts 1000/300/200 | posting with fee 1500 |
| P-02 | no split | fee 1500, no parts | posting with fee 1500 |
| P-03 | sum mismatch | fee 1500, parts 1000/300 | invalid event |
| P-04 | negative component | gateway -1 | invalid event |
| P-05 | foreign asset/scale | network 100 BTC/8 | invalid event |
| P-06 | split without fee | parts 100, fee nil | invalid event |
| P-07 | overflow | gateway MaxInt64, network 1 | invalid event |

### 10.2. Integration matrix (PostgreSQL, inbox processor)

| ID | Case | Input | Expected |
| :-- | :-- | :-- | :-- |
| I-01 | split posting | consistent completed event | balances: merchant net, fee total, clearing negative; 3 entries |
| I-02 | split rejection | inconsistent completed event | error, no transaction, no outbox row |
| I-03 | top-up child coverage | parent completed + child completed (split) | two transactions, merchant net of both deltas, fee account sum, two ledger.events |

### 10.3. Smoke set

`make test`, `make test-integration`, `make test-e2e` (testcontainers Kafka flow),
`make vet`.

### 10.4. E2E scenario descriptions

| Scenario | Description | Tag or stand |
| :-- | :-- | :-- |
| E2E_Ledger_ReserveFlow (checks extended) | `test/e2e/reserve_flow_test.go` on the compose stand (targeted `up postgres ledger`): reserve lifecycle, statements and ownership; additionally `test/e2e/payment_consumer_kafka_test.go` provisions its own testcontainers Kafka and PostgreSQL and verifies the consumer checks of this increment: a completed event with a split posts and publishes `ledger.events`, a top-up child event posts a second transaction with the delta, duplicates do not double-post | `test/e2e/E2E.md`, `go test -tags e2e ./test/e2e/` |

### 10.5. Load plan

Not applicable (one comparison per completed event).

### 10.6. Test-first evidence

Red run recorded before implementation (18.10.2026):
`go test ./internal/usecase/ -run TestBuildCompletedPostingFeeSplit -count=1`
failed on HEAD: the sum mismatch, split without fee, foreign component asset,
sum overflow and negative component cases were accepted (the split was
ignored); the consistent-split and no-split cases passed. After
implementation: unit green; the full battery with integration
(`go test -tags integration -coverpkg=./internal/... ./...`) green, total
coverage 88.7%; the E2E run per `test/e2e/E2E.md` (targeted compose stand
`up postgres ledger`, the full `go test -tags e2e ./test/e2e/` suite with the
reserve/statements scenarios and the consumer Kafka flow with the split and the
top-up child) PASS (mark `20261018t141304z`, artifacts in
`test/e2e/.runs/20261018t141304z/`). The `go mod tidy` leftovers of the Go
1.26 toolchain wave are folded into the commit (testify/cpuid patch bumps).

Review of the staged round (18.10.2026, four reviewers) produced no blocking
findings; the minor findings were closed on the staged diff: the overflow case
now pins the explicit guard (a wrap landing exactly on the fee), the rejection
subtests assert `errInvalidPaymentEvent`, the zero-component E-3 case is
covered, the I-01 integration case asserts the clearing balance and the entry
count, the e2e run uses the documented targeted stand with prepare evidence
and the summary template, and the scenario naming/attribution matches reality.

## 11. Work plan and commits

Single commit: spec, validation, tests, e2e extension, AGENTS journal; the
`go mod tidy` leftovers of the toolchain wave are folded in.
