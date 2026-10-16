# CBL-2.8. Property-based tests of ledger invariants

| Field | Value |
| :-- | :-- |
| Epic | E2, Ledger |
| Repository | `cbl-ledger` |
| Size | M |
| Wave and parallel group | W1-LEDGER |
| Depends on | 2.2, 2.4 (reserves), 2.7 (outbox emissions) |
| Contract version | unchanged |
| Status | done, review closed |

## 1. Context and goal

The money invariants of the platform (`CBL-000`, section 8) are currently checked by
examples; task 2.8 adds property-based checks: random inputs and operation sequences on
which the invariants must always hold. It closes the residual risk "examples cover the
happy path but not the combinations" before the CF-2 freeze and the start of consumer
services.

## 2. Boundaries

In scope: property tests of the domain (`ValidateEntries`) and of storage on real
PostgreSQL (balance = the sum of postings, global sum conservation, repeat idempotency,
failure atomicity), deterministic seeds.
Out of scope: new service features, schema changes, load (2.9), property tests of other
services (they will appear in their tasks), property tests of the consumer path (covered
by 2.7).

## 3. Functional requirements

FR-1. Domain properties of `ValidateEntries` (unit, no Docker):
- two generators: guaranteed balanced sets (the "acceptance" direction must be exercised:
  the test fails if no input is accepted) and raw inputs with amounts of any sign, invalid
  directions and empty accounts (there must be at least one rejection);
- for any random inputs: validation accepts a set if and only if all amounts are positive,
  the directions are correct, the accounts are non-empty and the sum of debits equals the
  sum of credits; the independent recalculation uses `big.Int` and string direction values
  (not domain predicates);
- an accepted set is always balanced (the amounts are recalculated independently);
- int64 overflow during summation is rejected;
- posting count boundaries: `MaxEntries`, `MaxEntries-1` are accepted, `MaxEntries+2` (the
  next balanced pair over the limit) is rejected.

FR-2. Storage properties (integration, real PostgreSQL): a random sequence of operations
(PostTransaction, Reserve, CommitReserve, ReleaseReserve) on a set of accounts of two
assets (USDT and BTC); after every operation the following invariants are checked:
- per account: `available + reserved == initial + Σcredits - Σdebits` (postings are
  counted by an SQL aggregate over `entries`);
- the account `reserved` equals the sum of the amounts of its active holds
  (`reserves.status='active'`): catches a release/commit transfer of funds in the wrong
  direction and unfinished transitions;
- reserve integrity: every `committed` references an existing posting, a capture writes
  exactly a pair of postings;
- globally per asset: `Σ(available + reserved)` is conserved (every transaction is
  balanced, there are no postings "out of thin air"; asset mixing is rejected);
- every transaction in `entries` is balanced (debits equal credits);
- `reserved >= 0` for all accounts; a negative `available` only for transit accounts;
- `transaction_balances` snapshots correspond to the set of (transaction, account) pairs of
  PostTransaction postings (`ref_type <> 'reserve'`) and are absent for reserve captures;
- exact row deltas: the number of transactions, postings, holds, operations, snapshots and
  outbox envelopes equals the expected one per the success counters (catches "empty"
  operations and extra records).

FR-3. Failure atomicity: the deterministic failure cases do not depend on the seed:
imbalance, insufficient funds on a posting and on a hold (amount = available + 1), asset
mixing, commit released and release committed, a key repeat with a different body; every
class must be reproduced (class counters with an assertion), and no rejected operation
changes the invariants or the row counts of
`transactions`/`entries`/`reserves`/`reserve_operations`/`transaction_balances`/`outbox`
(in the test the repository writes envelopes to the outbox via a sink - atomicity is
checked on real rows).

FR-4. Idempotency: a repeat of successful `PostTransaction` calls with the same keys (and
body) at the end of the scenario changes neither the invariants nor the counters; a key
repeat with a different body is rejected.

FR-5. Determinism: the generator seed is fixed by default and overridden by the
`PROPERTY_SEED` variable (for localizing rare failures) at both levels; the seed is printed
to the test log, and the stage number is included in assertion messages. Test metric: at
least 30 random operations, at least 200 domain cases per generator and mandatory failure
class counters.

## 4. Contracts

The contracts do not change. The properties are formulated over the existing storage RPC
equivalents and do not depend on the event format.

## 5. Data model and migrations

The schema does not change (migrations 00001 - 00008). The checks rely on SQL aggregates
over `entries`, `balances`, `transactions`, `transaction_balances`.

## 6. Finite state machines

Random sequences include reserve transitions only through valid states; invalid transitions
(commit released, release committed) are checked as failure properties in FR-3.

## 7. Edge cases

Zero available balance, exact exhaustion of `available` (amount = available + 1), a key
repeat (the same and a different body), opposite reserve transitions (commit released,
release committed), an account with zero postings, long postings (`MaxEntries`), a posting
with a repeated account (a snapshot per account, not per posting), amounts on the int64
boundary, fee and clearing accounts in common operations, asset mixing, two assets (BTC/8
and USDT/2), os seed override.

## 8. Non-functional requirements

The integration property test completes within 60 seconds on testcontainers PostgreSQL (a
scenario of 30 operations, the invariants are checked by aggregate queries); there are no
new dependencies (ADR-2.8). Unit+integration coverage is at least 80%; the tests are
deterministic and do not flake (fixed seeds, no timing; seeds 1 - 8 verified).

## 9. Test plan

| Level | Scenarios |
| :-- | :-- |
| Unit | `internal/domain`: 400 random sets (the balanced and the raw generator), the boundaries `MaxEntries`/`MaxEntries-1`/over the limit, overflow; live acceptance and live rejection assertions |
| Integration (storage) | `internal/storage`: 30 random operations with the FR-2 invariants checked after each; the deterministic FR-3 failures (NSF, imbalance, assets, opposite transitions, reuse) with a row-immutability check (including the outbox); replay; exact row deltas; the seed |
| E2E / Smoke | not required: the properties are checked at the storage layer; the 2.4/2.5/2.7 e2e runs remain |
| Load | not extended |

## 10. Work plan and commit

Commit `CBL-2.8: property-based tests of ledger invariants`; the spec is part of the
commit; AGENTS (commands, trade-off T-LED-10, the journal) and `CBL-004` are updated.

## 11. Open questions and decisions

- ADR-2.8: property testing without new dependencies: the domain uses the standard
  `testing/quick` (two generators: balanced and raw); storage uses a deterministic
  generator on `math/rand/v2` with a seed. The `pgregory.net/rapid` alternative was
  rejected because of a new dependency for an effect achievable with standard means
  (T-LED-10). The price of the decision: a manual scenario generator, no shrinking, the
  invariants duplicate knowledge of the SQL schema; failure localization is by the stage in
  the message and the seed (`PROPERTY_SEED`, a restart with the same seed reproduces the
  scenario; the operation journal is not printed).
- ADR: the invariants are checked by SQL aggregates (independent recalculation), not by
  reusing the code under test; independence is confirmed by a mutation check: swapping the
  release direction is caught by the invariant `reserved = Σ active holds`, snapshot
  removal by the snapshot correspondence, an idempotency bypass by the replay counters.
- Accepted with a limitation: the values of `transaction_balances` snapshots are checked by
  the examples of task 2.5 (a repeat returns the original snapshot); the property suite
  checks their presence and correspondence to (transaction, account) pairs.
- Open question: property tests of the consumer path (payment.events) may be added in 4.x
  when the payment state machine appears.

## 12. Review (CBL-001, step 6)

Four independent passes (14.10.2026). Blockers: the invariants' blindness to reserve
semantics (the release direction and statuses), the vacuous FR-3 and the unverifiable
outbox atomicity - closed by strengthening the suite; confirmed by mutations (swapping the
release direction fails on the active-holds invariant).

| Role | Key findings | Closure |
| :-- | :-- | :-- |
| No context | NSF and opposite reserve transitions are not reproduced with the default seed; the outbox is outside the counters; the "acceptance" direction in the domain is dead (`accepted=0`); PROPERTY_SEED only in storage; snapshot pairs are indistinguishable | NSF/imbalance/assets/opposite transitions are deterministic + class assertions; the outbox and reserve_operations in the counters (a real sink); two generators with live acceptance/rejection assertions; PROPERTY_SEED at both levels; a posting with a repeated account (a snapshot per account) |
| Architecture | outbox atomicity is claimed and unverifiable; opposite transitions are dead; the ADR does not describe the price; the generator does not falsify part of FR-1; snapshots are tied to ref_type='property'; one asset; no liveness checks | a real sink + counters; deterministic transitions; the ADR extended with the price; the raw generator (directions/empty accounts); the `ref_type <> 'reserve'` predicate; a second asset BTC/8 with per-asset grouping; exact row deltas per success counters |
| With context | FR-3 is partially vacuous; the outbox is outside the counters; the replay block is conditional; PROPERTY_SEED is not in the domain; duplication of examples | the same closures; `require.NotEmpty(replays)` and a scenario-composition requirement; the seed and stages; the duplication recorded as boundary tests (overflow, MaxEntries) |
| Security | BLOCKER: the invariants are blind to the reserve direction and statuses (a release mutation and a commit without finalization pass); BLOCKER: FR-3 is vacuous (the "remove the NSF check" mutation passes); BLOCKER: the outbox is not checked; no asset mixing; snapshot values | the invariant `reserved = Σ active holds` + committed integrity (catches both mutations, verified by swapping release); deterministic NSF/states with class counters; a real sink and atomicity counters; BTC/8 and rejection of a mixed posting; snapshot values are the 2.5 area (recorded) |
