# CBL-2.3. Concurrency: optimistic locking, retries with jitter, hot accounts

| Field | Value |
| :-- | :-- |
| Epic | E2, Ledger |
| Repository | `cbl-ledger` |
| Size | L |
| Wave and parallel group | W1-LEDGER |
| Depends on | 2.2, CF-1 |
| Contract version | `cbl-protos@cf-1` (`billing/ledger/v1`) |
| Status | done (review closed, commit CBL-2.3) |

## 1. Context and goal

Task 2.2 already implemented the basic concurrency framework: CAS on `lock_version`, retries
on conflict, a deterministic update order. Task 2.3 brings the mechanism to operational
quality: a managed retry policy with full jitter, conflict observability, a second
DB barrier for entry balance (T-LED-02) and hot account invariants proven under load
(`CBL-000`, invariants 2 - 5; `CBL-004`, the "hot ledger accounts" risk).

Goal: under concurrent postings to one account, lost updates, double effect and a negative
balance are impossible; the retry strategy is configurable and measurable; the sharding and
batching decision is made in 2.9 based on the benchmark of this mechanism.

## 2. Boundaries

In scope: the `RetryPolicy` retry policy (attempt count, base pause, ceiling, full
jitter), the `RetryObserver` observability hook, the fix of the wait after the last attempt,
migration 00006 (the balance constraint trigger), concurrent hot account tests,
emergency termination of retries in `ErrConflict`, the hot account benchmark (a prototype for 2.9).

Out of scope: sharding and batching (2.9), reserves (2.4), event consumer (2.7), Prometheus
metrics (0.7, 14.4; only the hook here), changing the locking model.

## 3. Functional requirements

FR-1. Retry policy: `RetryPolicy{MaxAttempts int, BaseDelay, MaxDelay time.Duration}`.
Default: 8 attempts, 5 ms base, 250 ms ceiling. Invalid values
(zero, negative, `MaxDelay < BaseDelay`) are normalized: the fields are replaced with the default
values, a ceiling below the base is raised to the base, `MaxAttempts` is capped above at 32
(protection against long-lived calls, consumer 2.7). Normalization is idempotent and is applied on
every posting so that a zero-value `Repository` works without explicit configuration.

FR-2. The delay between attempts is full jitter: `rand(0, min(MaxDelay, BaseDelay << attempt))`
with protection against int64 overflow at extreme `MaxDelay`. The total retry budget is
bounded: 8 attempts give no more than ~1 s of waiting. The wait is interrupted by `ctx.Done()`;
after the last attempt the wait is not performed (a defect of 2.2).

FR-3. Retryable causes: a `lock_version` conflict, an idempotency key insert race
(23505), a PostgreSQL deadlock (40P01). Exhausting the attempts returns `ErrConflict`
(`conflict`, resource_exhausted) and wraps the last failure cause; non-concurrency
errors are not retried.

FR-4. Observability: `RetryObserver func(RetryEvent)`; `RetryEvent{Attempt int, Cause string,
Delay time.Duration}`, where `Cause` is `lock_conflict`, `idempotency_race`, `deadlock` or
`unknown`. `main` wires an observer with a warn-level log (attempt, cause, delay); the hook
is reused by metrics in 0.7/14.4. The observer is called before the pause; the last
unsuccessful attempt has no pause and produces no event (the terminal failure is visible through
`ErrConflict` and the error metric). The observer may be called concurrently and must not panic.

FR-5. Hot accounts: global locks and `SELECT ... FOR UPDATE` are not used; conflicts
are resolved only by CAS and retries (T-06). The number of round trips per posting does not grow:
balance checks stay in the read-committed transaction, the balance update is a deterministic
CAS with an insufficient funds check before the write.

FR-6. Second DB barrier (T-LED-02): migration 00006 adds a deferred constraint
trigger on `entries`: at transaction commit the sum of debits must equal the sum of credits and
the number of entries must be at least two, otherwise the commit is rejected. The barrier fires even
on a direct insert bypassing the application. The trigger function addresses `public.entries`
and pins `search_path` (pg_catalog, pg_temp); the barrier limitations are in section 11.

FR-7. Invariants under concurrency (verified by tests): the hot account balance equals the
initial value plus the sum of applied deltas; no successful posting is lost;
the sum of successful debits equals the sum of successful credits; a negative merchant/payout balance
is impossible.

## 4. Contracts

- gRPC: `LedgerService.PostTransaction` unchanged (`cbl-protos@cf-1`).
- Kafka: unchanged.
- Storage: a new migration `migrations/00006_entries_balanced.sql`; the table schema does not change.

## 5. Data model and migrations

```sql
CREATE FUNCTION entries_transaction_balanced() RETURNS trigger
-- on commit: SUM(debit) = SUM(credit) and COUNT(*) >= 2 for NEW.transaction_id
CREATE CONSTRAINT TRIGGER entries_transaction_balanced_trigger
    AFTER INSERT ON entries DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION entries_transaction_balanced();
```

Deferral is mandatory: entries are inserted one by one, the intermediate state is unbalanced.
The cost (one aggregate per row at commit) is measured in 2.9 using the
`entries_transaction_idx` index; in case of confirmed degradation, the check is moved to a batch job
(open question of 2.9).

## 6. State machines and rules

Not applicable; the transaction is atomic. Retry rule: a retryable error -> a pause with jitter ->
the next attempt; otherwise return. A key replay with the same body is not a retry at the
DB level (idempotent replay, FR-2 of task 2.2).

## 7. Edge cases

Attempt exhaustion (ErrConflict with the last cause, no effect), context cancellation during a pause,
deadlock on opposing transactions, a long chain of conflicts on one account, direct SQL
bypassing the application (barrier 00006), a large number of entries in a transaction (the limit of
100 from 2.2), int64 overflow (the 2.2 checks are preserved), a zero policy (normalization), extreme
policy values (a `math.MaxInt64` ceiling, hundreds of attempts - protection and the ceiling of 32).

## 8. Non-functional requirements

- Retry latency: no more than ~1 s in total for 8 attempts (full jitter, a 250 ms ceiling).
- Observability: a warn log on every retry; conflict counters in 0.7/14.4 through the observer.
- Security: unchanged (KI-ledger-02 stands); the 2.2 input limits are preserved.
- Hot account benchmark: a Go benchmark in the integration suite for task 2.9.

## 9. Test plan

| Level | Scenarios |
| :-- | :-- |
| Smoke | `go build ./...`, migrations apply on a clean database (00006) |
| Unit | policy normalization (including the ceiling of 32); boundaries, ceiling and overflow protection of `nextDelay`; retry cause strings |
| Integration | hot account: 24 goroutines x 5 postings, preservation of money and balance, recorded retries (observer); deterministic attempt exhaustion (`MaxAttempts=1`, a held foreign balance row lock, waiting for `pg_stat_activity`): `ErrConflict`, no transactions or entries, funds are not debited; trigger 00006: an unbalanced insert is rejected, a single posting is rejected, a balanced direct insert is allowed |
| E2E local | unchanged: the ledger compose smoke; e2e scenarios are in 2.4 (KI-ledger-03) |
| Load | k6 and the profile are in 2.9; here the Go hot account benchmark (2.9 will complement it with a parallel profile) |

Target unit+integration coverage: at least 80% (the actual value is recorded in section 12).

## 10. Work plan and commit

1. Spec (this file), tests (unit + integration, including the benchmark), then implementation.
2. Migration 00006, changes to `repository.go`, `main.go`.
3. Four review passes of CBL-001 step 6, closing the findings.
4. AGENTS.md update (T-LED-01, T-LED-02, journal), linters, commit
   `CBL-2.3: retries with jitter and DB-level balance protection`.

## 11. Open questions and decisions

- ADR: a constraint trigger versus a trigger on `transactions` - `entries` with
  deferred firing was chosen: the check sees all transaction rows at commit and does not require
  duplicate state in `transactions`.
- ADR: full jitter instead of an exponential pause without jitter: it reduces synchronized
  repeated collisions on identical conflicts (thundering herd).
- Open question of 2.9: the cost of the deferred trigger on postings with dozens of entry
  rows (one aggregate per commit row); the batch check decision is made based on the benchmark.
- Open question of 2.9: the hot account TPS degradation threshold beyond which
  sharding/batching is introduced (T-LED-01, T-06).
- Accepted limitation: the barrier does not forbid appending a balanced pair of postings to an
  already committed transaction (append-only forbids only UPDATE/DELETE). A direct insert
  bypassing the application is outside the threat model, there is no money effect; balances and
  account statements remain correct.
- Accepted limitation: the barrier checks the numeric equality of sums and does not check the
  uniformity of the asset and decimals of the entries; at the application level
  `ErrAssetMismatch` applies (2.2), direct SQL is outside the threat model.

## 12. Review (CBL-001, step 6)

Four independent passes (14.10.2026): no-context, architecture, with-context,
security (blocking). There are no blockers; the security verdict is PASS; all major and minor
findings are closed or recorded as accepted limitations.

| Role | Key findings | Closure |
| :-- | :-- | :-- |
| No-context | FR-1 and the code diverged in the interpretation of `MaxDelay < BaseDelay`; `nextDelay` overflow at extreme values; the attempt exhaustion test could be falsely green; the spec said 16 goroutines versus 24 in the code | FR-1 reformulated (the ceiling is clamped, the ceiling of 32); `nextDelay` overflow protection + a unit test; a deterministic exhaustion test through a held lock; the spec synchronized |
| Architecture | AGENTS.md not updated (T-LED-01, T-LED-02, journal); the terminal conflict lost its cause; `MaxAttempts` was not capped above | AGENTS.md updated; `ErrConflict` wraps the last cause; the ceiling of 32 in normalization |
| With-context | AGENTS.md not updated; `nextDelay` overflow; duplication of the SQL skeleton in tests; a typo in the spec; commit statuses of 2.1/2.2 in the journal | AGENTS.md updated (journal, commit hashes); overflow protection; the `insertRawTransaction` helper, trigger tests moved to a separate file; the typo fixed |
| Security | search_path in the trigger function was resolved through the search path; append-only allows appending to a foreign transaction; asset mixing through direct SQL; `nextDelay` overflow; the observer concurrency contract | 00006: `public.entries` + `SET search_path`; two limitations recorded as accepted (direct SQL outside the threat model); overflow protection; the observer docstring extended (concurrency, panic prohibition). KI-ledger-05: 00005 was left without search_path pinning (an applied migration is not changed) |
