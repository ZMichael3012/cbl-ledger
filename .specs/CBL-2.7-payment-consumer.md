# CBL-2.7. payment.events consumer and ledger.events publication

| Field | Value |
| :-- | :-- |
| Epic | E2, Ledger |
| Repository | `cbl-ledger` |
| Size | M |
| Wave and parallel group | W1-LEDGER |
| Depends on | 2.2, 2.4, 2.6, CF-1 |
| Contract version | `cbl-protos@cf-1` (`billing/events/v1`, no proto changes) |
| Status | done, review closed |

## 1. Context and goal

Two deferred ledger contracts are closed (`CBL-000`, payment acceptance flow, step 6):
the Ledger consumes `payment.events` and on `payment.completed` makes a Dr clearing /
Cr merchant posting idempotently by the key `payment_id + event_type`; and it produces
`ledger.events` (`transaction.posted`, `reserve.created/committed/released`) via the
outbox (deferred by specs 2.2 and 2.4 to this task).

## 2. Boundaries

In scope: the `payment.events` consumer (group `cbl-ledger-payments`), the posting template
of a completed payment, publication of `ledger.events` for all ledger mutations via the
outbox in the business-change transaction, main and compose stand configuration,
integration and e2e tests (Kafka + PostgreSQL).
Out of scope: proto changes (CF-1/CF-2 are not affected), other payment event types,
consumers/producers of other topics, Schema Registry (as in 3.5: schema_version via a
header), outbox lag metrics (0.7/14.x).

## 3. Functional requirements

FR-1. The `payment.events` consumer (kit: `inbox.Processor` + `kafkaconsumer` + retry
topics and DLQ): the `PaymentEvent` envelope (protobuf) is decoded from the raw body; only
`PAYMENT_EVENT_TYPE_COMPLETED` is processed, other types are acknowledged without effect
(the ledger keeps no account state). Deduplication is by inbox on `(consumer, event_id)`;
poison messages go to retry/DLQ per the kit mechanics; redelivery creates no second effect
(two barriers: the inbox and the posting business key).

FR-2. The `payment.completed` posting template (ADR-2.7):
- accounts per the chart of accounts (2.6): the merchant account `(merchant_id, asset,
  decimals)`, the platform `clearing` and `fee` accounts per asset; the asset and decimals
  are taken from `received_amount`;
- postings: Dr clearing `received`; Cr merchant `received - fee` (if greater than zero);
  Cr fee `fee` (if greater than zero); with `fee = 0` there are two postings, with
  `received = fee` there is no merchant posting;
- idempotency key `payment:{payment_id}:completed`, `ref_type = payment`,
  `ref_id = payment_id`;
- validation of the untrusted event: `payment_id` is a UUID with canonicalization to
  lowercase (the business key and `ref_id` do not depend on the UUID representation);
  `merchant_id` follows the plan `merchant_ref` rule (`^[A-Za-z0-9_-]{1,128}$`);
  `received > 0`; `0 <= fee <= received`; the asset/decimals are valid and the same for
  received and fee; `expected_amount` is ignored (the calculation uses the received amount);
- an event with invalid fields is not retried indefinitely: a validation error is
  deterministic and normally goes to the retry topics, then to the DLQ (the kit mechanics),
  with the reason written to the `x-error` header; a broken redelivery envelope (an invalid
  `event_id`, a missing `event_type`) is sanitized by the kit fix `CBL-1.3 (fix)` and also
  goes to the DLQ without looping the partition.

FR-3. Publication of `ledger.events` (outbox, in the same DB transaction as the business
change):

| Event type | Source | `transaction_id` | `entries` | Partition key |
| :-- | :-- | :-- | :-- | :-- |
| `transaction.posted` | `PostTransaction`, the FR-2 posting | filled | the transaction postings | the first account_id of the postings (lexicographically minimal) |
| `reserve.created` | `Reserve` | empty | empty | the reserve account_id |
| `reserve.committed` | `CommitReserve` | filled | the pair Dr source / Cr payout | the source account_id |
| `reserve.released` | `ReleaseReserve` | empty | empty | the reserve account_id |

The body is the protobuf `billing.events.v1.LedgerEvent` (`transaction_id`, `type`,
`ref_type`, `ref_id`, `entries[]`, `occurred_at`); the kit envelope headers: `event_type`,
`producer` (`cbl-ledger`), `schema_version` (`v1`), `occurred_at`, `traceparent` when
present. Publication is by the kit relay (`FOR UPDATE SKIP LOCKED`, at-least-once). Events
are written only on the first mutation: a replay by the idempotency key creates no repeated
envelopes.

FR-4. The consumer path does not duplicate the Guard business transaction: the handler
performs `EnsureAccount` + `PostTransaction` in its own transactions, and the inbox record
is the second barrier. A failure between the posting commit and the Guard commit is safe:
the repeat is recognized by the business key (ADR-2.7). The reverse order is impossible
(the Guard inserts the inbox before calling the handler). The posting, the outbox envelope
and the snapshots are written atomically.

FR-5. Configuration: when `KAFKA_BROKERS` is set, the relay and the consumer start; the
`KAFKA_CONSUMER_GROUP` group defaults to `cbl-ledger-payments`; when `KAFKA_BROKERS` is
empty, the service runs without Kafka (WARN), the gRPC path works and outbox events keep
accumulating. The compose stand creates the topics `payment.events` (+ retry/DLQ) and
`ledger.events`.

## 4. Contracts

Topics and headers are in `CBL-003`, section 6. The `ledger.events` key is described in the
contract as `account_id`; for multi-account postings ADR-2.7 clarifies the choice of the
"first" posting (the lexicographically minimal account_id), which preserves a stable event
partition. The business key format `payment:{payment_id}:completed` matches `CBL-003`
(section 5, posting idempotency rules) and `CBL-000` (step 6).

## 5. Data model and migrations

The schema does not change: `outbox` and `inbox` are created by migrations 00002/00003 (the
kit reference). Postings and snapshots use existing tables; reserves - 00007.

## 6. Finite state machines

payment.completed -> posting (terminal for the ledger); a repeat of any event type does not
change the state (inbox deduplication, business key replay).

## 7. Edge cases

A duplicate event (the same `event_id`), a repeated notification with a different `event_id`
for the same payment (business key replay), a repeated completed with a changed body
(received/fee) - a body conflict on the key `idempotency_key_reuse`, retries and DLQ;
correction is only via reversal (10.x), not by repeating the event; `fee = 0`,
`received = fee` (no merchant posting), `fee > received` and negative amounts (rejected), a
non-UUID `payment_id`, `merchant_id` outside the plan rules, different assets of
received/fee, an event before `completed` (skipped), `UNSPECIFIED` and an unknown enum
(skipped), broken protobuf, a message without `event_id` and an invalid `event_id` (kit
DLQ), an invalid traceparent (ignored), Kafka unavailable (the outbox accumulates, the
consumer waits), a failure after the posting but before the Guard commit (replay),
concurrent delivery (a single effect).

## 8. Non-functional requirements

Deduplication is by the inbox unique key; at-least-once with an idempotent effect; relay
batches up to 100 (kit); event processing is up to three idempotent `EnsureAccount` calls
(usually a no-op by the index) plus a posting with the 2.3 retry budget. Unit+integration
coverage is at least 80%; money scenarios only on real PostgreSQL and Kafka (testcontainers).

## 9. Test plan

| Level | Scenarios |
| :-- | :-- |
| Smoke | `go build ./...`; compose: migrations version 8, the topics `payment.events`/`ledger.events` created, the consumer started (`payment consumer started`) |
| Unit | posting template (fee=0, received=fee, payment_id canonicalization, validation rejections); `ledger.events` envelope building (types, headers, key, payload, traceparent) |
| Integration (PostgreSQL) | processing of `payment.completed` via `inbox.Processor`: accounts, postings, snapshots, the outbox envelope; a duplicate `event_id` suppressed; a repeat with another `event_id` - replay; concurrent delivery (a single effect); the fee template; skipping of `UNSPECIFIED`/an unknown enum; an invalid traceparent; validation errors; `reserve.*` and `transaction.posted` events from the gRPC paths |
| Integration (Kafka, testcontainers) | e2e `test/e2e/payment_consumer_kafka_test.go`: an event in `payment.events` -> a posting in PostgreSQL + an envelope in `ledger.events`; a duplicate creates no second posting |
| E2E (compose) | extended smoke per the run instruction `test/e2e/E2E.md` (scenario E2E_Ledger_ReserveFlow, compose stand): topics created, consumer started |
| Load | not extended |

## 10. Work plan and commit

Commit `CBL-2.7: payment.completed consumer and ledger.events publication`; the spec is
part of the commit; AGENTS (commands, trade-offs T-LED-08/09, the journal), `CBL-004` and
the notes in specs 2.2/2.4 about the transferred responsibility are updated.

## 11. Open questions and decisions

- ADR-2.7: the handler does not use the Guard transaction. Compensation: the business
  operation is idempotent by the key `payment:{id}:completed` (a repeated replay has no
  effect), the inbox record remains the second barrier; a failure between the commits is
  safe. The alternative (a posting in the passed transaction) is impossible without
  revising the 2.3 retry loop.
- ADR: the `ledger.events` partition key for multi-account postings is the lexicographically
  first account_id; the `CBL-003` contract ("key account_id") is clarified by this rule.
- ADR: the platform fee is deducted from the merchant credit (Dr clearing for the full
  received amount, Cr fee for the fee); agreed with the CBL-000 flow (step 6).
- ADR: `expected_amount` does not participate in the posting: the ledger records what was
  actually received; underpayment/overpayment are Payment states (4.5), not ledger states.
- Open question: traceparent from the gRPC context in events will appear with the OTel
  rollout (0.7); currently only a Kafka message header valid per the W3C format is
  propagated on the consumer path (invalid ones are ignored), and traceparent is absent in
  events of the gRPC paths.
- Open question: Schema Registry for `ledger.events` - task 0.5 (as in 3.5).
- Accepted with a limitation: the ledger outbox grows on every mutation and is not cleaned
  (the relay only sets `published_at`); retention and archiving are task 14.2 (KI-ledger-09).
- ADR: `payment_id` is canonicalized before building the business key: an untrusted UUID
  representation (case, braces, urn, no hyphens) must not bypass the second barrier (a
  security finding of the 2.7 review).
- ADR: `UNSPECIFIED` and unknown enums are acknowledged without effect (after a WARN log):
  the ledger does not guess completion from an incomplete event; from the pipeline's point
  of view this is not poison.

## 12. Review (CBL-001, step 6)

Four independent passes (14.10.2026). Blockers: the non-canonical `payment_id` (double
posting) and the endless redelivery of poison messages in the kit - closed (the latter by
the kit fix `CBL-1.3 (fix)` with DLQ fallbacks). The rest is closed or recorded.

| Role | Key findings | Closure |
| :-- | :-- | :-- |
| No context | `UNSPECIFIED` goes to the DLQ instead of being skipped; `payment_id` without canonicalization; documentation/tests incomplete; the NFR about a single EnsureAccount is wrong; broker Split without TrimSpace | UNSPECIFIED/an unknown enum are skipped (tests); UUID canonicalization (representation tests); the NFR corrected; splitCSV with TrimSpace; snapshot/skip tests added; the journal/AGENTS are updated by this commit |
| Architecture | payment_id is not canonicalized (the ADR-2.7 pillar); the outbox grows without retention; CBL-003 is not clarified; the consequences of the min key are not documented; a repeat with a changed body is not described | canonicalization; KI-ledger-09 (retention, 14.2); CBL-003 section 6 clarified by the key rule; T-LED-09 records hot partitions and ordering; §7 and the runbook extended with the reuse -> DLQ/reversal scenario |
| With context | the UNSPECIFIED deviation; duplicated direction/money mapping; a dead sink log; retry/DLQ was not tested with the publisher; smoke without topic checks; %v instead of %w | skipping of types; the duplication is left deliberately (thin layer mappers, extraction upon contract extension); the log is used; kit DLQ tests + Kafka e2e; the script checks topics; `%w` |
| Security | BLOCKER: a non-canonical UUID bypasses the business key (double crediting); BLOCKER: poison envelopes loop the partition (kit); KI-ledger-02 is outdated (the Kafka path); traceparent without validation; UNSPECIFIED; no concurrency test | canonicalization + representation tests; the kit fix `CBL-1.3 (fix)` (DLQ sanitization, x-original-event-id, event_type=unknown) + regression tests; KI-ledger-02 extended with the Kafka path and ACLs before prod; W3C traceparent validation + a test; skipping; a concurrent integration test |
