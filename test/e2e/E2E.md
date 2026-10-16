# E2E cbl-ledger: reserve flow, balance queries and Kafka consumer

Run instruction for the local e2e scenario of the service. A run is performed by an
agent platform-independently: docker, a command shell and the Go toolchain. There are
no PowerShell run scripts (customer decision of 16.10.2026, `CBL-002` section 7).

Repository: `cbl-ledger`. Stand: `deploy/docker-compose.yml` (postgres, kafka,
kafka-init, schema-registry, ledger in the `app` profile).

## 1. Scenarios

| Scenario | Checks | Contracts |
| :-- | :-- | :-- |
| `E2E_Ledger_ReserveFlow` | compose stand up, goose migrations to version 9, topic creation, payment consumer start, then Go e2e tests: Reserve -> Commit / Reserve -> Release and funding via real gRPC + PostgreSQL (`TestReserveCommitAndReleaseFlow`, `TestBalanceAndEntriesFlow`), payment.completed Kafka flow with the ledger.events envelope and duplicate protection (`TestPaymentConsumerKafkaFlow`, self-contained on testcontainers) | gRPC LedgerService (CF-1), Kafka `payment.events`/`ledger.events` (CF-2/CF-3), CBL-2.4/2.5/2.7/2.10 |

## 2. Isolation and parallelism

- Run mark: UTC lowercase `yyyymmddthhmmssz` (uppercase is rejected by compose project
  naming rules).
- Artifact directory: `test/e2e/.runs/<mark>/` (create before the start).
- Compose project: `cbl-ledger-e2e-<mark>` (the compose file declares `name: cbl-ledger`;
  the `-p` flag overrides it).
- The stand publishes host ports 5432, 9092, 8081, 8040, 9040, 9240; do not run two
  ledger stands in parallel and stop other ledger stands before a run.
- Only postgres, kafka, kafka-init and ledger are started (targeted `up`); kafka and
  kafka-init come up through `depends_on`. schema-registry is NOT part of the run.
- All commands run from the repository root.

## 3. Common run order

1. `<MARK>` = current UTC time in lowercase; create `test/e2e/.runs/<MARK>/`.
2. Prepare the local toolchain: `go build ./...` and
   `go vet -tags "integration e2e" ./...` (output to `prepare.log`).
3. Stop the previous stand: `docker compose -p cbl-ledger-e2e-<MARK> -f deploy/docker-compose.yml --profile app down -v`
   (output to `compose-down.log`).
4. Start the stand: `docker compose -p cbl-ledger-e2e-<MARK> -f deploy/docker-compose.yml --profile app up -d --build postgres ledger`
   (`compose-up.log`); `docker compose ps` into `compose-ps.txt`.
5. Wait for `GET http://localhost:8040/healthz` = 200 and
   `GET http://localhost:8040/readyz` = 200 (curl.exe, up to 45 attempts with a 2 s
   interval; goose and zap write to stderr, so redirect native command output to files
   instead of piping).
6. Stand checks (records into `reserve_flow-checks.txt`):
   - migrations: the ledger container log contains
     `successfully migrated database to version: 9` and does not contain
     `apply migrations failed`; capture the log via
     `docker compose -p ... logs --no-color ledger > ledger-logs.txt`;
   - consumer: the same log contains `payment consumer started`;
   - topics: `docker compose -p ... exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list`
     contains `payment.events`, `payment.events.retry.5s`, `payment.events.retry.1m`,
     `payment.events.retry.10m`, `payment.events.dlq`, `ledger.events` (save to
     `topics.txt`).
7. Scenario: `go test -tags e2e -count=1 -v ./test/e2e/` (output to
   `reserve_flow.log`); all three test functions must pass. The tests connect to the
   stand with the default environment (`LEDGER_PG_DSN` =
   `postgres://cbl:cbl@localhost:5432/ledger?sslmode=disable`, `LEDGER_GRPC_ADDR` =
   `localhost:9040`); `TestPaymentConsumerKafkaFlow` starts its own testcontainers
   PostgreSQL and Kafka and does not use the stand.
8. Capture logs: `docker compose -p ... logs --no-color > compose-logs.txt`.
9. `summary.md` by the `.additionals/summary.template.md` template; stop the stand:
   `down -v` (append to `compose-down.log`).

Utility notes: use `curl.exe -s -o <file> -w "%{http_code}"` for HTTP status checks;
PowerShell 5.1 turns stderr of native commands into NativeCommandError, so prefer cmd
redirection to log files.

## 4. E2E_Ledger_ReserveFlow

Execute the common run order; the scenario adds no extra steps: the Go e2e tests cover
the end-to-end behavior (hold/capture/release with idempotent replays, balance and
entries listing with cursor pagination and balances_after snapshots, merchant-scoped
statements and ownership fail-closed replies, and the payment.completed to ledger.events
flow with duplicate protection).

Expected key facts checked by the tests:

- Reserve moves available -> reserved without postings; Commit posts a balanced
  Dr merchant / Cr payout pair and clears the hold; Release returns funds and a repeated
  Commit of a released reserve fails;
- a repeated PostTransaction with the same idempotency key returns the stored
  `balances_after` snapshot;
- ListEntries paginates with a cursor and returns funding and payments in entry id order;
- a foreign `merchant_ref` combined with another merchant's `account_id` replies NotFound.

## 5. Mandatory run artifacts

- `summary.md` (template `.additionals/summary.template.md`; PASS/FAIL verdict, steps,
  key values, artifact list, time);
- `prepare.log`, `compose-ps.txt`, `compose-up.log`, `compose-down.log`,
  `compose-logs.txt`, `ledger-logs.txt`, `topics.txt`;
- `reserve_flow.log` (go test -v output) and `reserve_flow-checks.txt`;
- on failure: container states, ledger log fragments around the failure, the last test
  output, Kafka container log tail.

## 6. Auxiliary files (.additionals)

- `summary.template.md` - the `summary.md` template;
- `inventory.md` - the list of auxiliary files and their purpose.

## 7. Cleanup

`docker compose -p cbl-ledger-e2e-<MARK> -f deploy/docker-compose.yml --profile app down -v`
at the end; `.runs/` is not committed; a repeated run must start from a clean stand.
