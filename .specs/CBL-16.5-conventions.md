# CBL-16.5. cbl-ledger: English comments and documentation; e2e via E2E.md

| Field | Value |
| :-- | :-- |
| Epic | E16, `Convention alignments` |
| Repository | `cbl-ledger` |
| Size | M |
| Wave and parallel group | W0-CORR |
| Depends on | `CBL-16.1` (convention), `CBL-16.2` (pilot pattern) |
| Contract version | not applicable (no contract changes) |
| Status | done, review closed |

## 1. Context and goal

Two cross-cutting customer corrections (16.10.2026): (1) comments and documentation are
written in English in all files (code, tests, migrations, specs, AGENTS.md, README,
configurations; log and error messages stay unchanged as executable contracts);
(2) e2e runs are not PowerShell scripts but an agent instruction `test/e2e/E2E.md` with
run artifacts in `test/e2e/.runs/`. The convention is `CBL-002` section 7.

## 2. Scope

In scope: English translation of all comments, docstrings, migration SQL comments,
Dockerfile comments, the k6 load-script header comments, all ten specs (CBL-2.1 - CBL-2.10),
AGENTS.md and README.md; `test/e2e/E2E.md` (scenario `E2E_Ledger_ReserveFlow` covering the
whole former script behavior: build, stand, migrations v9, topics, consumer, Go e2e tests);
removal of `reserve_flow.ps1` and the `make smoke` target; Makefile/.gitignore updates.

Out of scope: changing behavior and runtime messages (gRPC status messages built by
`grpcmw.E`, zap log fragments stay as executable contracts), test fixture inputs
(e.g. the non-ASCII merchant ref in `account_plan_test.go` proves validation rejects
non-ASCII), other repositories (16.6 - 16.16).

## 3. Requirements

FR-1. Language: no Russian comments or docstrings remain; Russian string literals remain
only as runtime messages (gRPC status texts in `internal/transport/grpcserver/ledger.go`)
and as test fixtures; test failure descriptions are English; godoc docstrings keep the
entity-name first line and end with a period; AGENTS.md development rules state the
English documentation rule.

FR-2. E2E: `test/e2e/E2E.md` contains preconditions, isolation (compose project per
lowercase UTC mark, fixed host ports), the run order, the stand checks (healthz/readyz,
migrations to version 9, the six topics, the payment consumer start), the Go e2e test run
(`go test -tags e2e -count=1 -v ./test/e2e/`), mandatory artifacts and cleanup; there are
no PowerShell run scripts; remainng `make test-e2e` runs the Go tests against a started
stand and points to the instruction.

FR-3. Auxiliary files live in `test/e2e/.additionals/` (summary template, inventory);
`.runs/` is gitignored.

FR-4. Validation run: the scenario executed per `E2E.md`, artifacts in
`test/e2e/.runs/<mark>/`, `summary.md` filled with a PASS verdict.

## 4. Plan

Comment and documentation translation (parallel by file groups: code, storage layer,
storage tests, transport/migrations/e2e/load, specs, AGENTS+README) -> E2E.md and
`.additionals` -> removal of the ps1 script and the smoke target -> validation run ->
4 reviews -> commit `CBL-16.5: English comments and docs; e2e via E2E.md`.

## 5. Reviews and runs

Verified: unit+integration tests green (no production code changes), lint 0, the e2e
scenario run per `E2E.md` with run artifacts, em-dash check clean, comment language
verified by a Cyrillic scan (remaining Cyrillic: gRPC status message literals in
`internal/transport/grpcserver/ledger.go` and the non-ASCII merchant-ref fixture input in
`internal/domain/account_plan_test.go`; none are comments).
