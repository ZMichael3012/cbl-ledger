# summary.md run report template

Used for service local e2e runs (`test/e2e/E2E.md`). Copy into
`test/e2e/.runs/<UTC mark>/summary.md` at the start of the run and fill it in as the run
progresses.

```markdown
# E2E run: cbl-ledger, <UTC run start mark>

- Stand: `deploy/docker-compose.yml`, project `cbl-ledger-e2e-<mark>`
- Code version: `<commit hash>`, branch `<branch>`
- Start: <UTC RFC 3339>, End: <UTC RFC 3339>
- Verdict: PASS | FAIL

## Steps

| # | Step | Command (short) | Status | Artifact |
| :-- | :-- | :-- | :-- | :-- |
| 1 | prepare | go build / go vet | OK | prepare.log |
| 2 | down -v | docker compose ... | OK | compose-down.log |
| 3 | up --build | docker compose ... | OK | compose-up.log, compose-ps.txt |
| 4 | stand checks | healthz, readyz, migrations v9, topics, consumer | OK | ledger-logs.txt, topics.txt, reserve_flow-checks.txt |
| 5 | scenario | go test -tags e2e ./test/e2e/ | OK | reserve_flow.log |
| 6 | down -v | docker compose ... | OK | compose-down.log |

## Key checks

| Scenario | Check | Expected | Actual | Status |
| :-- | :-- | :-- | :-- | :-- |

## Emergency dumps

<list on FAIL: compose ps, ledger logs, Kafka log tail, last test output>

## Notes

<deviations, retries, known flakes>
```
