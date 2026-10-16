# E2E auxiliary files (cbl-ledger)

Purpose: support runs per `test/e2e/E2E.md` and analysis of their artifacts.
There are no PowerShell run scripts here (convention `CBL-002` section 7).

| File/directory | Purpose |
| :-- | :-- |
| `summary.template.md` | The `summary.md` run report template |

The scenario itself uses only the Go tests in `test/e2e/` (`reserve_flow_test.go`,
`payment_consumer_kafka_test.go`) and standard tools (docker, curl, go); no extra helper
binaries are needed. New auxiliary files are added here and recorded in this table.
