# E2E CBL-2.4: compose-стенд ledger поднимается, миграции (до версии 7) применяются,
# сквозной сценарий Reserve -> Commit / Reserve -> Release проходит по реальному gRPC.
# Запуск: powershell -NoProfile -File test/e2e/reserve_flow.ps1
$ErrorActionPreference = "Stop"

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$ComposeFile = Join-Path $RepoRoot "deploy\docker-compose.yml"

Push-Location $RepoRoot
try {
    go build ./...
    if ($LASTEXITCODE -ne 0) { throw "go build failed" }

    # Сценарию нужны только PostgreSQL и сервис (Kafka стартует по depends_on).
    docker compose -f $ComposeFile --profile app up -d --build postgres ledger
    if ($LASTEXITCODE -ne 0) { throw "docker compose up failed" }

    $ready = $false
    foreach ($attempt in 1..45) {
        try {
            $response = Invoke-WebRequest -UseBasicParsing -TimeoutSec 2 http://localhost:8040/healthz
            if ($response.StatusCode -eq 200) { $ready = $true; break }
        } catch {
            Start-Sleep -Seconds 2
        }
    }
    if (-not $ready) { throw "ledger healthz is not ready" }

    # Логи читаются через cmd: PowerShell при Stop-политике превращает stderr
    # нативных команд (zap и goose пишут в stderr) в исключения NativeCommandError.
    # Идентификатор контейнера берется у compose: имя проекта не хардкодится.
    $containerId = docker compose -f $ComposeFile --profile app ps -q ledger
    if (-not $containerId) { throw "ledger container not found" }
    $logs = cmd /c "docker logs $containerId 2>&1"
    if ($LASTEXITCODE -ne 0) { throw "docker logs failed" }
    $logsText = ($logs | Out-String)
    if ($logsText -notmatch "successfully migrated database to version: 7") { throw "migrations to version 7 did not apply" }
    if ($logsText -match "apply migrations failed") { throw "migrations failed" }

    go test -tags e2e -count=1 -v ./test/e2e/
    if ($LASTEXITCODE -ne 0) { throw "e2e scenario failed" }

    Write-Host "OK: healthz ok, migrations applied (version 7), reserve flow green"
} finally {
    docker compose -f $ComposeFile --profile app down -v | Out-Null
    Pop-Location
}