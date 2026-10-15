# E2E CBL-2.4 / CBL-2.5 / CBL-2.10: compose-стенд ledger поднимается, миграции (до версии 9)
# применяются, сквозные сценарии Reserve -> Commit / Reserve -> Release и
# PostTransaction -> GetBalance -> ListEntries проходят по реальному gRPC.
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
    if ($logsText -notmatch "successfully migrated database to version: 9") { throw "migrations to version 9 did not apply" }
    if ($logsText -match "apply migrations failed") { throw "migrations failed" }
    if ($logsText -notmatch "payment consumer started") { throw "payment consumer did not start" }

    # Топики создает kafka-init; проверяем каталог топиков стенда.
    $topics = docker compose -f $ComposeFile --profile app exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
    if ($LASTEXITCODE -ne 0) { throw "kafka topics list failed" }
    $topicsText = ($topics | Out-String)
    if ($topicsText -notmatch "payment.events") { throw "payment.events topic is missing" }
    if ($topicsText -notmatch "ledger.events") { throw "ledger.events topic is missing" }

    go test -tags e2e -count=1 -v ./test/e2e/
    if ($LASTEXITCODE -ne 0) { throw "e2e scenario failed" }

    Write-Host "OK: healthz ok, migrations applied (version 9), topics and payment consumer up, e2e green"
} finally {
    docker compose -f $ComposeFile --profile app down -v | Out-Null
    Pop-Location
}