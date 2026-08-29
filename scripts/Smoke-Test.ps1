[CmdletBinding()]
param(
    [switch]$StartStack,
    [switch]$Cleanup,
    [switch]$ValidateOnly,
    [ValidateRange(30, 1800)]
    [int]$TimeoutSeconds = 300,
    [string]$KafkaRawTopic = $(if ($env:KAFKA_TOPIC_RAW) { $env:KAFKA_TOPIC_RAW } else { "financial-events-raw" }),
    [string]$KafkaInvalidTopic = $(if ($env:KAFKA_TOPIC_ERRORS) { $env:KAFKA_TOPIC_ERRORS } else { "financial-events-invalid" }),
    [string]$DatabaseName = $(if ($env:POSTGRES_DB) { $env:POSTGRES_DB } else { "pipeline" }),
    [string]$DatabaseUser = $(if ($env:POSTGRES_USER) { $env:POSTGRES_USER } else { "pipeline" })
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
$stackStartedByScript = $false

function Invoke-DockerChecked {
    param([Parameter(Mandatory)][string[]]$Arguments)

    # Windows PowerShell 5.1 transforma qualquer linha nativa em stderr em
    # NativeCommandError quando ErrorActionPreference=Stop. O Compose usa
    # stderr para progresso normal de build, portanto o exit code e a unica
    # fonte confiavel para decidir sucesso ou falha.
    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $output = & docker @Arguments 2>&1
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }

    if ($exitCode -ne 0) {
        throw "docker $($Arguments -join ' ') falhou (exit $exitCode):`n$($output | Out-String)"
    }
    return ($output | Out-String).Trim()
}

function Wait-ForCondition {
    param(
        [Parameter(Mandatory)][string]$Description,
        [Parameter(Mandatory)][scriptblock]$Condition
    )

    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    $lastFailure = $null
    while ([DateTimeOffset]::UtcNow -lt $deadline) {
        try {
            if (& $Condition) {
                Write-Host "OK: $Description"
                return
            }
        }
        catch {
            $lastFailure = $_.Exception.Message
        }
        Start-Sleep -Seconds 2
    }

    $suffix = if ($lastFailure) { " Ultima falha: $lastFailure" } else { "" }
    throw "Timeout aguardando: $Description.$suffix"
}

function Get-DatabaseCount {
    param([Parameter(Mandatory)][string]$Sql)

    $value = Invoke-DockerChecked -Arguments @(
        "compose", "exec", "-T", "timescaledb",
        "psql", "-U", $DatabaseUser, "-d", $DatabaseName,
        "-t", "-A", "-v", "ON_ERROR_STOP=1", "-c", $Sql
    )
    return [int64]$value.Trim()
}

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker CLI nao encontrado. Instale e inicie o Docker Desktop antes do smoke test."
}

Push-Location $projectRoot
try {
    [void](Invoke-DockerChecked -Arguments @("compose", "version"))
    [void](Invoke-DockerChecked -Arguments @("compose", "config", "--quiet"))

    if ($ValidateOnly) {
        Write-Host "Configuracao do Compose e pre-requisitos do smoke test validados."
        return
    }

    if ($StartStack) {
        Write-Host "Construindo e iniciando a stack minima do smoke test..."
        $stackStartedByScript = $true
        [void](Invoke-DockerChecked -Arguments @(
            "compose", "--profile", "app", "up", "--detach", "--build",
            "kafka", "kafka-init", "timescaledb", "flink-jobmanager", "flink-taskmanager", "flink-job"
        ))
    }

    Wait-ForCondition -Description "TimescaleDB aceitar consultas" -Condition {
        try {
            [void](Invoke-DockerChecked -Arguments @(
                "compose", "exec", "-T", "timescaledb",
                "pg_isready", "-U", $DatabaseUser, "-d", $DatabaseName
            ))
            return $true
        }
        catch {
            return $false
        }
    }

    Wait-ForCondition -Description "job Flink entrar em RUNNING" -Condition {
        $raw = Invoke-DockerChecked -Arguments @(
            "compose", "exec", "-T", "flink-jobmanager",
            "wget", "-q", "-O", "-", "http://localhost:8081/jobs/overview"
        )
        $overview = $raw | ConvertFrom-Json
        return @($overview.jobs | Where-Object { $_.state -eq "RUNNING" }).Count -ge 1
    }

    $runId = [Guid]::NewGuid().ToString("N")
    $validEventId = [Guid]::NewGuid().ToString()
    $invalidEventId = [Guid]::NewGuid().ToString()
    $timestamp = [DateTimeOffset]::UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ")

    $validEvent = [ordered]@{
        event_id = $validEventId
        producer_id = "smoke-$runId"
        symbol = "SMOKE"
        price = 100.25
        quantity = 10
        event_time = $timestamp
        producer_time = $timestamp
        source = "synthetic"
        scenario = "smoke-$runId"
        sequence = 0
    } | ConvertTo-Json -Compress

    $invalidEvent = [ordered]@{
        event_id = $invalidEventId
        producer_id = "smoke-$runId"
        symbol = "INVALID SYMBOL"
        price = 100.25
        quantity = 10
        event_time = $timestamp
        producer_time = $timestamp
        source = "synthetic"
        scenario = "smoke-$runId"
        sequence = 1
    } | ConvertTo-Json -Compress

    $payloadBytes = [Text.Encoding]::UTF8.GetBytes("$validEvent`n$invalidEvent`n")
    $payloadBase64 = [Convert]::ToBase64String($payloadBytes)
    $publishCommand = "printf '%s' '$payloadBase64' | base64 -d | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic '$KafkaRawTopic'"

    Write-Host "Publicando um evento valido e um invalido (run=$runId)..."
    [void](Invoke-DockerChecked -Arguments @("compose", "exec", "-T", "kafka", "bash", "-lc", $publishCommand))

    Wait-ForCondition -Description "persistir exatamente o evento valido" -Condition {
        (Get-DatabaseCount -Sql "SELECT count(*) FROM financial_events WHERE event_id = '$validEventId'::uuid;") -eq 1
    }

    $validCount = Get-DatabaseCount -Sql "SELECT count(*) FROM financial_events WHERE event_id = '$validEventId'::uuid;"
    $invalidPersistedCount = Get-DatabaseCount -Sql "SELECT count(*) FROM financial_events WHERE event_id = '$invalidEventId'::uuid;"
    if ($validCount -ne 1 -or $invalidPersistedCount -ne 0) {
        throw "Contagens divergentes no TimescaleDB: validos=$validCount (esperado 1), invalidos_persistidos=$invalidPersistedCount (esperado 0)."
    }

    Wait-ForCondition -Description "encontrar o evento rejeitado no topico de invalidos" -Condition {
        $consumeCommand = "for partition in `$('/opt/kafka/bin/kafka-get-offsets.sh' --bootstrap-server kafka:9092 --topic '$KafkaInvalidTopic' | cut -d: -f2); do /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic '$KafkaInvalidTopic' --partition `$partition --offset earliest --timeout-ms 5000 2>/dev/null; done | grep -F -m 1 '$invalidEventId'"
        $output = & docker compose exec -T kafka bash -lc $consumeCommand 2>&1
        return $LASTEXITCODE -eq 0 -and (($output | Out-String) -match [Regex]::Escape($invalidEventId))
    }

    Write-Host "SMOKE PASS: 1 evento persistido, 1 evento rejeitado e nenhuma persistencia indevida."
}
finally {
    if ($stackStartedByScript -and $Cleanup) {
        Write-Host "Removendo a stack e os volumes descartaveis criados para o smoke test..."
        try {
            [void](Invoke-DockerChecked -Arguments @("compose", "--profile", "app", "down", "--volumes", "--remove-orphans"))
        }
        catch {
            Write-Warning "Falha ao limpar a stack do smoke test: $($_.Exception.Message)"
        }
    }
    Pop-Location
}
