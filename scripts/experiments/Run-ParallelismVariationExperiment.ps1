param(
  [ValidateSet(1, 2, 3, 4, 6, 8)]
  [int]$FlinkParallelism = 3,

  [string]$OutputRoot = "results-parallelism-variation",

  [switch]$Build,

  [int]$StatsIntervalSeconds = 5,

  [int]$RatePerSecond = 1000,

  [int]$DurationSeconds = 300
)

$ErrorActionPreference = "Stop"

function Invoke-LoggedCommand {
  param(
    [string]$LogPath,
    [string[]]$Command
  )

  $line = "> " + ($Command -join " ")
  Add-Content -LiteralPath $LogPath -Value $line
  $exe = $Command[0]
  $args = @()
  if ($Command.Length -gt 1) {
    $args = $Command[1..($Command.Length - 1)]
  }
  $saved = $ErrorActionPreference
  $ErrorActionPreference = "Continue"
  & $exe @args 2>&1 | Tee-Object -FilePath $LogPath -Append
  $exitCode = $LASTEXITCODE
  $ErrorActionPreference = $saved
  if ($exitCode -ne 0) {
    throw "Comando falhou com código ${exitCode}: $line"
  }
}

# Validar parâmetros
if ($FlinkParallelism -le 0) {
  throw "FlinkParallelism deve ser positivo"
}

$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$runDir = Join-Path $OutputRoot "$timestamp-parallelism$FlinkParallelism"
New-Item -ItemType Directory -Force -Path $runDir | Out-Null

$PostgresUser = if ($env:POSTGRES_USER) { $env:POSTGRES_USER } else { "pipeline" }
$PostgresDb = if ($env:POSTGRES_DB) { $env:POSTGRES_DB } else { "pipeline" }

$logPath = Join-Path $runDir "run.log"
$metadataPath = Join-Path $runDir "metadata.json"

# Metadados do experimento
$metadata = [ordered]@{
  experiment_type = "parallelism_variation"
  flink_parallelism = $FlinkParallelism
  jdbc_batch_size = 1000
  jdbc_batch_interval_ms = 500
  rate_per_second = $RatePerSecond
  duration_seconds = $DurationSeconds
  stats_interval_seconds = $StatsIntervalSeconds
  postgres_user = $PostgresUser
  postgres_db = $PostgresDb
  flink_task_slots = $FlinkParallelism
  state_reset = $true
  expected_events = ($RatePerSecond * $DurationSeconds)
  started_at = (Get-Date).ToUniversalTime().ToString("o")
  output_dir = $runDir
  resource_collection_status = "pending"
}
$metadata | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $metadataPath -Encoding UTF8

Write-Host "Removendo stack e volumes da execução anterior..."
Invoke-LoggedCommand -LogPath $logPath -Command @(
  "docker", "compose", "--profile", "app", "down", "-v", "--remove-orphans"
)

Write-Host "Iniciando Docker Compose com FLINK_PARALLELISM=$FlinkParallelism e FLINK_TASK_SLOTS=$FlinkParallelism"

$upCommand = @("docker", "compose", "--profile", "app", "up", "-d")
if ($Build) {
  $upCommand += "--build"
}
$upCommand += @("kafka", "kafka-init", "timescaledb", "flink-jobmanager", "flink-taskmanager", "grafana", "flink-job")

$env:FLINK_PARALLELISM = [string]$FlinkParallelism
$env:FLINK_TASK_SLOTS = [string]$FlinkParallelism
$env:JDBC_BATCH_SIZE = "1000"
$env:JDBC_BATCH_INTERVAL_MS = "500"

try {
  Invoke-LoggedCommand -LogPath $logPath -Command $upCommand
}
finally {
  Remove-Item Env:\FLINK_PARALLELISM -ErrorAction SilentlyContinue
  Remove-Item Env:\FLINK_TASK_SLOTS -ErrorAction SilentlyContinue
  Remove-Item Env:\JDBC_BATCH_SIZE -ErrorAction SilentlyContinue
  Remove-Item Env:\JDBC_BATCH_INTERVAL_MS -ErrorAction SilentlyContinue
}

# Health check
Write-Host "Executando health check no TimescaleDB..."
Invoke-LoggedCommand -LogPath $logPath -Command @(
  "docker", "compose", "exec", "-T", "timescaledb",
  "psql", "-U", $PostgresUser, "-d", $PostgresDb, "-f", "/queries/00-healthcheck.sql"
)

# Iniciar coleta de docker stats
$statsSamples = [IO.Path]::GetFullPath((Join-Path $runDir "docker-stats-samples.csv"))
$startCollectorScript = Join-Path $PSScriptRoot "Start-DockerStatsCollector.ps1"
$stopCollectorScript = Join-Path $PSScriptRoot "Stop-DockerStatsCollector.ps1"
$statsCollector = $null

# Iniciar coleta de métricas Flink
$flinkMetricsPath = [IO.Path]::GetFullPath((Join-Path $runDir "flink-metrics-samples.csv"))

try {
  $statsCollector = & $startCollectorScript `
    -OutputPath $statsSamples `
    -IntervalSeconds $StatsIntervalSeconds `
    -FlinkMetricsOutputPath $flinkMetricsPath
  $metadata["resource_collection_status"] = "running"
  $metadata["resource_collector_pid"] = $statsCollector.process_id
  $metadata["resource_collector_log"] = $statsCollector.collector_log_path
  $metadata | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $metadataPath -Encoding UTF8
  Add-Content -LiteralPath $logPath -Value "Coletor de recursos iniciado: PID=$($statsCollector.process_id)"
}
catch {
  $metadata["resource_collection_status"] = "start_failed"
  $metadata["resource_collection_error"] = $_.Exception.Message
  $metadata | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $metadataPath -Encoding UTF8
  Add-Content -LiteralPath $logPath -Value "ERRO na coleta de recursos: $($_.Exception.Message)"
  throw
}

# Aguardar Flink estar pronto
Write-Host "Aguardando Flink job entrar em RUNNING..."
$flinkReady = $false
for ($i = 0; $i -lt 60; $i++) {
  try {
    $jobs = Invoke-RestMethod -Uri "http://localhost:8081/jobs" -TimeoutSec 3 -ErrorAction Stop
    if ($jobs.jobs | Where-Object { $_.status -eq "RUNNING" }) {
      $flinkReady = $true
      Write-Host "Flink job RUNNING."
      break
    }
  } catch {}
  Start-Sleep -Seconds 5
}

if (-not $flinkReady) {
  Write-Warning "Flink job nao entrou em RUNNING em 5 minutos. Prosseguindo mesmo assim."
}

# Executar produtor
$producerLog = Join-Path $runDir "producer.log"

try {
  $producerCommand = @(
    "docker", "compose", "--profile", "app", "up", "-d",
    "--scale", "producer=1",
    "producer"
  )

  $env:PRODUCER_SCENARIO = "high"
  $env:PRODUCER_RATE_PER_SECOND = [string]$RatePerSecond
  $env:PRODUCER_RUN_DURATION_SECONDS = [string]$DurationSeconds
  $env:PRODUCER_ID = ""
  $env:PRODUCER_TYPE = "random"

  Write-Host "Iniciando produtor: $RatePerSecond eventos/s por $DurationSeconds segundos"
  Invoke-LoggedCommand -LogPath $producerLog -Command $producerCommand

  Start-Sleep -Seconds ($DurationSeconds + 20)

  Write-Host "Parando produtor..."
  Invoke-LoggedCommand -LogPath $producerLog -Command @("docker", "compose", "--profile", "app", "stop", "producer")
}
finally {
  Remove-Item Env:\PRODUCER_SCENARIO -ErrorAction SilentlyContinue
  Remove-Item Env:\PRODUCER_RATE_PER_SECOND -ErrorAction SilentlyContinue
  Remove-Item Env:\PRODUCER_RUN_DURATION_SECONDS -ErrorAction SilentlyContinue
  Remove-Item Env:\PRODUCER_ID -ErrorAction SilentlyContinue
  Remove-Item Env:\PRODUCER_TYPE -ErrorAction SilentlyContinue

  Write-Host "Parando coleta de metricas..."
  if ($statsCollector) {
    try {
      $collectorResult = & $stopCollectorScript `
        -ProcessId $statsCollector.process_id `
        -OutputPath $statsCollector.output_path `
        -StopSignalPath $statsCollector.stop_signal_path `
        -CollectorLogPath $statsCollector.collector_log_path `
        -FlinkMetricsOutputPath $statsCollector.flink_metrics_output_path
      $metadata["resource_collection_status"] = $collectorResult.status
      $metadata["resource_collection_exit_code"] = $collectorResult.exit_code
      $metadata["resource_sample_count"] = $collectorResult.docker_sample_count
      $metadata["flink_metric_sample_count"] = $collectorResult.flink_sample_count
      Add-Content -LiteralPath $logPath -Value "Coleta de recursos concluida: docker_samples=$($collectorResult.docker_sample_count); flink_samples=$($collectorResult.flink_sample_count)"
    }
    catch {
      $metadata["resource_collection_status"] = "failed"
      $metadata["resource_collection_error"] = $_.Exception.Message
      Add-Content -LiteralPath $logPath -Value "ERRO na coleta de recursos: $($_.Exception.Message)"
      $metadata | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $metadataPath -Encoding UTF8
      throw "Falha observavel na coleta de recursos. Consulte $logPath. $($_.Exception.Message)"
    }
    $metadata | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $metadataPath -Encoding UTF8
  }
}

$expectedEvents = $RatePerSecond * $DurationSeconds
$observedEvents = 0
$processingDeadline = (Get-Date).AddSeconds(120)

Write-Host "Aguardando persistência de $expectedEvents eventos..."
do {
  $countOutput = & docker compose exec -T timescaledb psql `
    -U $PostgresUser -d $PostgresDb -Atc `
    "SELECT count(*) FROM financial_events WHERE scenario = 'high';" 2>$null

  $countValue = $countOutput | Select-Object -Last 1
  if ($countValue -match "^\d+$") {
    $observedEvents = [long]$countValue
  }

  if ($observedEvents -ge $expectedEvents) {
    break
  }

  Start-Sleep -Seconds 5
} while ((Get-Date) -lt $processingDeadline)

$countTolerance = [math]::Max(1, [math]::Ceiling($expectedEvents * 0.01))
$countIsValid = [math]::Abs($observedEvents - $expectedEvents) -le $countTolerance

$metadata["observed_events"] = $observedEvents
$metadata["count_tolerance"] = $countTolerance
$metadata["valid_event_count"] = $countIsValid

if (-not $countIsValid) {
  Write-Warning "Contagem inválida: esperado=$expectedEvents, observado=$observedEvents, tolerância=$countTolerance"
}

# Coletar resultados
Write-Host "Coletando metricas de TimescaleDB..."

$latencyBucketsCsv = Join-Path $runDir "latency-by-bucket.csv"
$saved = $ErrorActionPreference; $ErrorActionPreference = "Continue"
& docker compose exec -T timescaledb psql -U $PostgresUser -d $PostgresDb --csv -c @"
SELECT
  date_trunc('10 seconds', processing_time) as bucket,
  count(*) as event_count,
  round(avg(ingestion_latency_ms)::numeric, 2) as avg_latency_ms,
  percentile_cont(0.50) WITHIN GROUP (ORDER BY ingestion_latency_ms) as p50_latency_ms,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY ingestion_latency_ms) as p95_latency_ms,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY ingestion_latency_ms) as p99_latency_ms,
  max(ingestion_latency_ms) as max_latency_ms,
  round(stddev(ingestion_latency_ms)::numeric, 2) as stddev_latency_ms
FROM financial_events
WHERE scenario = 'high'
GROUP BY bucket
ORDER BY bucket
"@ 2>$null | Set-Content -LiteralPath $latencyBucketsCsv -Encoding UTF8
$ErrorActionPreference = $saved

$latencySummaryCsv = Join-Path $runDir "latency-summary.csv"
$saved = $ErrorActionPreference; $ErrorActionPreference = "Continue"
& docker compose exec -T timescaledb psql -U $PostgresUser -d $PostgresDb --csv -c @"
SELECT
  scenario,
  count(*) AS total_events,
  round(avg(ingestion_latency_ms)::numeric, 2) AS avg_latency_ms,
  percentile_cont(0.50) WITHIN GROUP (ORDER BY ingestion_latency_ms) AS p50_latency_ms,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY ingestion_latency_ms) AS p95_latency_ms,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY ingestion_latency_ms) AS p99_latency_ms,
  max(ingestion_latency_ms) AS max_latency_ms,
  round(stddev(ingestion_latency_ms)::numeric, 2) AS stddev_latency_ms
FROM financial_events
WHERE scenario = 'high'
GROUP BY scenario
"@ 2>$null | Set-Content -LiteralPath $latencySummaryCsv -Encoding UTF8
$ErrorActionPreference = $saved

$throughputCsv = Join-Path $runDir "throughput.csv"
$saved = $ErrorActionPreference; $ErrorActionPreference = "Continue"
& docker compose exec -T timescaledb psql -U $PostgresUser -d $PostgresDb --csv -c @"
SELECT
  scenario,
  min(processing_time) AS first_event_time,
  max(processing_time) AS last_event_time,
  count(*) AS total_events,
  CASE
    WHEN extract(epoch FROM max(processing_time) - min(processing_time)) > 0
    THEN round((count(*) / extract(epoch FROM max(processing_time) - min(processing_time)))::numeric, 2)
    ELSE count(*)
  END AS effective_events_per_second
FROM financial_events
WHERE scenario = 'high'
GROUP BY scenario
"@ 2>$null | Set-Content -LiteralPath $throughputCsv -Encoding UTF8
$ErrorActionPreference = $saved

$dockerStatsFinal = Join-Path $runDir "docker-stats-final.txt"
$saved = $ErrorActionPreference; $ErrorActionPreference = "Continue"
docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.NetIO}}\t{{.BlockIO}}" 2>$null |
  Set-Content -LiteralPath $dockerStatsFinal -Encoding UTF8
$ErrorActionPreference = $saved

# Atualizar metadados
$metadata["completed_at"] = (Get-Date).ToUniversalTime().ToString("o")
$metadata | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $metadataPath -Encoding UTF8

if (-not $countIsValid) {
  throw "Experimento concluído com contagem de eventos inválida. Consulte $metadataPath"
}

Write-Host "Experimento parallelism variation completo. Resultados salvos em: $runDir"
Write-Host ""
Write-Host "Arquivos gerados:"
Write-Host "  - $latencyBucketsCsv"
Write-Host "  - $latencySummaryCsv"
Write-Host "  - $throughputCsv"
Write-Host "  - $statsSamples"
Write-Host "  - $flinkMetricsPath"
