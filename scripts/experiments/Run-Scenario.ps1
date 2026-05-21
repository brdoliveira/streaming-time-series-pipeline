param(
  [ValidateSet("low", "medium", "high", "custom")]
  [string]$Scenario = "low",

  [int]$RatePerSecond = 0,

  [int]$DurationSeconds = 0,

  [string]$OutputRoot = "results",

  [switch]$SkipStackStart,

  [switch]$Build,

  [int]$StatsIntervalSeconds = 10,

  [int]$ProducerCount = 1,

  [ValidateSet("random", "trend", "burst")]
  [string]$ProducerType = "random"
)

$ErrorActionPreference = "Stop"

function Get-DefaultRate {
  param([string]$Name)
  switch ($Name) {
    "low" { return 10 }
    "medium" { return 100 }
    "high" { return 1000 }
    default { return 10 }
  }
}

function Get-DefaultDuration {
  param([string]$Name)
  switch ($Name) {
    "low" { return 300 }
    "medium" { return 600 }
    "high" { return 600 }
    default { return 300 }
  }
}

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
  # Docker Compose writes status lines to stderr; with 2>&1 those become NativeCommandError
  # objects that trigger $ErrorActionPreference = "Stop". Temporarily relax to Continue.
  $saved = $ErrorActionPreference
  $ErrorActionPreference = "Continue"
  & $exe @args 2>&1 | Tee-Object -FilePath $LogPath -Append
  $ErrorActionPreference = $saved
}

if ($RatePerSecond -le 0) {
  $RatePerSecond = Get-DefaultRate -Name $Scenario
}

if ($DurationSeconds -le 0) {
  $DurationSeconds = Get-DefaultDuration -Name $Scenario
}

$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$runDir = Join-Path $OutputRoot "$timestamp-$Scenario"
New-Item -ItemType Directory -Force -Path $runDir | Out-Null

$PostgresUser = if ($env:POSTGRES_USER) { $env:POSTGRES_USER } else { "pipeline" }
$PostgresDb = if ($env:POSTGRES_DB) { $env:POSTGRES_DB } else { "pipeline" }

$logPath = Join-Path $runDir "run.log"
$metadataPath = Join-Path $runDir "metadata.json"

$metadata = [ordered]@{
  scenario = $Scenario
  rate_per_second = $RatePerSecond
  duration_seconds = $DurationSeconds
  stats_interval_seconds = $StatsIntervalSeconds
  producer_count = $ProducerCount
  producer_type = $ProducerType
  postgres_user = $PostgresUser
  postgres_db = $PostgresDb
  started_at = (Get-Date).ToUniversalTime().ToString("o")
  output_dir = $runDir
}
$metadata | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $metadataPath -Encoding UTF8

if (-not $SkipStackStart) {
  $upCommand = @("docker", "compose", "--profile", "app", "up", "-d")
  if ($Build) {
    $upCommand += "--build"
  }
  $upCommand += @("kafka", "kafka-init", "timescaledb", "flink-jobmanager", "flink-taskmanager", "grafana", "flink-job")
  Invoke-LoggedCommand -LogPath $logPath -Command $upCommand
}

Invoke-LoggedCommand -LogPath $logPath -Command @(
  "docker", "compose", "exec", "-T", "timescaledb",
  "psql", "-U", $PostgresUser, "-d", $PostgresDb, "-f", "/queries/00-healthcheck.sql"
)

$producerLog = Join-Path $runDir "producer.log"
$statsSamples = Join-Path $runDir "docker-stats-samples.csv"
"timestamp_utc,container,cpu_percent,memory_usage,memory_percent,net_io,block_io" |
  Set-Content -LiteralPath $statsSamples -Encoding UTF8

$statsJob = Start-Job -ScriptBlock {
  param($Path, $IntervalSeconds)
  while ($true) {
    $timestamp = (Get-Date).ToUniversalTime().ToString("o")
    docker stats --no-stream --format "{{.Container}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}},{{.NetIO}},{{.BlockIO}}" 2>$null |
      ForEach-Object {
        "$timestamp,$_" | Add-Content -LiteralPath $Path
      }
    Start-Sleep -Seconds $IntervalSeconds
  }
} -ArgumentList $statsSamples, $StatsIntervalSeconds

$producerCommand = @(
  "docker", "compose", "--profile", "app", "up", "-d",
  "--scale", "producer=$ProducerCount",
  "producer"
)

Write-Host "Aguardando Flink job entrar em RUNNING..."
$flinkReady = $false
for ($i = 0; $i -lt 60; $i++) {
  try {
    $jobs = Invoke-RestMethod -Uri "http://localhost:8081/jobs" -TimeoutSec 3 -ErrorAction Stop
    if ($jobs.jobs | Where-Object { $_.status -eq "RUNNING" }) {
      $flinkReady = $true
      break
    }
  } catch {}
  Start-Sleep -Seconds 5
}
if (-not $flinkReady) {
  Write-Warning "Flink job nao entrou em RUNNING em 5 minutos. Prosseguindo mesmo assim."
}

try {
  $env:PRODUCER_SCENARIO = $Scenario
  $env:PRODUCER_RATE_PER_SECOND = [string]$RatePerSecond
  $env:PRODUCER_RUN_DURATION_SECONDS = [string]$DurationSeconds
  $env:PRODUCER_ID = ""
  $env:PRODUCER_TYPE = $ProducerType
  Invoke-LoggedCommand -LogPath $producerLog -Command $producerCommand
  Start-Sleep -Seconds ($DurationSeconds + 10)
  Invoke-LoggedCommand -LogPath $producerLog -Command @("docker", "compose", "--profile", "app", "stop", "producer")
}
finally {
  Remove-Item Env:\PRODUCER_SCENARIO -ErrorAction SilentlyContinue
  Remove-Item Env:\PRODUCER_RATE_PER_SECOND -ErrorAction SilentlyContinue
  Remove-Item Env:\PRODUCER_RUN_DURATION_SECONDS -ErrorAction SilentlyContinue
  Remove-Item Env:\PRODUCER_ID -ErrorAction SilentlyContinue
  Remove-Item Env:\PRODUCER_TYPE -ErrorAction SilentlyContinue
  Stop-Job -Job $statsJob -ErrorAction SilentlyContinue | Out-Null
  Receive-Job -Job $statsJob -ErrorAction SilentlyContinue | Out-Null
  Remove-Job -Job $statsJob -Force -ErrorAction SilentlyContinue | Out-Null
}

Start-Sleep -Seconds 10

$queries = @(
  "01-last-5-minutes.sql",
  "02-last-1-hour.sql",
  "03-last-24-hours.sql",
  "04-latency-by-scenario.sql",
  "05-throughput-per-second.sql",
  "06-recent-events.sql",
  "07-aggregated-metrics-recent.sql",
  "08-aggregated-throughput.sql",
  "10-cagg-last-5-minutes.sql",
  "11-cagg-last-1-hour.sql",
  "12-cagg-last-24-hours.sql"
)

foreach ($query in $queries) {
  $name = [System.IO.Path]::GetFileNameWithoutExtension($query)
  $outFile = Join-Path $runDir "$name.txt"
  $saved = $ErrorActionPreference; $ErrorActionPreference = "Continue"
  & docker compose exec -T timescaledb psql -U $PostgresUser -d $PostgresDb -f "/queries/$query" 2>&1 |
    Tee-Object -FilePath $outFile
  $ErrorActionPreference = $saved
}

$summaryCsv = Join-Path $runDir "latency-by-scenario.csv"
$saved = $ErrorActionPreference; $ErrorActionPreference = "Continue"
& docker compose exec -T timescaledb psql -U $PostgresUser -d $PostgresDb --csv -c @"
SELECT scenario,
       count(*) AS total_events,
       round(avg(ingestion_latency_ms)::numeric,2) AS avg_latency_ms,
       round(stddev(ingestion_latency_ms)::numeric,2) AS stddev_latency_ms,
       percentile_cont(0.50) WITHIN GROUP (ORDER BY ingestion_latency_ms) AS p50_latency_ms,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY ingestion_latency_ms) AS p95_latency_ms,
       max(ingestion_latency_ms) AS max_latency_ms,
       round(avg(event_lag_ms)::numeric,2) AS avg_event_lag_ms
FROM financial_events
GROUP BY scenario
ORDER BY scenario
"@ 2>$null | Set-Content -LiteralPath $summaryCsv -Encoding UTF8
$ErrorActionPreference = $saved

$throughputCsv = Join-Path $runDir "throughput-by-scenario.csv"
$saved = $ErrorActionPreference; $ErrorActionPreference = "Continue"
& docker compose exec -T timescaledb psql -U $PostgresUser -d $PostgresDb --csv -c @"
SELECT scenario,
       min(processing_time) AS first_processing_time,
       max(processing_time) AS last_processing_time,
       count(*) AS total_events,
       CASE
         WHEN extract(epoch FROM max(processing_time) - min(processing_time)) > 0
         THEN round((count(*) / extract(epoch FROM max(processing_time) - min(processing_time)))::numeric,2)
         ELSE count(*)
       END AS effective_events_per_second
FROM financial_events
GROUP BY scenario
ORDER BY scenario
"@ 2>$null | Set-Content -LiteralPath $throughputCsv -Encoding UTF8
$ErrorActionPreference = $saved

Write-Host "Refreshing continuous aggregates..."
$saved = $ErrorActionPreference; $ErrorActionPreference = "Continue"
& docker compose exec -T timescaledb psql -U $PostgresUser -d $PostgresDb `
  -c "CALL refresh_continuous_aggregate('cagg_events_1min', now() - interval '25 hours', now());" 2>$null | Out-Null
& docker compose exec -T timescaledb psql -U $PostgresUser -d $PostgresDb `
  -c "CALL refresh_continuous_aggregate('cagg_events_15min', now() - interval '26 hours', now());" 2>$null | Out-Null
$ErrorActionPreference = $saved

$queryTimingRows = @("query,window,scenario,type,elapsed_ms")
$timedQueries = @(
  @{ Name = "01-last-5-minutes";      Window = "5min"; File = "/queries/01-last-5-minutes.sql";      Type = "raw"  },
  @{ Name = "02-last-1-hour";         Window = "1h";   File = "/queries/02-last-1-hour.sql";         Type = "raw"  },
  @{ Name = "03-last-24-hours";       Window = "24h";  File = "/queries/03-last-24-hours.sql";       Type = "raw"  },
  @{ Name = "10-cagg-last-5-minutes"; Window = "5min"; File = "/queries/10-cagg-last-5-minutes.sql"; Type = "cagg" },
  @{ Name = "11-cagg-last-1-hour";    Window = "1h";   File = "/queries/11-cagg-last-1-hour.sql";    Type = "cagg" },
  @{ Name = "12-cagg-last-24-hours";  Window = "24h";  File = "/queries/12-cagg-last-24-hours.sql";  Type = "cagg" }
)
foreach ($q in $timedQueries) {
  $elapsed = (Measure-Command {
    $saved2 = $ErrorActionPreference; $ErrorActionPreference = "Continue"
    & docker compose exec -T timescaledb psql -U $PostgresUser -d $PostgresDb -f $q.File 2>$null | Out-Null
    $ErrorActionPreference = $saved2
  }).TotalMilliseconds
  $queryTimingRows += "$($q.Name),$($q.Window),$Scenario,$($q.Type),$([math]::Round($elapsed, 2))"
}
$queryTimingRows | Set-Content -LiteralPath (Join-Path $runDir "query-response-times.csv") -Encoding UTF8

$saved = $ErrorActionPreference; $ErrorActionPreference = "Continue"
docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.NetIO}}\t{{.BlockIO}}" 2>$null |
  Set-Content -LiteralPath (Join-Path $runDir "docker-stats.txt") -Encoding UTF8
$ErrorActionPreference = $saved

$metadata.completed_at = (Get-Date).ToUniversalTime().ToString("o")
$metadata | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $metadataPath -Encoding UTF8

Write-Host "Scenario '$Scenario' finished. Results saved to $runDir"
