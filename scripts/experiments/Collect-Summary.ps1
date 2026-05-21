param(
  [string]$OutputRoot = "results"
)

$ErrorActionPreference = "Stop"

$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$summaryDir = Join-Path $OutputRoot "summary-$timestamp"
New-Item -ItemType Directory -Force -Path $summaryDir | Out-Null

$PostgresUser = if ($env:POSTGRES_USER) { $env:POSTGRES_USER } else { "pipeline" }
$PostgresDb = if ($env:POSTGRES_DB) { $env:POSTGRES_DB } else { "pipeline" }

# Use psql --csv with -c flag (avoids PowerShell UTF-16 pipe encoding issues with \copy)
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
"@ 2>$null | Set-Content -LiteralPath (Join-Path $summaryDir "latency-by-scenario.csv") -Encoding UTF8

& docker compose exec -T timescaledb psql -U $PostgresUser -d $PostgresDb --csv -c @"
SELECT scenario,
       min(processing_time) AS first_processing_time,
       max(processing_time) AS last_processing_time,
       count(*) AS total_events,
       round((count(*) / NULLIF(extract(epoch FROM max(processing_time) - min(processing_time)),0))::numeric,2) AS effective_events_per_second
FROM financial_events
GROUP BY scenario
ORDER BY scenario
"@ 2>$null | Set-Content -LiteralPath (Join-Path $summaryDir "throughput-by-scenario.csv") -Encoding UTF8

& docker compose exec -T timescaledb psql -U $PostgresUser -d $PostgresDb --csv -c @"
SELECT time_bucket('1 minute', processing_time) AS minute,
       scenario,
       count(*) AS events,
       round(avg(ingestion_latency_ms)::numeric,2) AS avg_latency_ms,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY ingestion_latency_ms) AS p95_latency_ms
FROM financial_events
GROUP BY minute, scenario
ORDER BY minute, scenario
"@ 2>$null | Set-Content -LiteralPath (Join-Path $summaryDir "minute-by-minute.csv") -Encoding UTF8

& docker compose exec -T timescaledb psql -U $PostgresUser -d $PostgresDb --csv -c @"
SELECT bucket_start,
       bucket_end,
       symbol,
       scenario,
       event_count,
       avg_price,
       total_quantity,
       avg_ingestion_latency_ms,
       p95_ingestion_latency_ms,
       max_ingestion_latency_ms
FROM financial_event_metrics
ORDER BY bucket_start, symbol, scenario
"@ 2>$null | Set-Content -LiteralPath (Join-Path $summaryDir "aggregated-metrics.csv") -Encoding UTF8

# Tempo de resposta das consultas por cenário (agrega os query-response-times.csv de cada run)
$allQueryTimings = Get-ChildItem -Path $OutputRoot -Recurse -Filter "query-response-times.csv" -ErrorAction SilentlyContinue |
  Where-Object { $_.DirectoryName -notlike "*summary*" }
if ($allQueryTimings) {
  $qtRows = @("query,window,scenario,type,elapsed_ms")
  foreach ($f in $allQueryTimings) {
    Import-Csv -LiteralPath $f.FullName | Where-Object { $_.elapsed_ms -and $_.elapsed_ms -ne "elapsed_ms" } | ForEach-Object {
      $qtRows += "$($_.query),$($_.window),$($_.scenario),$($_.elapsed_ms)"
    }
  }
  $qtRows | Set-Content -LiteralPath (Join-Path $summaryDir "query-response-times.csv") -Encoding UTF8
}

# Uso médio de CPU e memória separado por cenário
$allSamples = Get-ChildItem -Path $OutputRoot -Recurse -Filter "docker-stats-samples.csv" -ErrorAction SilentlyContinue
if ($allSamples) {
  $resourceRows = @("scenario,container,sample_count,avg_cpu_percent,avg_mem_percent")
  $grouped = @{}
  foreach ($f in $allSamples) {
    $dirName = Split-Path $f.DirectoryName -Leaf
    $scenario = ($dirName -split '-')[-1]
    Import-Csv -LiteralPath $f.FullName | Where-Object { $_.cpu_percent -and $_.cpu_percent -ne "cpu_percent" } | ForEach-Object {
      $key = "$scenario|$($_.container)"
      $cpu = [double]($_.cpu_percent -replace '%','')
      $mem = [double]($_.memory_percent -replace '%','')
      if (-not $grouped[$key]) { $grouped[$key] = @{ scenario = $scenario; container = $_.container; cpu = @(); mem = @() } }
      $grouped[$key].cpu += $cpu
      $grouped[$key].mem += $mem
    }
  }
  foreach ($key in ($grouped.Keys | Sort-Object)) {
    $entry = $grouped[$key]
    $avgCpu = [math]::Round(($entry.cpu | Measure-Object -Average).Average, 2)
    $avgMem = [math]::Round(($entry.mem | Measure-Object -Average).Average, 2)
    $resourceRows += "$($entry.scenario),$($entry.container),$($entry.cpu.Count),$avgCpu,$avgMem"
  }
  $resourceRows | Set-Content -LiteralPath (Join-Path $summaryDir "resource-usage-summary.csv") -Encoding UTF8
}

docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.NetIO}}\t{{.BlockIO}}" |
  Set-Content -LiteralPath (Join-Path $summaryDir "docker-stats.txt") -Encoding UTF8

@"
# Resumo Comparativo

Gerado em: $((Get-Date).ToUniversalTime().ToString("o"))

Banco: $PostgresDb
Usuário: $PostgresUser

Arquivos:

- latency-by-scenario.csv       (avg, stddev, p50, p95, max por cenário)
- throughput-by-scenario.csv    (eventos/s efetivo por cenário)
- minute-by-minute.csv          (série temporal minuto a minuto)
- aggregated-metrics.csv        (janelas de 10 s do Flink)
- query-response-times.csv      (tempo de resposta das consultas 5 min / 1 h / 24 h por cenário)
- resource-usage-summary.csv    (média de CPU% e memória% por container por cenário)
- docker-stats.txt              (snapshot final dos containers)

Use estes arquivos para comparar latência média, p50, p95, desvio padrão, throughput efetivo, tempo de resposta de consultas e consumo de recursos entre os cenários.
"@ | Set-Content -LiteralPath (Join-Path $summaryDir "README.md") -Encoding UTF8

Write-Host "Summary saved to $summaryDir"
