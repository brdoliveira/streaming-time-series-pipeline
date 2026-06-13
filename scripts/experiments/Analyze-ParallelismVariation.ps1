param(
  [string]$ResultsRoot = "results-parallelism-variation",

  [switch]$IncludeInvalid
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $ResultsRoot)) {
  throw "Diretório de resultados não encontrado: $ResultsRoot"
}

$experimentDirs = Get-ChildItem -LiteralPath $ResultsRoot -Directory |
  Where-Object { $_.Name -match "^\d{8}-\d{6}-parallelism\d+$" }

$results = foreach ($dir in $experimentDirs) {
  $metadataPath = Join-Path $dir.FullName "metadata.json"
  $latencyPath = Join-Path $dir.FullName "latency-summary.csv"
  $throughputPath = Join-Path $dir.FullName "throughput.csv"

  if (-not (Test-Path $metadataPath) -or -not (Test-Path $latencyPath)) {
    Write-Warning "Execução incompleta ignorada: $($dir.Name)"
    continue
  }

  $metadata = Get-Content -Raw -LiteralPath $metadataPath -Encoding UTF8 | ConvertFrom-Json
  $latency = Import-Csv -LiteralPath $latencyPath
  $throughput = if (Test-Path $throughputPath) { Import-Csv -LiteralPath $throughputPath } else { $null }

  $expectedEvents = [long]$metadata.rate_per_second * [long]$metadata.duration_seconds
  $observedEvents = [long]$latency.total_events
  $countTolerance = [math]::Max(1, [math]::Ceiling($expectedEvents * 0.01))
  $validEventCount = [math]::Abs($observedEvents - $expectedEvents) -le $countTolerance
  $parallelism = [int]$metadata.flink_parallelism
  $taskSlots = if ($metadata.flink_task_slots) { [int]$metadata.flink_task_slots } else { 0 }
  $stateResetRecorded = (
    $metadata.PSObject.Properties.Name -contains "state_reset" -and
    [bool]$metadata.state_reset
  )
  $configApplied = $stateResetRecorded -and $taskSlots -ge $parallelism

  [PSCustomObject]@{
    Parallelism = $parallelism
    TaskSlots = $taskSlots
    ExpectedEvents = $expectedEvents
    TotalEvents = $observedEvents
    ValidEventCount = $validEventCount
    StateResetRecorded = $stateResetRecorded
    ConfigApplied = $configApplied
    AvgLatencyMs = [double]$latency.avg_latency_ms
    P50LatencyMs = [double]$latency.p50_latency_ms
    P95LatencyMs = [double]$latency.p95_latency_ms
    P99LatencyMs = [double]$latency.p99_latency_ms
    MaxLatencyMs = [double]$latency.max_latency_ms
    StddevLatencyMs = [double]$latency.stddev_latency_ms
    EffectiveThroughput = if ($throughput) { [double]$throughput.effective_events_per_second } else { 0 }
    ExperimentDir = $dir.Name
  }
}

$auditPath = Join-Path $ResultsRoot "audit-results.csv"
$results | Sort-Object Parallelism | Export-Csv -LiteralPath $auditPath -NoTypeInformation -Encoding UTF8

$validResults = @($results | Where-Object { $_.ValidEventCount -and $_.ConfigApplied })
if ($IncludeInvalid) {
  $validResults = @($results)
}

$consolidatedPath = Join-Path $ResultsRoot "consolidated-results.csv"
$validResults | Sort-Object Parallelism | Export-Csv -LiteralPath $consolidatedPath -NoTypeInformation -Encoding UTF8

Write-Host "Execuções auditadas: $($results.Count)"
Write-Host "Execuções elegíveis para consolidação: $($validResults.Count)"
Write-Host "Auditoria: $auditPath"
Write-Host "Consolidado: $consolidatedPath"

if ($validResults.Count -eq 0) {
  Write-Warning "Nenhuma execução válida para análise comparativa"
  exit 0
}

$validResults |
  Sort-Object P95LatencyMs |
  Format-Table Parallelism, TaskSlots, TotalEvents, P50LatencyMs, P95LatencyMs, P99LatencyMs, EffectiveThroughput -AutoSize
