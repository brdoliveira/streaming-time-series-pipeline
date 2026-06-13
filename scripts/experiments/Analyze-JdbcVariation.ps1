param(
  [string]$ResultsRoot = "results-jdbc-variation",

  [switch]$IncludeInvalid
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $ResultsRoot)) {
  throw "Diretorio de resultados nao encontrado: $ResultsRoot"
}

$experimentDirs = Get-ChildItem -LiteralPath $ResultsRoot -Directory |
  Where-Object { $_.Name -match "^\d{8}-\d{6}-batch\d+-interval\d+$" }

$results = foreach ($dir in $experimentDirs) {
  $metadataPath = Join-Path $dir.FullName "metadata.json"
  $latencyPath = Join-Path $dir.FullName "latency-summary.csv"
  $throughputPath = Join-Path $dir.FullName "throughput.csv"

  if (-not (Test-Path $metadataPath) -or -not (Test-Path $latencyPath)) {
    Write-Warning "Execucao incompleta ignorada: $($dir.Name)"
    continue
  }

  $metadata = Get-Content -Raw -LiteralPath $metadataPath -Encoding UTF8 | ConvertFrom-Json
  $latency = Import-Csv -LiteralPath $latencyPath
  $throughput = if (Test-Path $throughputPath) {
    Import-Csv -LiteralPath $throughputPath
  } else {
    $null
  }

  $expectedEvents = [long]$metadata.rate_per_second * [long]$metadata.duration_seconds
  $observedEvents = [long]$latency.total_events
  $countTolerance = [math]::Max(1, [math]::Ceiling($expectedEvents * 0.01))
  $validEventCount = [math]::Abs($observedEvents - $expectedEvents) -le $countTolerance
  $stateResetRecorded = (
    $metadata.PSObject.Properties.Name -contains "state_reset" -and
    [bool]$metadata.state_reset
  )

  [PSCustomObject]@{
    BatchSize = [int]$metadata.jdbc_batch_size
    BatchIntervalMs = [int]$metadata.jdbc_batch_interval_ms
    ExpectedEvents = $expectedEvents
    TotalEvents = $observedEvents
    ValidEventCount = $validEventCount
    StateResetRecorded = $stateResetRecorded
    AvgLatencyMs = [double]$latency.avg_latency_ms
    P50LatencyMs = [double]$latency.p50_latency_ms
    P95LatencyMs = [double]$latency.p95_latency_ms
    P99LatencyMs = [double]$latency.p99_latency_ms
    MaxLatencyMs = [double]$latency.max_latency_ms
    StddevLatencyMs = [double]$latency.stddev_latency_ms
    EffectiveThroughput = if ($throughput) {
      [double]$throughput.effective_events_per_second
    } else {
      0
    }
    ExperimentDir = $dir.Name
  }
}

$auditPath = Join-Path $ResultsRoot "audit-results.csv"
$results |
  Sort-Object BatchSize, BatchIntervalMs, ExperimentDir |
  Export-Csv -LiteralPath $auditPath -NoTypeInformation -Encoding UTF8

$validResults = @(
  $results | Where-Object {
    $_.ValidEventCount -and $_.StateResetRecorded
  }
)
if ($IncludeInvalid) {
  $validResults = @($results)
}

$consolidatedPath = Join-Path $ResultsRoot "consolidated-results.csv"
$validResults |
  Sort-Object BatchSize, BatchIntervalMs, ExperimentDir |
  Export-Csv -LiteralPath $consolidatedPath -NoTypeInformation -Encoding UTF8

Write-Host "Execucoes auditadas: $($results.Count)"
Write-Host "Execucoes elegiveis para consolidacao: $($validResults.Count)"
Write-Host "Auditoria: $auditPath"
Write-Host "Consolidado: $consolidatedPath"

if ($validResults.Count -eq 0) {
  Write-Warning "Nenhuma execucao valida para analise comparativa"
  exit 0
}

$batchAnalysis = $validResults |
  Group-Object BatchSize |
  ForEach-Object {
    [PSCustomObject]@{
      BatchSize = [int]$_.Name
      Count = $_.Count
      AvgP95LatencyMs = [math]::Round(
        ($_.Group | Measure-Object P95LatencyMs -Average).Average,
        2
      )
    }
  } |
  Sort-Object BatchSize

$intervalAnalysis = $validResults |
  Group-Object BatchIntervalMs |
  ForEach-Object {
    [PSCustomObject]@{
      BatchIntervalMs = [int]$_.Name
      Count = $_.Count
      AvgP95LatencyMs = [math]::Round(
        ($_.Group | Measure-Object P95LatencyMs -Average).Average,
        2
      )
    }
  } |
  Sort-Object BatchIntervalMs

$batchAnalysis |
  Export-Csv -LiteralPath (Join-Path $ResultsRoot "analysis-by-batch-size.csv") `
    -NoTypeInformation -Encoding UTF8

$intervalAnalysis |
  Export-Csv -LiteralPath (Join-Path $ResultsRoot "analysis-by-batch-interval.csv") `
    -NoTypeInformation -Encoding UTF8

$batchSizes = @($validResults.BatchSize | Sort-Object -Unique)
$intervals = @($validResults.BatchIntervalMs | Sort-Object -Unique)
$matrix = foreach ($interval in $intervals) {
  $row = [ordered]@{ BatchIntervalMs = $interval }
  foreach ($batchSize in $batchSizes) {
    $measure = $validResults |
      Where-Object {
        $_.BatchSize -eq $batchSize -and
        $_.BatchIntervalMs -eq $interval
      } |
      Measure-Object P95LatencyMs -Average

    $row["BS_$batchSize"] = if ($measure.Count -gt 0) {
      [math]::Round($measure.Average, 2)
    } else {
      ""
    }
  }
  [PSCustomObject]$row
}

$matrix |
  Export-Csv -LiteralPath (Join-Path $ResultsRoot "analysis-matrix-p95.csv") `
    -NoTypeInformation -Encoding UTF8

$validResults |
  Sort-Object P95LatencyMs |
  Format-Table BatchSize, BatchIntervalMs, TotalEvents, P50LatencyMs, P95LatencyMs, P99LatencyMs, EffectiveThroughput -AutoSize
