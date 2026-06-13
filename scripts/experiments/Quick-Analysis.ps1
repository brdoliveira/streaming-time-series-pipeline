# Quick Analysis - Analisa resultados JDBC variation rapidamente
param([string]$ResultsRoot = "results-jdbc-variation")

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "ANALISE RAPIDA - JDBC Variation" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Encontrar todos os diretórios de teste
$testDirs = Get-ChildItem -LiteralPath $ResultsRoot -Directory -ErrorAction SilentlyContinue |
  Where-Object { $_.Name -match "^\d{8}-\d{6}-batch\d+-interval\d+$" }

if ($testDirs.Count -eq 0) {
  Write-Host "Nenhum teste encontrado em $ResultsRoot" -ForegroundColor Red
  exit 1
}

Write-Host "Encontrados $($testDirs.Count) testes" -ForegroundColor Green
Write-Host ""

# Coletar resultados
$results = @()

foreach ($dir in $testDirs) {
  if ($dir.Name -match "batch(\d+)-interval(\d+)") {
    $batchSize = [int]$Matches[1]
    $interval = [int]$Matches[2]

    $latencyCsv = Join-Path $dir.FullName "latency-summary.csv"
    if (Test-Path $latencyCsv) {
      $data = Get-Content -LiteralPath $latencyCsv | ConvertFrom-Csv
      $metadataPath = Join-Path $dir.FullName "metadata.json"
      if (-not (Test-Path $metadataPath)) {
        continue
      }
      $metadata = Get-Content -Raw -LiteralPath $metadataPath -Encoding UTF8 | ConvertFrom-Json
      $expectedEvents = [long]$metadata.rate_per_second * [long]$metadata.duration_seconds
      $observedEvents = [long]$data.total_events
      $countTolerance = [math]::Max(1, [math]::Ceiling($expectedEvents * 0.01))
      $validEventCount = [math]::Abs($observedEvents - $expectedEvents) -le $countTolerance
      $stateResetRecorded = (
        $metadata.PSObject.Properties.Name -contains "state_reset" -and
        [bool]$metadata.state_reset
      )

      $results += [PSCustomObject]@{
        BatchSize = $batchSize
        IntervalMs = $interval
        ExpectedEvents = $expectedEvents
        Events = $observedEvents
        Valid = ($validEventCount -and $stateResetRecorded)
        P50 = [double]$data.p50_latency_ms
        P95 = [double]$data.p95_latency_ms
        P99 = [double]$data.p99_latency_ms
        Avg = [double]$data.avg_latency_ms
        StdDev = [double]$data.stddev_latency_ms
        Dir = $dir.Name
      }
    }
  }
}

$invalidResults = @($results | Where-Object { -not $_.Valid })
$results = @($results | Where-Object { $_.Valid })

if ($invalidResults.Count -gt 0) {
  Write-Warning "$($invalidResults.Count) execução(ões) excluída(s) por contagem incompatível ou reset de estado não registrado"
}

if ($results.Count -eq 0) {
  Write-Host "Nenhuma execução válida encontrada" -ForegroundColor Red
  exit 1
}

# Exibir resultados em tabela
Write-Host "RESULTADOS COLETADOS:" -ForegroundColor Yellow
Write-Host ""
Write-Host "Format" -ForegroundColor White
$results | Sort-Object -Property BatchSize, IntervalMs |
  Format-Table -AutoSize -Property @(
    @{Label="BatchSize"; Expression={$_.BatchSize}; Width=10},
    @{Label="Interval(ms)"; Expression={$_.IntervalMs}; Width=12},
    @{Label="P50"; Expression={[math]::Round($_.P50, 1)}; Width=8},
    @{Label="P95"; Expression={[math]::Round($_.P95, 1)}; Width=8},
    @{Label="P99"; Expression={[math]::Round($_.P99, 1)}; Width=8},
    @{Label="Avg"; Expression={[math]::Round($_.Avg, 1)}; Width=8},
    @{Label="Events"; Expression={$_.Events}; Width=10}
  )

# Análise
Write-Host ""
Write-Host "ANALISE:" -ForegroundColor Yellow
Write-Host ""

# Menor P95 observado
$bestP95 = $results | Sort-Object -Property P95 | Select-Object -First 1
Write-Host "Menor P95 observado: BatchSize=$($bestP95.BatchSize), Interval=$($bestP95.IntervalMs)ms"
Write-Host "  P95: $([math]::Round($bestP95.P95, 2)) ms"
Write-Host "  P99: $([math]::Round($bestP95.P99, 2)) ms"
Write-Host "  Avg: $([math]::Round($bestP95.Avg, 2)) ms"

Write-Host ""

# Comparação por BatchSize
Write-Host "P95 por BatchSize:" -ForegroundColor Yellow
$byBatchSize = $results | Group-Object -Property BatchSize | Sort-Object -Property Name
foreach ($group in $byBatchSize) {
  $avgP95 = ($group.Group | Measure-Object -Property P95 -Average).Average
  Write-Host "  BatchSize $($group.Name): P95 media = $([math]::Round($avgP95, 2)) ms"
}

Write-Host ""

# Comparação por Interval
Write-Host "P95 por Interval:" -ForegroundColor Yellow
$byInterval = $results | Group-Object -Property IntervalMs | Sort-Object -Property Name
foreach ($group in $byInterval) {
  $avgP95 = ($group.Group | Measure-Object -Property P95 -Average).Average
  Write-Host "  Interval $($group.Name)ms: P95 media = $([math]::Round($avgP95, 2)) ms"
}

Write-Host ""
$underRepeatedConfigs = @(
  $results |
    Group-Object BatchSize, IntervalMs |
    Where-Object { $_.Count -lt 3 }
)
if ($underRepeatedConfigs.Count -gt 0) {
  Write-Warning "Ha configuracoes com menos de tres repeticoes validas; nao conclua qual configuracao e melhor"
}

Write-Host "========================================" -ForegroundColor Green
Write-Host "Analise completa!"
Write-Host "========================================" -ForegroundColor Green
Write-Host ""

# Salvar CSV
$csvPath = Join-Path $ResultsRoot "quick-analysis.csv"
$results | Sort-Object -Property BatchSize, IntervalMs | Export-Csv -LiteralPath $csvPath -NoTypeInformation
Write-Host "Resultados salvos em: $csvPath"
