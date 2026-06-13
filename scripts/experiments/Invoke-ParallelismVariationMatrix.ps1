param(
  [string]$OutputRoot = "results-parallelism-variation",

  [ValidateRange(1, 100)]
  [int]$Repetitions = 3,

  [int[]]$ParallelismValues = @(2, 3, 4, 6, 8),

  [int]$RatePerSecond = 1000,

  [int]$DurationSeconds = 300,

  [switch]$Build,

  [switch]$ContinueOnError
)

$ErrorActionPreference = "Stop"

New-Item -ItemType Directory -Force -Path $OutputRoot | Out-Null

$matrixMetadata = [ordered]@{
  experiment_type = "parallelism_variation_matrix"
  parallelism_values = $ParallelismValues
  repetitions = $Repetitions
  rate_per_second = $RatePerSecond
  duration_seconds = $DurationSeconds
  jdbc_batch_size = 1000
  jdbc_batch_interval_ms = 500
  total_tests = ($ParallelismValues.Count * $Repetitions)
  started_at = (Get-Date).ToUniversalTime().ToString("o")
  output_root = $OutputRoot
}

$matrixLogPath = Join-Path $OutputRoot "matrix-execution.log"
$matrixMetadataPath = Join-Path $OutputRoot "matrix-metadata.json"

$matrixMetadata | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $matrixMetadataPath -Encoding UTF8

$logHeader = "Parallelism Variation Matrix Experiment`n" +
  "Started: $(Get-Date)`n" +
  "Total tests: $($matrixMetadata.total_tests)`n" +
  "Parallelism Values: $($ParallelismValues -join ', ')`n" +
  "Repetitions: $Repetitions`n" +
  "Batch Size: 1000`n" +
  "Batch Interval: 500ms`n" +
  "Rate: $RatePerSecond ev/s`n" +
  "Duration: $DurationSeconds seconds`n" +
  "="*80

Add-Content -LiteralPath $matrixLogPath -Value $logHeader

Write-Host $logHeader

$testResults = @()
$testCount = 0
$passCount = 0
$failCount = 0

foreach ($parallelism in $ParallelismValues) {
  foreach ($rep in 1..$Repetitions) {
    $testCount++
    $testId = "p$parallelism-rep$rep"

    Write-Host ""
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host "Teste $testCount/$($matrixMetadata.total_tests): $testId" -ForegroundColor Cyan
    Write-Host "FLINK_PARALLELISM=$parallelism" -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan

    $logEntry = "Test $testCount/$($matrixMetadata.total_tests): $testId"
    Add-Content -LiteralPath $matrixLogPath -Value $logEntry

    $testStartTime = Get-Date

    try {
      $scriptPath = Join-Path $PSScriptRoot "Run-ParallelismVariationExperiment.ps1"

      $scriptArgs = @{
        FlinkParallelism = $parallelism
        OutputRoot = $OutputRoot
        RatePerSecond = $RatePerSecond
        DurationSeconds = $DurationSeconds
        StatsIntervalSeconds = 5
      }

      if ($Build -and $testCount -eq 1) {
        $scriptArgs.Build = $true
      }

      & $scriptPath @scriptArgs

      $testEndTime = Get-Date
      $duration = ($testEndTime - $testStartTime).TotalSeconds

      $testResults += [PSCustomObject]@{
        TestId = $testId
        Parallelism = $parallelism
        Repetition = $rep
        Status = "SUCCESS"
        Duration_Seconds = [Math]::Round($duration, 2)
        Timestamp = $testStartTime.ToString("o")
      }

      $passCount++
      Write-Host "Teste $testId completado em ${duration}s" -ForegroundColor Green
      Add-Content -LiteralPath $matrixLogPath -Value "  SUCCESS (${duration}s)"

    } catch {
      $testEndTime = Get-Date
      $duration = ($testEndTime - $testStartTime).TotalSeconds

      $testResults += [PSCustomObject]@{
        TestId = $testId
        Parallelism = $parallelism
        Repetition = $rep
        Status = "FAILED"
        Duration_Seconds = [Math]::Round($duration, 2)
        Timestamp = $testStartTime.ToString("o")
        Error = $_.Exception.Message
      }

      $failCount++
      Write-Host "Teste $testId FALHOU: $_" -ForegroundColor Red
      Add-Content -LiteralPath $matrixLogPath -Value "  FAILED: $_"

      if (-not $ContinueOnError) {
        throw
      }
    }

    if ($testCount -lt $matrixMetadata.total_tests) {
      Write-Host "Aguardando 30s para estabilizacao..."
      Start-Sleep -Seconds 30
    }
  }
}

$resultsCsv = Join-Path $OutputRoot "matrix-results.csv"
$testResults | Export-Csv -LiteralPath $resultsCsv -NoTypeInformation -Encoding UTF8

$resultsJson = Join-Path $OutputRoot "matrix-results.json"
$testResults | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $resultsJson -Encoding UTF8

$summaryPath = Join-Path $OutputRoot "matrix-summary.txt"
$summary = @(
  ""
  "MATRIZ DE VARIACAO DE PARALELISMO - RESUMO"
  "=========================================="
  ""
  "Timestamp: $(Get-Date)"
  "Total de testes: $testCount"
  "Sucessos: $passCount"
  "Falhas: $failCount"
  "Taxa de sucesso: $(if ($testCount -gt 0) { [Math]::Round(($passCount / $testCount) * 100, 2) } else { 0 })%"
  ""
  "Arquivo de resultados: $resultsCsv"
  "Arquivo JSON: $resultsJson"
  "Log de execucao: $matrixLogPath"
  ""
  "Proximos passos:"
  "1. Auditar resultados com: .\scripts\experiments\Analyze-ParallelismVariation.ps1 -ResultsRoot `"$OutputRoot`""
  "2. Comparar P95 latencia vs paralelismo"
  "3. Interpretar apenas configuracoes com repeticoes validas"
)

$summary | Set-Content -LiteralPath $summaryPath -Encoding UTF8

Write-Host ""
Write-Host "RESUMO FINAL" -ForegroundColor Cyan
Write-Host "============" -ForegroundColor Cyan
$summary | ForEach-Object { Write-Host $_ }

$matrixMetadata["completed_at"] = (Get-Date).ToUniversalTime().ToString("o")
$matrixMetadata["total_tests_completed"] = $testCount
$matrixMetadata["successful_tests"] = $passCount
$matrixMetadata["failed_tests"] = $failCount
$matrixMetadata | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $matrixMetadataPath -Encoding UTF8

Write-Host ""
Write-Host "Resultados salvos em: $OutputRoot" -ForegroundColor Green
