param(
  [string]$OutputRoot = "results-jdbc-variation",

  [ValidateRange(1, 100)]
  [int]$Repetitions = 3,

  [int[]]$BatchSizes = @(50, 100, 250, 500, 1000, 2000),

  [int[]]$BatchIntervals = @(250, 500, 1000, 2000),

  [int]$RatePerSecond = 1000,

  [int]$DurationSeconds = 300,

  [switch]$Build,

  [switch]$ContinueOnError
)

$ErrorActionPreference = "Stop"

# Criar diretório de saída
New-Item -ItemType Directory -Force -Path $OutputRoot | Out-Null

# Metadados da matriz
$matrixMetadata = [ordered]@{
  experiment_type = "jdbc_variation_matrix"
  batch_sizes = $BatchSizes
  batch_intervals = $BatchIntervals
  repetitions = $Repetitions
  rate_per_second = $RatePerSecond
  duration_seconds = $DurationSeconds
  total_tests = ($BatchSizes.Count * $BatchIntervals.Count * $Repetitions)
  started_at = (Get-Date).ToUniversalTime().ToString("o")
  output_root = $OutputRoot
}

$matrixLogPath = Join-Path $OutputRoot "matrix-execution.log"
$matrixMetadataPath = Join-Path $OutputRoot "matrix-metadata.json"

$matrixMetadata | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $matrixMetadataPath -Encoding UTF8

# Log
$logHeader = "JDBC Variation Matrix Experiment`n" +
  "Started: $(Get-Date)`n" +
  "Total tests: $($matrixMetadata.total_tests)`n" +
  "Batch Sizes: $($BatchSizes -join ', ')`n" +
  "Batch Intervals: $($BatchIntervals -join ', ')`n" +
  "Repetitions: $Repetitions`n" +
  "Rate: $RatePerSecond ev/s`n" +
  "Duration: $DurationSeconds seconds`n" +
  "="*80

Add-Content -LiteralPath $matrixLogPath -Value $logHeader

Write-Host $logHeader

# Rastrear resultados
$testResults = @()
$testCount = 0
$passCount = 0
$failCount = 0

# Matriz de testes
foreach ($batchSize in $BatchSizes) {
  foreach ($batchInterval in $BatchIntervals) {
    foreach ($rep in 1..$Repetitions) {
      $testCount++
      $testId = "b$batchSize-i$batchInterval-rep$rep"

      Write-Host ""
      Write-Host "========================================" -ForegroundColor Cyan
      Write-Host "Teste $testCount/$($matrixMetadata.total_tests): $testId" -ForegroundColor Cyan
      Write-Host "========================================" -ForegroundColor Cyan

      $logEntry = "Test $testCount/$($matrixMetadata.total_tests): $testId"
      Add-Content -LiteralPath $matrixLogPath -Value $logEntry

      $testStartTime = Get-Date

      try {
        # Executar experimento
        $scriptPath = Join-Path $PSScriptRoot "Run-JdbcVariationExperiment.ps1"

        $scriptArgs = @{
          JdbcBatchSize = $batchSize
          JdbcBatchIntervalMs = $batchInterval
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
          BatchSize = $batchSize
          BatchInterval = $batchInterval
          Repetition = $rep
          Status = "SUCCESS"
          Duration_Seconds = [Math]::Round($duration, 2)
          Timestamp = $testStartTime.ToString("o")
        }

        $passCount++
        Write-Host "✓ Teste $testId completado em ${duration}s" -ForegroundColor Green
        Add-Content -LiteralPath $matrixLogPath -Value "  ✓ SUCCESS (${duration}s)"

      } catch {
        $testEndTime = Get-Date
        $duration = ($testEndTime - $testStartTime).TotalSeconds

        $testResults += [PSCustomObject]@{
          TestId = $testId
          BatchSize = $batchSize
          BatchInterval = $batchInterval
          Repetition = $rep
          Status = "FAILED"
          Duration_Seconds = [Math]::Round($duration, 2)
          Timestamp = $testStartTime.ToString("o")
          Error = $_.Exception.Message
        }

        $failCount++
        Write-Host "✗ Teste $testId FALHOU: $_" -ForegroundColor Red
        Add-Content -LiteralPath $matrixLogPath -Value "  ✗ FAILED: $_"

        if (-not $ContinueOnError) {
          throw
        }
      }

      # Aguardar 30s entre testes para estabilização
      if ($testCount -lt $matrixMetadata.total_tests) {
        Write-Host "Aguardando 30s para estabilização..."
        Start-Sleep -Seconds 30
      }
    }
  }
}

# Salvar resultados
$resultsCsv = Join-Path $OutputRoot "matrix-results.csv"
$testResults | Export-Csv -LiteralPath $resultsCsv -NoTypeInformation -Encoding UTF8

$resultsJson = Join-Path $OutputRoot "matrix-results.json"
$testResults | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $resultsJson -Encoding UTF8

# Gerar resumo
$summaryPath = Join-Path $OutputRoot "matrix-summary.txt"
$summary = @(
  ""
  "MATRIZ DE VARIAÇÃO JDBC - RESUMO"
  "================================"
  ""
  "Timestamp: $(Get-Date)"
  "Total de testes: $testCount"
  "Sucessos: $passCount"
  "Falhas: $failCount"
  "Taxa de sucesso: $(if ($testCount -gt 0) { [Math]::Round(($passCount / $testCount) * 100, 2) } else { 0 })%"
  ""
  "Arquivo de resultados: $resultsCsv"
  "Arquivo JSON: $resultsJson"
  "Log de execução: $matrixLogPath"
  ""
  "Próximos passos:"
  "1. Analisar resultados com: .\scripts\experiments\Analyze-JdbcVariation.ps1"
  "2. Gerar gráficos de P95 vs BatchSize"
  "3. Interpretar apenas configurações com repetições válidas"
)

$summary | Set-Content -LiteralPath $summaryPath -Encoding UTF8

Write-Host ""
Write-Host "RESUMO FINAL" -ForegroundColor Cyan
Write-Host "============" -ForegroundColor Cyan
$summary | ForEach-Object { Write-Host $_ }

# Atualizar metadados
$matrixMetadata["completed_at"] = (Get-Date).ToUniversalTime().ToString("o")
$matrixMetadata["total_tests_completed"] = $testCount
$matrixMetadata["successful_tests"] = $passCount
$matrixMetadata["failed_tests"] = $failCount
$matrixMetadata | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $matrixMetadataPath -Encoding UTF8

Write-Host ""
Write-Host "Metadados salvos em: $matrixMetadataPath" -ForegroundColor Green
Write-Host "Resultados CSV salvos em: $resultsCsv" -ForegroundColor Green
