# Quick Start - Teste de Validacao (7 minutos)
param([switch]$CleanFirst)

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "QUICK START - Validacao JDBC Variation" -ForegroundColor Cyan
Write-Host "Tempo estimado: 7 minutos" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

$scriptDir = Split-Path -LiteralPath $MyInvocation.MyCommand.Path

# 1. Validação inicial
Write-Host "Passo 1/4: Validando ambiente..." -ForegroundColor Yellow

$dockerVersion = docker --version 2>$null
if ($LASTEXITCODE -eq 0) {
  Write-Host "  [OK] Docker encontrado"
} else {
  Write-Host "  [ERRO] Docker nao encontrado" -ForegroundColor Red
  exit 1
}

$diskInfo = Get-Volume | Where-Object { $_.DriveLetter -eq 'C' }
$freeGb = [math]::Round($diskInfo.SizeRemaining / 1GB, 2)
Write-Host "  [OK] Espaco em disco: ${freeGb} GB"

# 2. Limpeza (opcional)
Write-Host ""
Write-Host "Passo 2/4: Preparando ambiente..." -ForegroundColor Yellow

if ($CleanFirst) {
  Write-Host "  Limpeza explícita solicitada; o runner também fará o reset obrigatório."
}

# 3. Executar teste
Write-Host ""
Write-Host "Passo 3/4: Executando teste (6-7 minutos)..." -ForegroundColor Yellow
Write-Host "  Parametros:"
Write-Host "    BatchSize: 500"
Write-Host "    BatchInterval: 1000 ms"
Write-Host "    Taxa: 1000 eventos/s"
Write-Host "    Duracao: 300 segundos (5 min)"
Write-Host ""

$startTime = Get-Date

& (Join-Path $scriptDir "Run-JdbcVariationExperiment.ps1") `
  -JdbcBatchSize 500 `
  -JdbcBatchIntervalMs 1000 `
  -OutputRoot "results-jdbc-variation" `
  -Build

$endTime = Get-Date
$duration = ($endTime - $startTime).TotalMinutes

# 4. Validar resultados
Write-Host ""
Write-Host "Passo 4/4: Validando resultados..." -ForegroundColor Yellow

$resultDir = Get-ChildItem -LiteralPath "results-jdbc-variation" -Directory -ErrorAction SilentlyContinue |
  Sort-Object -Property LastWriteTime -Descending |
  Select-Object -First 1

if ($null -eq $resultDir) {
  Write-Host "  [ERRO] Nenhum resultado encontrado" -ForegroundColor Red
  exit 1
}

Write-Host "  [OK] Resultados: $($resultDir.Name)"

# Verificar arquivos
$expectedFiles = @("metadata.json", "latency-summary.csv", "latency-by-bucket.csv", "throughput.csv", "docker-stats-samples.csv")
foreach ($file in $expectedFiles) {
  $path = Join-Path $resultDir.FullName $file
  if (Test-Path $path) {
    Write-Host "  [OK] $file"
  } else {
    Write-Host "  [AUSENTE] $file" -ForegroundColor Yellow
  }
}

# Exibir métricas
Write-Host ""
Write-Host "METRICAS COLETADAS:" -ForegroundColor Green

$latencyCsv = Join-Path $resultDir.FullName "latency-summary.csv"
if (Test-Path $latencyCsv) {
  $latency = Get-Content -LiteralPath $latencyCsv | ConvertFrom-Csv
  Write-Host "  Eventos: $($latency.total_events)"
  Write-Host "  P95 (CRITICO): $($latency.p95_latency_ms) ms"
  Write-Host "  P99: $($latency.p99_latency_ms) ms"
  Write-Host "  Avg: $($latency.avg_latency_ms) ms"
  Write-Host "  StdDev: $($latency.stddev_latency_ms) ms"
}

# Resultado final
Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "VALIDACAO CONCLUIDA COM SUCESSO!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green

Write-Host ""
Write-Host "PROXIMOS PASSOS:" -ForegroundColor Cyan
Write-Host ""
Write-Host "1. Rodar matriz reduzida (cerca de 2 horas, 18 execucoes):"
Write-Host "   .\scripts\experiments\Invoke-JdbcVariationMatrix.ps1 -Repetitions 3 -BatchSizes @(100,500,1000) -BatchIntervals @(500,1000)"
Write-Host ""
Write-Host "2. Ou rodar matriz completa:"
Write-Host "   .\scripts\experiments\Invoke-JdbcVariationMatrix.ps1"
Write-Host ""
Write-Host "3. Analisar resultados:"
Write-Host "   .\scripts\experiments\Analyze-JdbcVariation.ps1"
Write-Host ""
Write-Host "Tempo total: $([math]::Round($duration, 2)) minutos"
Write-Host ""
