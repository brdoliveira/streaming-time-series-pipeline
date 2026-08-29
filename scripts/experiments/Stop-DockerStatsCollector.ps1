[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)]
  [int]$ProcessId,

  [Parameter(Mandatory = $true)]
  [string]$OutputPath,

  [Parameter(Mandatory = $true)]
  [string]$StopSignalPath,

  [Parameter(Mandatory = $true)]
  [string]$CollectorLogPath,

  [string]$FlinkMetricsOutputPath = "",

  [ValidateRange(1, 300)]
  [int]$TimeoutSeconds = 30
)

$ErrorActionPreference = "Stop"
$expectedHeader = "timestamp_utc,container,cpu_percent,memory_usage,memory_percent,net_io,block_io"

function Get-CollectorLogDetails {
  if (Test-Path -LiteralPath $CollectorLogPath -PathType Leaf) {
    return (Get-Content -LiteralPath $CollectorLogPath -Raw).Trim()
  }
  return "log do coletor indisponivel"
}

$process = Get-Process -Id $ProcessId -ErrorAction SilentlyContinue
if (-not $process) {
  throw "O coletor de recursos (PID $ProcessId) terminou antes do encerramento solicitado. $(Get-CollectorLogDetails)"
}

try {
  "stop" | Set-Content -LiteralPath $StopSignalPath -Encoding ASCII

  if (-not $process.WaitForExit($TimeoutSeconds * 1000)) {
    Stop-Process -Id $ProcessId -Force -ErrorAction SilentlyContinue
    throw "Timeout de ${TimeoutSeconds}s ao encerrar o coletor de recursos (PID $ProcessId)."
  }

  $exitCode = $process.ExitCode
  if ($exitCode -ne 0) {
    throw "O coletor de recursos terminou com exit_code=$exitCode. $(Get-CollectorLogDetails)"
  }
}
finally {
  Remove-Item -LiteralPath $StopSignalPath -Force -ErrorAction SilentlyContinue
}

if (-not (Test-Path -LiteralPath $OutputPath -PathType Leaf)) {
  throw "O coletor nao gerou o arquivo esperado: $OutputPath"
}

$lines = @(Get-Content -LiteralPath $OutputPath)
if ($lines.Count -lt 2) {
  throw "A coleta de docker stats nao possui amostras: $OutputPath"
}
if ($lines[0].Trim() -ne $expectedHeader) {
  throw "Cabecalho invalido na coleta de docker stats: '$($lines[0])'"
}

$rows = @($lines | Select-Object -Skip 1 | Where-Object { $_.Trim() })
$parsedRows = @($rows | ConvertFrom-Csv -Header @(
  "timestamp_utc", "container", "cpu_percent", "memory_usage",
  "memory_percent", "net_io", "block_io"
))

foreach ($row in $parsedRows) {
  $parsedTimestamp = [DateTimeOffset]::MinValue
  if (-not [DateTimeOffset]::TryParse($row.timestamp_utc, [ref]$parsedTimestamp)) {
    throw "Timestamp invalido na coleta de docker stats: '$($row.timestamp_utc)'"
  }
  if ([string]::IsNullOrWhiteSpace($row.container) -or
      [string]::IsNullOrWhiteSpace($row.cpu_percent) -or
      [string]::IsNullOrWhiteSpace($row.memory_usage)) {
    throw "Amostra incompleta na coleta de docker stats: $($row | ConvertTo-Csv -NoTypeInformation | Select-Object -Last 1)"
  }
}

$flinkSampleCount = 0
if ($FlinkMetricsOutputPath -and (Test-Path -LiteralPath $FlinkMetricsOutputPath -PathType Leaf)) {
  $flinkLines = @(Get-Content -LiteralPath $FlinkMetricsOutputPath)
  if ($flinkLines.Count -gt 1) {
    $flinkSampleCount = @($flinkLines | Select-Object -Skip 1 | Where-Object { $_.Trim() }).Count
  }
}

[pscustomobject]@{
  status = "success"
  process_id = $ProcessId
  exit_code = 0
  docker_sample_count = $parsedRows.Count
  flink_sample_count = $flinkSampleCount
  completed_at = (Get-Date).ToUniversalTime().ToString("o")
}
