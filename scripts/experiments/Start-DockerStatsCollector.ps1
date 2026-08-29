[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)]
  [string]$OutputPath,

  [ValidateRange(1, 3600)]
  [int]$IntervalSeconds = 5,

  [string]$FlinkMetricsOutputPath = ""
)

$ErrorActionPreference = "Stop"

function ConvertTo-QuotedArgument {
  param([string]$Value)
  return '"' + $Value.Replace('"', '\"') + '"'
}

$outputFullPath = [IO.Path]::GetFullPath($OutputPath)
$stopSignalPath = "$outputFullPath.stop"
$collectorLogPath = "$outputFullPath.collector.log"
$collectorScriptPath = Join-Path $PSScriptRoot "Collect-DockerStats.ps1"

if (-not (Test-Path -LiteralPath $collectorScriptPath -PathType Leaf)) {
  throw "Script do coletor nao encontrado: $collectorScriptPath"
}

if (Test-Path -LiteralPath $stopSignalPath) {
  Remove-Item -LiteralPath $stopSignalPath -Force
}

$enginePath = (Get-Process -Id $PID).Path
if (-not $enginePath) {
  throw "Nao foi possivel localizar o executavel do PowerShell atual."
}

$arguments = @(
  "-NoProfile",
  "-NonInteractive",
  "-ExecutionPolicy", "Bypass",
  "-File", (ConvertTo-QuotedArgument $collectorScriptPath),
  "-OutputPath", (ConvertTo-QuotedArgument $outputFullPath),
  "-StopSignalPath", (ConvertTo-QuotedArgument $stopSignalPath),
  "-CollectorLogPath", (ConvertTo-QuotedArgument $collectorLogPath),
  "-IntervalSeconds", [string]$IntervalSeconds
)

if ($FlinkMetricsOutputPath) {
  $flinkFullPath = [IO.Path]::GetFullPath($FlinkMetricsOutputPath)
  $arguments += @("-FlinkMetricsOutputPath", (ConvertTo-QuotedArgument $flinkFullPath))
}
else {
  $flinkFullPath = ""
}

$process = Start-Process `
  -FilePath $enginePath `
  -ArgumentList ($arguments -join " ") `
  -WindowStyle Hidden `
  -PassThru

Start-Sleep -Milliseconds 750
$process.Refresh()
if ($process.HasExited) {
  $details = if (Test-Path -LiteralPath $collectorLogPath) {
    (Get-Content -LiteralPath $collectorLogPath -Raw).Trim()
  } else {
    "log do coletor indisponivel"
  }
  throw "O coletor de recursos terminou durante a inicializacao (exit_code=$($process.ExitCode)). $details"
}

[pscustomobject]@{
  process_id = $process.Id
  output_path = $outputFullPath
  stop_signal_path = $stopSignalPath
  collector_log_path = $collectorLogPath
  flink_metrics_output_path = $flinkFullPath
  started_at = (Get-Date).ToUniversalTime().ToString("o")
}
