[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)]
  [string]$OutputPath,

  [Parameter(Mandatory = $true)]
  [string]$StopSignalPath,

  [Parameter(Mandatory = $true)]
  [string]$CollectorLogPath,

  [ValidateRange(1, 3600)]
  [int]$IntervalSeconds = 5,

  [string]$FlinkMetricsOutputPath = ""
)

$ErrorActionPreference = "Stop"
$dockerHeader = "timestamp_utc,container,cpu_percent,memory_usage,memory_percent,net_io,block_io"
$flinkHeader = "timestamp_utc,metric_name,metric_value"

function Write-CollectorLog {
  param(
    [ValidateSet("INFO", "WARN", "ERROR")]
    [string]$Level,
    [string]$Message
  )

  $timestamp = (Get-Date).ToUniversalTime().ToString("o")
  Add-Content -LiteralPath $CollectorLogPath -Value "$timestamp [$Level] $Message" -Encoding UTF8
}

function Initialize-OutputFile {
  param(
    [string]$Path,
    [string]$Header
  )

  $parent = Split-Path -Parent $Path
  if ($parent) {
    New-Item -ItemType Directory -Force -Path $parent | Out-Null
  }
  $Header | Set-Content -LiteralPath $Path -Encoding UTF8
}

function Wait-CollectorInterval {
  param([int]$Seconds)

  $deadline = (Get-Date).AddSeconds($Seconds)
  while ((Get-Date) -lt $deadline) {
    if (Test-Path -LiteralPath $StopSignalPath) {
      return
    }
    Start-Sleep -Milliseconds 250
  }
}

try {
  $OutputPath = [IO.Path]::GetFullPath($OutputPath)
  $StopSignalPath = [IO.Path]::GetFullPath($StopSignalPath)
  $CollectorLogPath = [IO.Path]::GetFullPath($CollectorLogPath)
  if ($FlinkMetricsOutputPath) {
    $FlinkMetricsOutputPath = [IO.Path]::GetFullPath($FlinkMetricsOutputPath)
  }

  $logParent = Split-Path -Parent $CollectorLogPath
  if ($logParent) {
    New-Item -ItemType Directory -Force -Path $logParent | Out-Null
  }
  "" | Set-Content -LiteralPath $CollectorLogPath -Encoding UTF8
  Initialize-OutputFile -Path $OutputPath -Header $dockerHeader
  if ($FlinkMetricsOutputPath) {
    Initialize-OutputFile -Path $FlinkMetricsOutputPath -Header $flinkHeader
  }

  $flinkJobId = $null
  $dockerSampleCount = 0
  $flinkSampleCount = 0
  Write-CollectorLog -Level INFO -Message "Coletor iniciado (intervalo=${IntervalSeconds}s)."

  while (-not (Test-Path -LiteralPath $StopSignalPath)) {
    $timestamp = (Get-Date).ToUniversalTime().ToString("o")
    $dockerOutput = @(& docker stats --no-stream --format "{{.Container}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}},{{.NetIO}},{{.BlockIO}}" 2>&1)
    $dockerExitCode = $LASTEXITCODE

    if ($dockerExitCode -ne 0) {
      $details = ($dockerOutput | ForEach-Object { $_.ToString() }) -join " | "
      throw "docker stats falhou com codigo $dockerExitCode. $details"
    }

    $dockerRows = @($dockerOutput | ForEach-Object { $_.ToString().Trim() } | Where-Object { $_ })
    if ($dockerRows.Count -eq 0) {
      throw "docker stats terminou sem retornar containers."
    }

    foreach ($row in $dockerRows) {
      "$timestamp,$row" | Add-Content -LiteralPath $OutputPath -Encoding UTF8
      $dockerSampleCount++
    }

    if ($FlinkMetricsOutputPath) {
      if (-not $flinkJobId) {
        try {
          $jobsResponse = Invoke-RestMethod -Uri "http://localhost:8081/jobs" -TimeoutSec 2 -ErrorAction Stop
          $runningJob = $jobsResponse.jobs |
            Where-Object { $_.status -eq "RUNNING" } |
            Select-Object -First 1
          if ($runningJob) {
            $flinkJobId = $runningJob.id
            Write-CollectorLog -Level INFO -Message "Job Flink RUNNING encontrado: $flinkJobId"
          }
        }
        catch {
          # O job pode ainda estar iniciando; docker stats continua sendo coletado.
        }
      }

      if ($flinkJobId) {
        $metricsToCollect = @(
          "jvm.memory.heap.used",
          "jvm.memory.heap.max",
          "jvm.gc.count",
          "jvm.gc.time"
        )

        foreach ($metric in $metricsToCollect) {
          try {
            $metricResponse = @(Invoke-RestMethod `
              -Uri "http://localhost:8081/jobs/$flinkJobId/metrics?get=$metric" `
              -TimeoutSec 2 `
              -ErrorAction Stop)

            foreach ($item in $metricResponse) {
              if ($null -ne $item.value -and $item.id) {
                "$timestamp,$($item.id),$($item.value)" |
                  Add-Content -LiteralPath $FlinkMetricsOutputPath -Encoding UTF8
                $flinkSampleCount++
              }
            }
          }
          catch {
            # As metricas do Flink sao complementares; a falha fica registrada ao encerrar.
          }
        }
      }
    }

    Wait-CollectorInterval -Seconds $IntervalSeconds
  }

  Write-CollectorLog -Level INFO -Message "Coletor encerrado: docker_samples=$dockerSampleCount; flink_samples=$flinkSampleCount."
  exit 0
}
catch {
  try {
    Write-CollectorLog -Level ERROR -Message $_.Exception.Message
  }
  catch {}
  [Console]::Error.WriteLine("Falha no coletor de recursos: $($_.Exception.Message)")
  exit 1
}
