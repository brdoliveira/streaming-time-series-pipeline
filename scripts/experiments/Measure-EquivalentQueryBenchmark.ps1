param(
  [ValidateRange(3, 100)]
  [int]$Repetitions = 10,

  [ValidateRange(0, 20)]
  [int]$WarmupRuns = 2,

  [ValidateRange(1, 168)]
  [int]$WindowHours = 24,

  [string]$OutputRoot = "results-query-benchmark",
  [string]$PostgresUser = "pipeline",
  [string]$PostgresDb = "pipeline"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectDir = (Resolve-Path (Join-Path $scriptDir "..\..")).Path
$queryPath = Join-Path $projectDir "infra\timescaledb\queries\benchmark-equivalent-queries.sql"
$outputDir = Join-Path $scriptDir $OutputRoot
$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$resultPath = Join-Path $outputDir "$timestamp-equivalent-query-benchmark.csv"
$metadataPath = Join-Path $outputDir "$timestamp-equivalent-query-benchmark.json"

function Get-QueryBlock {
  param(
    [Parameter(Mandatory)] [string]$Sql,
    [Parameter(Mandatory)] [ValidateSet("RAW_QUERY", "CAGG_QUERY")] [string]$Name
  )

  $pattern = "(?s)-- BEGIN $Name\s*(.*?)\s*-- END $Name"
  $match = [regex]::Match($Sql, $pattern)
  if (-not $match.Success) {
    throw "Bloco $Name nao encontrado em $queryPath"
  }
  return $match.Groups[1].Value.Trim()
}

function Invoke-Psql {
  param([Parameter(Mandatory)] [string]$Sql)

  $previousErrorActionPreference = $ErrorActionPreference
  try {
    $ErrorActionPreference = "Continue"
    $output = & docker compose exec -T timescaledb psql -X -q -t -A -F "|" `
      -v ON_ERROR_STOP=1 -U $PostgresUser -d $PostgresDb -c $Sql 2>&1
    $exitCode = $LASTEXITCODE
  }
  finally {
    $ErrorActionPreference = $previousErrorActionPreference
  }
  if ($exitCode -ne 0) {
    throw "psql falhou (exit $exitCode): $($output -join [Environment]::NewLine)"
  }
  $resultLines = $output | Where-Object { $_ -isnot [System.Management.Automation.ErrorRecord] }
  return (($resultLines | ForEach-Object { $_.ToString().TrimEnd() }) -join "`n").Trim()
}

function Invoke-MeasuredQuery {
  param(
    [Parameter(Mandatory)] [ValidateSet("raw", "cagg")] [string]$QueryType,
    [Parameter(Mandatory)] [string]$Sql
  )

  $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
  $result = Invoke-Psql -Sql $Sql
  $stopwatch.Stop()
  $bytes = [System.Text.Encoding]::UTF8.GetBytes($result)
  $sha256 = [System.Security.Cryptography.SHA256]::Create()
  try {
    $hashBytes = $sha256.ComputeHash($bytes)
  }
  finally {
    $sha256.Dispose()
  }
  $hash = [BitConverter]::ToString($hashBytes).Replace("-", "").ToLowerInvariant()
  $rowCount = if ([string]::IsNullOrWhiteSpace($result)) { 0 } else { ($result -split "`n").Count }

  return [pscustomobject]@{
    QueryType = $QueryType
    ElapsedMs = [math]::Round($stopwatch.Elapsed.TotalMilliseconds, 3)
    RowCount = $rowCount
    ResultHash = $hash
    Result = $result
  }
}

if (-not (Test-Path -LiteralPath $queryPath)) {
  throw "Arquivo de queries nao encontrado: $queryPath"
}

New-Item -ItemType Directory -Path $outputDir -Force | Out-Null
Push-Location $projectDir
try {
  & docker compose config --quiet
  if ($LASTEXITCODE -ne 0) { throw "docker compose config falhou" }

  $now = [DateTimeOffset]::UtcNow
  $alignedMinute = [math]::Floor($now.Minute / 15) * 15
  $windowEnd = [DateTimeOffset]::new($now.Year, $now.Month, $now.Day, $now.Hour, $alignedMinute, 0, [TimeSpan]::Zero)
  $windowStart = $windowEnd.AddHours(-$WindowHours)
  $startIso = $windowStart.ToString("yyyy-MM-ddTHH:mm:ssZ")
  $endIso = $windowEnd.ToString("yyyy-MM-ddTHH:mm:ssZ")

  $queryFile = Get-Content -LiteralPath $queryPath -Raw
  $rawSql = (Get-QueryBlock -Sql $queryFile -Name "RAW_QUERY").Replace("__WINDOW_START__", $startIso).Replace("__WINDOW_END__", $endIso)
  $caggSql = (Get-QueryBlock -Sql $queryFile -Name "CAGG_QUERY").Replace("__WINDOW_START__", $startIso).Replace("__WINDOW_END__", $endIso)

  Invoke-Psql -Sql "CALL refresh_continuous_aggregate('cagg_events_1min', TIMESTAMPTZ '$startIso', TIMESTAMPTZ '$endIso');" | Out-Null
  Invoke-Psql -Sql "CALL refresh_continuous_aggregate('cagg_events_15min', TIMESTAMPTZ '$startIso', TIMESTAMPTZ '$endIso');" | Out-Null

  $rawReference = Invoke-MeasuredQuery -QueryType raw -Sql $rawSql
  $caggReference = Invoke-MeasuredQuery -QueryType cagg -Sql $caggSql
  if ($rawReference.ResultHash -ne $caggReference.ResultHash) {
    throw "Resultados nao equivalentes: raw rows/hash=$($rawReference.RowCount)/$($rawReference.ResultHash), cagg rows/hash=$($caggReference.RowCount)/$($caggReference.ResultHash)"
  }

  for ($warmup = 1; $warmup -le $WarmupRuns; $warmup++) {
    Invoke-MeasuredQuery -QueryType raw -Sql $rawSql | Out-Null
    Invoke-MeasuredQuery -QueryType cagg -Sql $caggSql | Out-Null
  }

  $measurements = [System.Collections.Generic.List[object]]::new()
  $sequence = 0
  for ($repetition = 1; $repetition -le $Repetitions; $repetition++) {
    $order = if (($repetition % 2) -eq 1) { @("raw", "cagg") } else { @("cagg", "raw") }
    foreach ($queryType in $order) {
      $sequence++
      $sql = if ($queryType -eq "raw") { $rawSql } else { $caggSql }
      $measurement = Invoke-MeasuredQuery -QueryType $queryType -Sql $sql
      $measurements.Add([pscustomobject]@{
          sequence = $sequence
          repetition = $repetition
          query_type = $queryType
          elapsed_ms = $measurement.ElapsedMs.ToString("0.000", [Globalization.CultureInfo]::InvariantCulture)
          row_count = $measurement.RowCount
          result_hash = $measurement.ResultHash
          window_start_utc = $startIso
          window_end_utc = $endIso
          measured_at_utc = [DateTimeOffset]::UtcNow.ToString("o")
        })
    }
  }

  $measurements | Export-Csv -LiteralPath $resultPath -NoTypeInformation -Encoding UTF8
  [ordered]@{
    generated_at_utc = [DateTimeOffset]::UtcNow.ToString("o")
    repetitions = $Repetitions
    warmup_runs_per_query = $WarmupRuns
    window_hours = $WindowHours
    window_start_utc = $startIso
    window_end_utc = $endIso
    equivalent_row_count = $rawReference.RowCount
    equivalent_result_hash = $rawReference.ResultHash
    results_csv = $resultPath
  } | ConvertTo-Json | Set-Content -LiteralPath $metadataPath -Encoding UTF8

  Write-Host "Benchmark concluido com resultados equivalentes."
  Write-Host "Medicoes: $resultPath"
  Write-Host "Metadados: $metadataPath"
}
finally {
  Pop-Location
}
