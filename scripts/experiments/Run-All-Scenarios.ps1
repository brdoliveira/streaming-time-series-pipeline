param(
  [string]$OutputRoot = "results",
  [switch]$Build,
  [int]$ProducerCount = 1,
  [ValidateSet("random", "trend", "burst")]
  [string]$ProducerType = "random"
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$runScenario = Join-Path $scriptDir "Run-Scenario.ps1"

$scenarios = @(
  @{ Name = "low"; Rate = 10; Duration = 300 },
  @{ Name = "medium"; Rate = 100; Duration = 600 },
  @{ Name = "high"; Rate = 1000; Duration = 600 }
)

$first = $true
foreach ($scenario in $scenarios) {
  $argsList = @(
    "-Scenario", $scenario.Name,
    "-RatePerSecond", $scenario.Rate,
    "-DurationSeconds", $scenario.Duration,
    "-OutputRoot", $OutputRoot,
    "-ProducerCount", $ProducerCount,
    "-ProducerType", $ProducerType
  )

  if (-not $first) {
    $argsList += "-SkipStackStart"
  }

  if ($Build -and $first) {
    $argsList += "-Build"
  }

  & $runScenario @argsList
  $first = $false
}

& (Join-Path $scriptDir "Collect-Summary.ps1") -OutputRoot $OutputRoot
