param(
  [ValidateRange(1, 9)]
  [int]$Repetition,

  [string]$OutputBase = "scripts/experiments/results-repeated",

  [switch]$Build
)

$ErrorActionPreference = "Stop"

$prefix = "tcc-rep$Repetition"
$env:COMPOSE_PROJECT_NAME = $prefix
$env:PIPELINE_CONTAINER_PREFIX = $prefix
$env:PIPELINE_NETWORK_NAME = "$prefix-net"
$env:PIPELINE_VOLUME_PREFIX = $prefix

function Assert-IsolatedComposeNames {
  $configText = docker compose config --format json
  if ($LASTEXITCODE -ne 0) {
    throw "Falha ao resolver a configuração Docker Compose isolada."
  }
  $config = $configText | ConvertFrom-Json
  $containerNames = @($config.services.PSObject.Properties.Value.container_name)
  $volumeNames = @($config.volumes.PSObject.Properties.Value.name)
  $networkNames = @($config.networks.PSObject.Properties.Value.name)
  $allNames = @($containerNames + $volumeNames + $networkNames | Where-Object { $_ })
  $unexpected = @($allNames | Where-Object { $_ -notlike "$prefix*" })
  if ($unexpected.Count -gt 0) {
    throw "Isolamento recusado; nomes fora do prefixo ${prefix}: $($unexpected -join ', ')"
  }
}

$outputRoot = Join-Path $OutputBase "rep-$Repetition"
$runAll = Join-Path $PSScriptRoot "Run-All-Scenarios.ps1"

Assert-IsolatedComposeNames

try {
  docker compose --profile app down -v --remove-orphans
  if ($LASTEXITCODE -ne 0) {
    throw "Falha ao preparar a stack isolada $prefix."
  }

  if ($Build) {
    & $runAll -OutputRoot $outputRoot -Build
  }
  else {
    & $runAll -OutputRoot $outputRoot
  }
}
finally {
  Assert-IsolatedComposeNames
  docker compose --profile app down -v --remove-orphans
}
