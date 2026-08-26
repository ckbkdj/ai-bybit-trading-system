param(
    [Parameter(Mandatory=$true)][ValidateSet('predictor','executor','lab')][string]$Role,
    [ValidateSet('up','down','config','logs')][string]$Action = 'up'
)
$ErrorActionPreference = 'Stop'
$Root = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$Compose = Join-Path $PSScriptRoot "$Role.compose.yml"
if ($Role -eq 'predictor') { $Compose = Join-Path $PSScriptRoot 'predictor.compose.yml' }
if ($Role -eq 'executor') { $Compose = Join-Path $PSScriptRoot 'executor.compose.yml' }
if ($Role -eq 'lab') { $Compose = Join-Path $PSScriptRoot 'shadow-lab.compose.yml' }
& docker compose -f $Compose config | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'docker compose config failed' }
switch ($Action) {
    'config' { & docker compose -f $Compose config }
    'up' {
        if ($Role -eq 'predictor') {
            & docker compose -f $Compose --profile full --profile ops up -d --build
        } else {
            & docker compose -f $Compose up -d --build
        }
    }
    'down' { & docker compose -f $Compose down }
    'logs' { & docker compose -f $Compose logs -f --tail 200 }
}
if ($LASTEXITCODE -ne 0) { throw "docker compose $Action failed" }
