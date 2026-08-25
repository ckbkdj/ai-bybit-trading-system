param(
    [Parameter(Mandatory=$true)][string]$NssmPath,
    [string]$Root = "C:\Program Files\AI-Bybit",
    [string]$EnvFile = "C:\ProgramData\AI-Bybit\executor-testnet.env"
)
$ErrorActionPreference = "Stop"
$approval = "C:\ProgramData\AI-Bybit\TESTNET_HUMAN_APPROVED"
if (-not (Test-Path -LiteralPath $approval)) {
    throw "Testnet is not authorized: approval marker is absent"
}
$nssm = (Resolve-Path -LiteralPath $NssmPath).Path
$python = Join-Path $Root ".venv\Scripts\python.exe"
$working = Join-Path $Root "BybitContractBotV4"
$environment = @(
    Get-Content -LiteralPath $EnvFile |
        Where-Object { $_ -and -not $_.StartsWith("#") -and $_.Contains("=") }
)
$service = "AIBybitExecutorTestnet"
if (Get-Service -Name $service -ErrorAction SilentlyContinue) {
    throw "Service already exists: $service"
}
& $nssm install $service $python "main.py"
& $nssm set $service AppDirectory $working
& $nssm set $service AppEnvironmentExtra $environment
& $nssm set $service AppPriority ABOVE_NORMAL_PRIORITY_CLASS
& $nssm set $service Start SERVICE_DEMAND_START
# Installation does not start the service. This task does not authorize testnet.
