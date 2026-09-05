param(
    [Parameter(Mandatory=$true)][string]$NssmPath,
    [string]$Root = "C:\Program Files\AI-Bybit",
    [string]$EnvFile = "C:\ProgramData\AI-Bybit\executor-production-paper.env"
)
$ErrorActionPreference = "Stop"
$nssm = (Resolve-Path -LiteralPath $NssmPath).Path
$python = Join-Path $Root ".venv\Scripts\python.exe"
$working = Join-Path $Root "BybitContractBotV4"
$entrypoint = Join-Path $working "main.py"
if (-not (Test-Path -LiteralPath $python)) {
    throw "Python runtime not found: $python"
}
if (-not (Test-Path -LiteralPath $entrypoint)) {
    throw "Executor entrypoint not found: $entrypoint"
}
$environment = @(
    Get-Content -LiteralPath $EnvFile |
        Where-Object { $_ -and -not $_.StartsWith("#") -and $_.Contains("=") }
)
$environment += "PYTHONPATH=$Root;$working"
$service = "AIBybitExecutorProductionPaper"
if (Get-Service -Name $service -ErrorAction SilentlyContinue) {
    throw "Service already exists: $service"
}
& $nssm install $service $python "main.py"
& $nssm set $service AppDirectory $working
& $nssm set $service AppEnvironmentExtra $environment
& $nssm set $service AppPriority ABOVE_NORMAL_PRIORITY_CLASS
& $nssm set $service Start SERVICE_AUTO_START
& $nssm set $service AppExit Default Restart
# Apply the documented 2 GiB/CPU Windows Job Object policy before service start.
