param(
    [Parameter(Mandatory=$true)][string]$NssmPath,
    [string]$Root = "C:\Program Files\AI-Bybit",
    [string]$EnvFile = "C:\ProgramData\AI-Bybit\predictor-production-paper.env"
)
$ErrorActionPreference = "Stop"
$nssm = (Resolve-Path -LiteralPath $NssmPath).Path
$python = Join-Path $Root ".venv\Scripts\python.exe"
$working = Join-Path $Root "ai_bot3\ai_bot3"
$preflight = Join-Path $working "scripts\preflight_production_predictor.py"

foreach ($required in @(
    $python,
    $working,
    $EnvFile,
    (Join-Path $Root "shadow_contracts"),
    $preflight,
    (Join-Path $working "main_forecast.py"),
    (Join-Path $working "api\control_plane_server.py"),
    (Join-Path $working "scripts\run_bybit_public_pit_collector.py"),
    (Join-Path $working "scripts\run_publication_worker.py")
)) {
    if (-not (Test-Path -LiteralPath $required)) {
        throw "Required predictor deployment path is missing: $required"
    }
}

$environment = @(
    Get-Content -LiteralPath $EnvFile |
        Where-Object {
            $_ -and
            -not $_.StartsWith("#") -and
            $_.Contains("=") -and
            -not $_.StartsWith("PYTHONPATH=")
        }
)
$environment += "PYTHONPATH=$Root;$working"

# Apply the same fail-closed preflight used by systemd and Docker before NSSM
# registers any service. The preflight opens no exchange or control-plane socket.
$oldEnvironment = @{}
foreach ($line in $environment) {
    $key, $value = $line -split "=", 2
    $oldEnvironment[$key] = [Environment]::GetEnvironmentVariable($key, "Process")
    [Environment]::SetEnvironmentVariable($key, $value, "Process")
}
try {
    & $python $preflight
    if ($LASTEXITCODE -ne 0) {
        throw "Predictor preflight failed with exit code $LASTEXITCODE"
    }
}
finally {
    foreach ($item in $oldEnvironment.GetEnumerator()) {
        [Environment]::SetEnvironmentVariable($item.Key, $item.Value, "Process")
    }
}

$services = @(
    @{ Name="AIBybitPredictorRealtime"; Arguments="main_forecast.py"; Priority="HIGH_PRIORITY_CLASS" },
    @{ Name="AIBybitControlPlane"; Arguments="api\control_plane_server.py"; Priority="ABOVE_NORMAL_PRIORITY_CLASS" },
    @{ Name="AIBybitMarketCollector"; Arguments="scripts\run_bybit_public_pit_collector.py --database C:\ProgramData\AI-Bybit\market-collector\bybit-public.sqlite3"; Priority="NORMAL_PRIORITY_CLASS" },
    @{ Name="AIBybitPublicationWorker"; Arguments="scripts\run_publication_worker.py"; Priority="NORMAL_PRIORITY_CLASS" }
)
foreach ($service in $services) {
    if (Get-Service -Name $service.Name -ErrorAction SilentlyContinue) {
        throw "Service already exists: $($service.Name)"
    }
    & $nssm install $service.Name $python $service.Arguments
    & $nssm set $service.Name AppDirectory $working
    & $nssm set $service.Name AppEnvironmentExtra $environment
    & $nssm set $service.Name AppPriority $service.Priority
    & $nssm set $service.Name Start SERVICE_AUTO_START
    & $nssm set $service.Name AppExit Default Restart
}
# Apply the documented CPU and memory ceilings with a Windows Job Object policy
# before starting services. Training/research/backfill are forbidden on this host.
