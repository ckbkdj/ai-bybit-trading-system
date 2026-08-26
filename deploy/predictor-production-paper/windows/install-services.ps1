param(
    [Parameter(Mandatory=$true)][string]$NssmPath,
    [string]$Root = "C:\Program Files\AI-Bybit",
    [string]$EnvFile = "C:\ProgramData\AI-Bybit\predictor-production-paper.env"
)
$ErrorActionPreference = "Stop"
$nssm = (Resolve-Path -LiteralPath $NssmPath).Path
$python = Join-Path $Root ".venv\Scripts\python.exe"
$working = Join-Path $Root "ai_bot3\ai_bot3"
if (-not (Test-Path -LiteralPath $python)) {
    throw "Python runtime not found: $python"
}
foreach ($required in @(
    "main_forecast.py",
    "scripts\run_bybit_public_pit_collector.py",
    "scripts\run_publication_worker.py"
)) {
    $target = Join-Path $working $required
    if (-not (Test-Path -LiteralPath $target)) {
        throw "Predictor entrypoint not found: $target"
    }
}
$environment = @(
    Get-Content -LiteralPath $EnvFile |
        Where-Object { $_ -and -not $_.StartsWith("#") -and $_.Contains("=") }
)
$environment += "PYTHONPATH=$Root;$working"
$services = @(
    @{ Name="AIBybitPredictorRealtime"; Arguments="main_forecast.py"; Priority="HIGH_PRIORITY_CLASS" },
    @{ Name="AIBybitControlPlane"; Arguments="-m uvicorn api.control_plane_main:app --host 127.0.0.1 --port 8000"; Priority="ABOVE_NORMAL_PRIORITY_CLASS" },
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
