param(
    [Parameter(Mandatory=$true)][string]$NssmPath,
    [string]$Root = "C:\Program Files\AI-Bybit",
    [string]$EnvFile = "C:\ProgramData\AI-Bybit\predictor-production-paper.env"
)
$ErrorActionPreference = "Stop"
$nssm = (Resolve-Path -LiteralPath $NssmPath).Path
$python = Join-Path $Root ".venv\Scripts\python.exe"
$working = Join-Path $Root "ai_bot3\ai_bot3"
$environment = @(
    Get-Content -LiteralPath $EnvFile |
        Where-Object { $_ -and -not $_.StartsWith("#") -and $_.Contains("=") }
)
$services = @(
    @{ Name="AIBybitPredictorRealtime"; Script="main_forecast.py"; Priority="HIGH_PRIORITY_CLASS" },
    @{ Name="AIBybitControlPlane"; Script="api\api_server.py"; Priority="ABOVE_NORMAL_PRIORITY_CLASS" },
    @{ Name="AIBybitMarketCollector"; Script="scripts\run_bybit_public_pit_collector.py --database C:\ProgramData\AI-Bybit\market-collector\bybit-public.sqlite3"; Priority="NORMAL_PRIORITY_CLASS" },
    @{ Name="AIBybitPublicationWorker"; Script="scripts\run_publication_worker.py"; Priority="NORMAL_PRIORITY_CLASS" }
)
foreach ($service in $services) {
    if (Get-Service -Name $service.Name -ErrorAction SilentlyContinue) {
        throw "Service already exists: $($service.Name)"
    }
    & $nssm install $service.Name $python $service.Script
    & $nssm set $service.Name AppDirectory $working
    & $nssm set $service.Name AppEnvironmentExtra $environment
    & $nssm set $service.Name AppPriority $service.Priority
    & $nssm set $service.Name Start SERVICE_AUTO_START
    & $nssm set $service.Name AppExit Default Restart
}
# Apply the documented CPU and memory ceilings with a Windows Job Object policy
# before starting services. Training/research/backfill are forbidden on this host.
