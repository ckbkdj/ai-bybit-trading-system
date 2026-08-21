[CmdletBinding(PositionalBinding = $false)]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]] $GitArguments
)

$taskRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$taskGitDirectory = Join-Path $taskRoot '.version-history'

if (-not (Test-Path -LiteralPath $taskGitDirectory)) {
    throw "Local version history has not been initialized: $taskGitDirectory"
}

& git --git-dir=$taskGitDirectory --work-tree=$taskRoot @GitArguments
exit $LASTEXITCODE
