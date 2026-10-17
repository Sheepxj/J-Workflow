$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
Push-Location -LiteralPath $projectRoot
try {
    git config core.hooksPath .githooks
    if ($LASTEXITCODE -ne 0) { throw 'Failed to configure Git hooks.' }
    Write-Host 'Secret checks enabled for commits and pushes.'
} finally {
    Pop-Location
}
