param(
    [Parameter(Mandatory = $true)]
    [string]$JMeterHome,
    [string]$TargetHost = "localhost",
    [int]$TargetPort = 7880,
    [int]$DurationSeconds = 300,
    [int]$RampUpSeconds = 30,
    [int[]]$ThreadCounts = @(10, 20, 30, 40, 50)
)

$ErrorActionPreference = "Stop"
$loadTestDir = $PSScriptRoot
$jmeter = Join-Path $JMeterHome "bin\jmeter.bat"
if (-not (Test-Path -LiteralPath $jmeter)) {
    throw "jmeter.bat not found: $jmeter"
}

$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$resultRoot = Join-Path $loadTestDir "results\$timestamp"
New-Item -ItemType Directory -Path $resultRoot -Force | Out-Null

foreach ($threads in $ThreadCounts) {
    $plan = Join-Path $loadTestDir "mock-workflow-$threads-threads.jmx"
    $jtl = Join-Path $resultRoot "mock-workflow-$threads-threads.jtl"
    $report = Join-Path $resultRoot "report-$threads-threads"

    & $jmeter -n -t $plan "-Jhost=$TargetHost" "-Jport=$TargetPort" "-Jduration=$DurationSeconds" "-JrampUp=$RampUpSeconds" -l $jtl -e -o $report
    if ($LASTEXITCODE -ne 0) {
        throw "JMeter test failed for $threads threads with exit code $LASTEXITCODE"
    }
}

Write-Host "All reports generated under: $resultRoot"
