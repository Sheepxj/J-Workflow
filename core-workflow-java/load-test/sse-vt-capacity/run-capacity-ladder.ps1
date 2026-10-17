param(
    [Parameter(Mandatory = $true)][string]$ResultDirectory,
    [Parameter(Mandatory = $true)][int]$JavaProcessId,
    [string]$JMeterHome = 'E:\Jmeter\apache-jmeter-5.6.3',
    [int[]]$ThreadCounts = @(5, 10, 20, 24, 25, 26, 30, 40, 50),
    [int]$DurationSeconds = 30,
    [int]$RampUpSeconds = 5,
    [int]$ThinkTimeMs = 250
)

$ErrorActionPreference = 'Stop'
$testPlan = Join-Path $PSScriptRoot 'capacity-workflow.jmx'
$summarizer = Join-Path $PSScriptRoot 'summarize-jtl.ps1'
$monitor = Join-Path $PSScriptRoot 'monitor-java.ps1'
$jmeter = Join-Path $JMeterHome 'bin\jmeter.bat'
$summaryPath = Join-Path $ResultDirectory 'capacity-summary.jsonl'

foreach ($threads in $ThreadCounts) {
    $stage = "capacity-$threads"
    $jtl = Join-Path $ResultDirectory "$stage.jtl"
    $jmeterLog = Join-Path $ResultDirectory "$stage-jmeter.log"
    $monitorPath = Join-Path $ResultDirectory "$stage-monitor.csv"
    $monitorArgs = @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $monitor, '-ProcessId', $JavaProcessId, '-OutputPath', $monitorPath, '-DurationSeconds', ($DurationSeconds + 10))
    $monitorProcess = Start-Process -FilePath 'powershell.exe' -ArgumentList $monitorArgs -WindowStyle Hidden -PassThru
    $dumpPath = Join-Path $ResultDirectory "$stage-threads.json"
    $dumpJob = Start-Job -ScriptBlock {
        param($PidToDump, $PathToDump, $Delay)
        Start-Sleep -Seconds $Delay
        & 'D:\JDK\JDK21\bin\jcmd.exe' $PidToDump Thread.dump_to_file -overwrite -format=json $PathToDump
    } -ArgumentList $JavaProcessId, $dumpPath, ([Math]::Max(3, [Math]::Floor($DurationSeconds / 2)))

    & $jmeter -n -t $testPlan "-Jthreads=$threads" "-JrampUp=$RampUpSeconds" "-Jduration=$DurationSeconds" "-JthinkTime=$ThinkTimeMs" '-JflowId=826202600000000001' -l $jtl -j $jmeterLog
    if ($LASTEXITCODE -ne 0) {
        throw "JMeter failed in $stage with exit code $LASTEXITCODE"
    }
    Wait-Job -Job $dumpJob | Out-Null
    Receive-Job -Job $dumpJob | Out-Null
    Remove-Job -Job $dumpJob
    if (-not $monitorProcess.HasExited) {
        Wait-Process -Id $monitorProcess.Id -Timeout 15 -ErrorAction SilentlyContinue
    }
    $summary = & $summarizer -Path $jtl | ConvertFrom-Json
    $summary | Add-Member -NotePropertyName Threads -NotePropertyValue $threads
    $summary | ConvertTo-Json -Compress | Tee-Object -FilePath $summaryPath -Append
    Start-Sleep -Seconds 5
}
