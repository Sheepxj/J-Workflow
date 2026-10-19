param(
    [Parameter(Mandatory = $true)][string]$JdbcUrl,
    [Parameter(Mandatory = $true)][string]$JdbcUsername,
    [string]$JdbcPassword = '',
    [Parameter(Mandatory = $true)][ValidateRange(1024, 65535)][int]$Port
)

$ErrorActionPreference = 'Stop'
$verificationRepo = Split-Path $PSScriptRoot -Parent
$verificationJar = Join-Path $verificationRepo 'core-workflow-java/target/workflow-java.jar'
if (-not (Test-Path -LiteralPath $verificationJar)) {
    throw 'Build the Java module before running restart verification.'
}
$verificationOutput = Join-Path $verificationRepo ('.local/workflow-v2-restart-' + [Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $verificationOutput | Out-Null
$verificationBaseUrl = "http://127.0.0.1:$Port"
$verificationJava = (Get-Command java).Source
$verificationFirst = $null
$verificationSecond = $null
$verificationThird = $null

function Start-VerificationJava([string]$Name) {
    $verificationVariables = @{
        WORKFLOW_JDBC_URL = $JdbcUrl
        WORKFLOW_JDBC_USERNAME = $JdbcUsername
        WORKFLOW_JDBC_PASSWORD = $JdbcPassword
        WORKFLOW_PORT = [string]$Port
        WORKFLOW_STATE_STORE = 'jdbc'
        WORKFLOW_INITIALIZE_SCHEMA = 'true'
        WORKFLOW_LINK_ENABLED = 'false'
        WORKFLOW_LOG_FILE = (Join-Path $verificationOutput "$Name.log")
        SPRING_CONFIG_IMPORT = ('optional:file:' + (Join-Path $verificationOutput 'no-secrets.properties').Replace('\', '/'))
    }
    $previousVariables = @{}
    foreach ($entry in $verificationVariables.GetEnumerator()) {
        $previousVariables[$entry.Key] = [Environment]::GetEnvironmentVariable($entry.Key, 'Process')
        [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, 'Process')
    }
    try {
        Start-Process -FilePath $verificationJava -WindowStyle Hidden -PassThru -WorkingDirectory $verificationOutput `
            -ArgumentList @('-Dloader.main=com.iflytek.astron.workflowv2.WorkflowV2Application', '-cp', ('"' + $verificationJar + '"'), 'org.springframework.boot.loader.launch.PropertiesLauncher') `
            -RedirectStandardOutput (Join-Path $verificationOutput "$Name.stdout.log") `
            -RedirectStandardError (Join-Path $verificationOutput "$Name.stderr.log")
    } finally {
        foreach ($entry in $previousVariables.GetEnumerator()) {
            [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, 'Process')
        }
    }
}

function Wait-VerificationReady($Process) {
    $deadline = [DateTime]::UtcNow.AddSeconds(45)
    while ([DateTime]::UtcNow -lt $deadline) {
        $Process.Refresh()
        if ($Process.HasExited) { throw "Verification Java exited with code $($Process.ExitCode); logs: $verificationOutput" }
        try {
            $health = Invoke-RestMethod -Uri "$verificationBaseUrl/actuator/health" -TimeoutSec 2
            if ($health.status -eq 'UP') { return }
        } catch { }
        Start-Sleep -Milliseconds 250
    }
    throw "Verification Java did not become ready; logs: $verificationOutput"
}

function Get-VerificationRun([string]$RunId) {
    Invoke-RestMethod -Uri "$verificationBaseUrl/api/workflow/v2/runs/$RunId" -TimeoutSec 5
}

function Wait-VerificationRun([string]$RunId, [string]$Status, [int]$MinimumGenerations = 0) {
    $deadline = [DateTime]::UtcNow.AddSeconds(30)
    while ([DateTime]::UtcNow -lt $deadline) {
        $run = Get-VerificationRun $RunId
        if ($run.status -eq 'FAILED' -or $run.status -eq 'CANCELLED') {
            throw "Run $RunId ended as $($run.status): $($run.errorCode) $($run.error)"
        }
        $generations = @($run.executions.PSObject.Properties.Value | Where-Object { $_.nodeId -eq 'generate' }).Count
        if ($run.status -eq $Status -and $generations -ge $MinimumGenerations) { return $run }
        Start-Sleep -Milliseconds 100
    }
    throw "Run $RunId did not reach $Status"
}

function Start-VerificationRun($Request) {
    $json = $Request | ConvertTo-Json -Depth 50
    Invoke-RestMethod -Method Post -Uri "$verificationBaseUrl/api/workflow/v2/runs" `
        -ContentType 'application/json; charset=utf-8' -Body ([Text.Encoding]::UTF8.GetBytes($json)) -TimeoutSec 10
}

function Stop-VerificationJava($Process) {
    if ($null -ne $Process) {
        $Process.Refresh()
        if (-not $Process.HasExited) {
            Stop-Process -InputObject $Process -Force
            if (-not $Process.WaitForExit(10000)) { throw 'Verification Java did not stop.' }
        }
    }
}

try {
    $verificationFirst = Start-VerificationJava 'first'
    Wait-VerificationReady $verificationFirst
    $request = Get-Content -LiteralPath (Join-Path $verificationRepo 'core-workflow-java/examples/structured-loop.json') -Raw -Encoding utf8 | ConvertFrom-Json
    $request.definition.nodes[2] | Add-Member -NotePropertyName breakpoint -NotePropertyValue $true -Force
    $created = Start-VerificationRun $request
    $runId = $created.runId
    $before = Wait-VerificationRun $runId 'PAUSED'
    $committed = @($before.executions.PSObject.Properties.Name)
    if ($committed.Count -ne 1) { throw 'Expected only the start node to have committed before the breakpoint.' }
    Stop-VerificationJava $verificationFirst

    $verificationSecond = Start-VerificationJava 'second'
    Wait-VerificationReady $verificationSecond
    $restored = Get-VerificationRun $runId
    if ($restored.status -ne 'PAUSED' -or $restored.executions.PSObject.Properties.Name -notcontains $committed[0]) {
        throw 'Paused checkpoint or committed execution was not preserved after process restart.'
    }
    for ($iteration = 1; $iteration -le 3; $iteration++) {
        Invoke-RestMethod -Method Post -Uri "$verificationBaseUrl/api/workflow/v2/runs/$runId/resume" -TimeoutSec 5 | Out-Null
        $expected = if ($iteration -eq 3) { 'SUCCEEDED' } else { 'PAUSED' }
        $result = Wait-VerificationRun $runId $expected $iteration
    }
    if ($result.outputs.result -ne 'draft-3' -or @($result.executions.PSObject.Properties).Count -ne 8) {
        throw 'Loop execution or committed history differs after restart.'
    }
    if ($result.executions.PSObject.Properties.Name -notcontains $committed[0]) { throw 'The original start execution was replaced.' }

    $parallelRequest = Get-Content -LiteralPath (Join-Path $verificationRepo 'core-workflow-java/examples/parallel.json') -Raw -Encoding utf8 | ConvertFrom-Json
    $parallelCreated = Start-VerificationRun $parallelRequest
    $parallelResult = Wait-VerificationRun $parallelCreated.runId 'SUCCEEDED'
    if ($parallelResult.outputs.left -ne 'L' -or $parallelResult.outputs.right -ne 'R') { throw 'Parallel result mismatch.' }

    $modelProbe = Get-Content -LiteralPath (Join-Path $verificationRepo 'core-workflow-java/examples/structured-loop.json') -Raw -Encoding utf8 | ConvertFrom-Json
    $modelProbe.definition.nodes[2].type = 'LLM'
    $modelProbe.definition.nodes[2] | Add-Member -NotePropertyName breakpoint -NotePropertyValue $true -Force
    $modelCreated = Start-VerificationRun $modelProbe
    $modelPaused = Wait-VerificationRun $modelCreated.runId 'PAUSED'
    if (@($modelPaused.executions.PSObject.Properties.Value | Where-Object { $_.nodeId -eq 'generate' }).Count -ne 0) {
        throw 'Model registration probe must stop before any external model call.'
    }
    Invoke-RestMethod -Method Post -Uri "$verificationBaseUrl/api/workflow/v2/runs/$($modelCreated.runId)/cancel" -TimeoutSec 5 | Out-Null

    $crashRequest = Get-Content -LiteralPath (Join-Path $verificationRepo 'core-workflow-java/examples/structured-loop.json') -Raw -Encoding utf8 | ConvertFrom-Json
    $crashRequest.definition.nodes[2].config | Add-Member -NotePropertyName delayMs -NotePropertyValue 10000 -Force
    $crashRequest.definition.nodes[2].config | Add-Member -NotePropertyName recovery -NotePropertyValue MANUAL -Force
    $crashRequest.definition.nodes[3].config.passesOnIteration = 1
    $crashCreated = Start-VerificationRun $crashRequest
    $crashRunId = $crashCreated.runId
    $crashDeadline = [DateTime]::UtcNow.AddSeconds(10)
    $interrupted = $null
    while ([DateTime]::UtcNow -lt $crashDeadline) {
        $crashRun = Get-VerificationRun $crashRunId
        $interrupted = @($crashRun.executions.PSObject.Properties.Value | Where-Object { $_.nodeId -eq 'generate' -and $_.status -eq 'RUNNING' }) | Select-Object -First 1
        if ($null -ne $interrupted) { break }
        Start-Sleep -Milliseconds 100
    }
    if ($null -eq $interrupted) { throw 'Could not observe the controlled in-flight invocation.' }
    Stop-VerificationJava $verificationSecond
    $verificationThird = Start-VerificationJava 'third'
    Wait-VerificationReady $verificationThird
    $recoveryRun = Wait-VerificationRun $crashRunId 'PAUSED'
    $recoveryExecution = $recoveryRun.executions.PSObject.Properties[$interrupted.executionId].Value
    if ($recoveryExecution.status -ne 'RECOVERY_REQUIRED' -or $recoveryExecution.attempts[0].status -ne 'INTERRUPTED') {
        throw 'Unknown in-flight result was not fenced for explicit recovery.'
    }
    $resolution = @{ action = 'COMPLETE'; outputs = @{ text = 'draft-1' } } | ConvertTo-Json -Depth 5
    Invoke-RestMethod -Method Post -Uri "$verificationBaseUrl/api/workflow/v2/runs/$crashRunId/executions/$($interrupted.executionId)/resolve" `
        -ContentType 'application/json; charset=utf-8' -Body ([Text.Encoding]::UTF8.GetBytes($resolution)) -TimeoutSec 5 | Out-Null
    Invoke-RestMethod -Method Post -Uri "$verificationBaseUrl/api/workflow/v2/runs/$crashRunId/resume" -TimeoutSec 5 | Out-Null
    $crashCompleted = Wait-VerificationRun $crashRunId 'SUCCEEDED' 1
    if ($crashCompleted.outputs.result -ne 'draft-1' -or @($crashCompleted.executions.PSObject.Properties).Count -ne 4) {
        throw 'Explicit recovery did not preserve the original logical execution.'
    }

    $report = [ordered]@{
        verifiedAtUtc = [DateTime]::UtcNow.ToString('o')
        storage = 'JDBC'
        runId = $runId
        statusAfterRestart = $restored.status
        finalStatus = $result.status
        finalOutput = $result.outputs.result
        executionCount = @($result.executions.PSObject.Properties).Count
        originalExecutionPreserved = $true
        firstProcessId = $verificationFirst.Id
        secondProcessId = $verificationSecond.Id
        parallelRunId = $parallelCreated.runId
        parallelStatus = $parallelResult.status
        modelExecutorRegisteredWithoutExternalCall = $true
        thirdProcessId = $verificationThird.Id
        interruptedRunId = $crashRunId
        interruptedExecutionPreserved = $crashCompleted.executions.PSObject.Properties.Name -contains $interrupted.executionId
        interruptedInvocationResolvedExplicitly = $true
    }
    $reportPath = Join-Path $verificationOutput 'report.json'
    $report | ConvertTo-Json | Set-Content -LiteralPath $reportPath -Encoding utf8
    Write-Output "Restart verification passed. Report: $reportPath"
} finally {
    Stop-VerificationJava $verificationThird
    Stop-VerificationJava $verificationSecond
    Stop-VerificationJava $verificationFirst
}
