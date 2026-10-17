param(
    [Parameter(Mandatory = $true)][int]$ProcessId,
    [Parameter(Mandatory = $true)][string]$OutputPath,
    [int]$DurationSeconds = 90
)

"timestamp,cpu_seconds,working_set_bytes,private_bytes,threads,handles,jvm_threads_live,jvm_threads_peak,tomcat_busy,tomcat_current,heap_used_bytes,process_cpu,system_cpu,workflow_active,workflow_executor_active,workflow_executor_pool,workflow_executor_max,workflow_rejected,sse_connections,workflow_stream_vt,sse_vt,sse_backpressure" | Set-Content -LiteralPath $OutputPath
$end = (Get-Date).AddSeconds($DurationSeconds)
while ((Get-Date) -lt $end) {
    $process = Get-Process -Id $ProcessId -ErrorAction SilentlyContinue
    if (-not $process) { break }
    $metrics = @{}
    foreach ($name in @('jvm.threads.live','jvm.threads.peak','tomcat.threads.busy','tomcat.threads.current','jvm.memory.used','process.cpu.usage','system.cpu.usage','workflow.executions.active','workflow.executor.active','workflow.executor.pool.size','workflow.executor.maximum','workflow.executor.rejected','sse.connections.active','workflow.stream.virtual.threads','sse.virtual.threads','sse.queue.backpressure')) {
        try {
            $uri = "http://localhost:7880/actuator/metrics/$name"
            if ($name -eq 'jvm.memory.used') { $uri += '?tag=area:heap' }
            $body = Invoke-RestMethod -Uri $uri -TimeoutSec 2
            $metrics[$name] = ($body.measurements | Where-Object statistic -eq 'VALUE').value
        } catch {
            $metrics[$name] = ''
        }
    }
    $line = @(
        (Get-Date -Format o), $process.CPU, $process.WorkingSet64, $process.PrivateMemorySize64,
        $process.Threads.Count, $process.HandleCount, $metrics['jvm.threads.live'], $metrics['jvm.threads.peak'],
        $metrics['tomcat.threads.busy'], $metrics['tomcat.threads.current'], $metrics['jvm.memory.used'],
        $metrics['process.cpu.usage'], $metrics['system.cpu.usage'], $metrics['workflow.executions.active'],
        $metrics['workflow.executor.active'], $metrics['workflow.executor.pool.size'], $metrics['workflow.executor.maximum'],
        $metrics['workflow.executor.rejected'], $metrics['sse.connections.active'], $metrics['workflow.stream.virtual.threads'],
        $metrics['sse.virtual.threads'], $metrics['sse.queue.backpressure']
    ) -join ','
    Add-Content -LiteralPath $OutputPath -Value $line
    Start-Sleep -Seconds 1
}
