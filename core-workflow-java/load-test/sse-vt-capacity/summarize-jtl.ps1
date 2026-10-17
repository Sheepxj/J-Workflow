param([Parameter(Mandatory = $true)][string]$Path)

$rows = Import-Csv -LiteralPath $Path
if (-not $rows) {
    throw "No samples found in $Path"
}

function Get-Percentile([double[]]$Values, [double]$Percentile) {
    $sorted = $Values | Sort-Object
    $index = [Math]::Ceiling($Percentile * $sorted.Count) - 1
    return [double]$sorted[[Math]::Max(0, $index)]
}

$elapsed = [double[]]($rows | ForEach-Object { [double]$_.elapsed })
$latency = [double[]]($rows | ForEach-Object { [double]$_.Latency })
$success = @($rows | Where-Object success -eq 'true').Count
$start = ($rows | Measure-Object -Property timeStamp -Minimum).Minimum
$end = ($rows | ForEach-Object { [double]$_.timeStamp + [double]$_.elapsed } | Measure-Object -Maximum).Maximum
$wallSeconds = [Math]::Max(0.001, ($end - $start) / 1000)
$bytes = ($rows | Measure-Object -Property bytes -Sum).Sum

[pscustomobject]@{
    Samples = $rows.Count
    Success = $success
    Failure = $rows.Count - $success
    SuccessRatePct = [Math]::Round(100 * $success / $rows.Count, 3)
    QPS = [Math]::Round($rows.Count / $wallSeconds, 3)
    P50Ms = Get-Percentile $elapsed 0.50
    P95Ms = Get-Percentile $elapsed 0.95
    P99Ms = Get-Percentile $elapsed 0.99
    FirstByteP50Ms = Get-Percentile $latency 0.50
    FirstByteP95Ms = Get-Percentile $latency 0.95
    FirstByteP99Ms = Get-Percentile $latency 0.99
    MiBPerSecond = [Math]::Round(([double]$bytes / 1MB) / $wallSeconds, 3)
} | ConvertTo-Json -Compress
