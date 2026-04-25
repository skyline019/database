param(
    [string]$DemoExe = "",
    [string]$DataDir = "e:\temp\newdb_test_isolation",
    [int[]]$Sizes = @(100000, 500000, 1000000),
    [string]$SizesCsv = "",
    [int]$QueryWarmupLoops = 1,
    [int]$QueryLoops = 3,
    [int]$TxnPerMode = 60,
    [int]$BuildChunkSize = 20000
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$scriptsRoot = $PSScriptRoot
$projectRoot = Split-Path -Parent (Split-Path -Parent $scriptsRoot)

if ([string]::IsNullOrWhiteSpace($DemoExe)) {
    $exeName = if ($env:OS -eq "Windows_NT") { "newdb_demo.exe" } else { "newdb_demo" }
    $DemoExe = Join-Path (Join-Path $projectRoot "build_mingw") $exeName
} elseif (-not [System.IO.Path]::IsPathRooted($DemoExe)) {
    $DemoExe = Join-Path $projectRoot $DemoExe
}

if (-not (Test-Path $DemoExe)) {
    throw "DemoExe not found: $DemoExe"
}
if ($BuildChunkSize -lt 1000) {
    throw "BuildChunkSize must be >= 1000"
}
if (-not [string]::IsNullOrWhiteSpace($SizesCsv)) {
    $Sizes = @($SizesCsv.Split(",") | ForEach-Object { [int]($_.Trim()) })
}
$baseDir = $DataDir
if (-not (Test-Path $baseDir)) {
    New-Item -Path $baseDir -ItemType Directory | Out-Null
}
$leafName = [System.IO.Path]::GetFileName($baseDir.TrimEnd('\','/'))
if ($leafName -match "^run_\d{8}_\d{6}$") {
    $DataDir = $baseDir
} else {
    $runStamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $DataDir = Join-Path $baseDir ("run_" + $runStamp)
    New-Item -Path $DataDir -ItemType Directory -Force | Out-Null
}
Write-Host ("Million-scale bench data dir: {0}" -f $DataDir)

$queryCommands = @(
    "WHERE(dept,=,ENG,AND,age,>,20)",
    "COUNT(dept,=,ENG,AND,age,>,20)",
    "AVG(salary,WHERE,dept,=,ENG,AND,age,>,20)"
)
$walModes = @("full", "normal", "off")

$outDir = Join-Path $PSScriptRoot "results"
if (-not (Test-Path $outDir)) {
    New-Item -Path $outDir -ItemType Directory | Out-Null
}
$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$resultJson = Join-Path $outDir ("million_scale_bench_" + $stamp + ".json")
$resultCsv = Join-Path $outDir ("million_scale_bench_" + $stamp + ".csv")
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)

function Invoke-DemoRunMdb([string]$scriptPath, [string]$tableArg = "") {
    $logName = "__scale_tmp_" + [guid]::NewGuid().ToString("N") + ".bin"
    $args = @("--data-dir", $DataDir, "--log-file", $logName)
    if (-not [string]::IsNullOrWhiteSpace($tableArg)) {
        $args += @("--table", $tableArg)
    }
    $args += @("--run-mdb", $scriptPath)
    $out = & $DemoExe @args 2>&1
    $tmpLogPath = Join-Path $DataDir $logName
    if (Test-Path $tmpLogPath) {
        Remove-Item -Force $tmpLogPath -ErrorAction SilentlyContinue
    }
    return $out
}

function Invoke-DemoExec([string]$table, [string]$cmd) {
    $logName = "__scale_tmp_" + [guid]::NewGuid().ToString("N") + ".bin"
    $args = @("--data-dir", $DataDir, "--table", $table, "--log-file", $logName, "--exec-line", $cmd)
    $out = & $DemoExe @args 2>&1
    $tmpLogPath = Join-Path $DataDir $logName
    if (Test-Path $tmpLogPath) {
        Remove-Item -Force $tmpLogPath -ErrorAction SilentlyContinue
    }
    return $out
}

function Invoke-DemoRunMdbLazy([string]$scriptPath, [string]$tableArg = "") {
    $oldLazy = $env:NEWDB_LAZY_HEAP
    $env:NEWDB_LAZY_HEAP = "1"
    try {
        return Invoke-DemoRunMdb -scriptPath $scriptPath -tableArg $tableArg
    } finally {
        if ($null -eq $oldLazy) {
            Remove-Item Env:NEWDB_LAZY_HEAP -ErrorAction SilentlyContinue
        } else {
            $env:NEWDB_LAZY_HEAP = $oldLazy
        }
    }
}

function New-BaseTable([string]$table) {
    $scriptPath = Join-Path $env:TEMP ("scale_init_" + $table + ".mdb")
    $sw = [System.IO.StreamWriter]::new($scriptPath, $false, $utf8NoBom)
    try {
        $sw.WriteLine("CREATE TABLE($table)")
        $sw.WriteLine("USE($table)")
        $sw.WriteLine("DEFATTR(name:string,dept:string,age:int,salary:int)")
    } finally {
        $sw.Dispose()
    }
    Invoke-DemoRunMdb -scriptPath $scriptPath | Out-Null
    Remove-Item -Force $scriptPath
}

function Append-Rows([string]$table, [int]$startId, [int]$endId) {
    $totalRows = $endId - $startId + 1
    $scriptPath = Join-Path $env:TEMP ("scale_append_" + $table + "_" + $startId + "_" + $endId + ".mdb")
    $sw = [System.IO.StreamWriter]::new($scriptPath, $false, $utf8NoBom)
    $doneRows = 0
    try {
        $sw.WriteLine("USE($table)")
        for ($chunkStart = $startId; $chunkStart -le $endId; $chunkStart += $BuildChunkSize) {
            $chunkEnd = [math]::Min($chunkStart + $BuildChunkSize - 1, $endId)
            $chunkCount = $chunkEnd - $chunkStart + 1
            $sw.WriteLine(("BULKINSERTFAST({0},{1})" -f $chunkStart, $chunkCount))

            $doneRows += $chunkCount
            $pct = [int](($doneRows * 100.0) / [math]::Max($totalRows, 1))
            Write-Host ("  [build-plan] {0}/{1} ({2}%)" -f $doneRows, $totalRows, $pct)
            Write-Progress -Id 2 -Activity "Build dataset rows" -Status "$doneRows / $totalRows (planned)" -PercentComplete $pct
        }
    } finally {
        $sw.Dispose()
    }
    $timer = [System.Diagnostics.Stopwatch]::StartNew()
    Invoke-DemoRunMdb -scriptPath $scriptPath | Out-Null
    $timer.Stop()
    Remove-Item -Force $scriptPath
    Write-Progress -Id 2 -Activity "Build dataset rows" -Completed
    return [math]::Round($timer.Elapsed.TotalSeconds, 3)
}

function Run-QueryBench([string]$table, [int]$rows) {
    $records = @()
    $warmPath = Join-Path $env:TEMP ("scale_query_warm_" + $table + ".mdb")
    $swWarm = [System.IO.StreamWriter]::new($warmPath, $false, $utf8NoBom)
    try {
        $swWarm.WriteLine("USE($table)")
        for ($ci = 0; $ci -lt $queryCommands.Count; $ci++) {
            $cmd = $queryCommands[$ci]
            $cmdPct = [int](($ci * 100.0) / [math]::Max($queryCommands.Count, 1))
            Write-Progress -Id 3 -Activity "Query benchmark" -Status ("warmup: " + $cmd) -PercentComplete $cmdPct
            for ($w = 0; $w -lt $QueryWarmupLoops; $w++) {
                $swWarm.WriteLine($cmd)
            }
        }
    } finally {
        $swWarm.Dispose()
    }
    Invoke-DemoRunMdbLazy -scriptPath $warmPath -tableArg $table | Out-Null
    Remove-Item -Force $warmPath

    for ($ci = 0; $ci -lt $queryCommands.Count; $ci++) {
        $cmd = $queryCommands[$ci]
        $cmdPct = [int](($ci * 100.0) / [math]::Max($queryCommands.Count, 1))
        Write-Progress -Id 3 -Activity "Query benchmark" -Status $cmd -PercentComplete $cmdPct
        $runPath = Join-Path $env:TEMP ("scale_query_run_" + $table + "_" + $ci + ".mdb")
        $swRun = [System.IO.StreamWriter]::new($runPath, $false, $utf8NoBom)
        try {
            $swRun.WriteLine("USE($table)")
            for ($i = 0; $i -lt $QueryLoops; $i++) {
                $swRun.WriteLine($cmd)
            }
        } finally {
            $swRun.Dispose()
        }
        $timer = [System.Diagnostics.Stopwatch]::StartNew()
        Invoke-DemoRunMdbLazy -scriptPath $runPath -tableArg $table | Out-Null
        $timer.Stop()
        Remove-Item -Force $runPath
        $records += [pscustomobject]@{
            rows = $rows
            table = $table
            phase = "query"
            metric = $cmd
            total_ms = [math]::Round($timer.Elapsed.TotalMilliseconds, 3)
            avg_ms = [math]::Round($timer.Elapsed.TotalMilliseconds / [math]::Max($QueryLoops, 1), 3)
            tps = $null
        }
    }
    Write-Progress -Id 3 -Activity "Query benchmark" -Completed
    return ,$records
}

function New-TxnBenchTable([string]$table) {
    $scriptPath = Join-Path $env:TEMP ("scale_txn_init_" + $table + ".mdb")
    $sw = [System.IO.StreamWriter]::new($scriptPath, $false, $utf8NoBom)
    try {
        $sw.WriteLine("CREATE TABLE($table)")
        $sw.WriteLine("USE($table)")
        $sw.WriteLine("DEFATTR(name:string,dept:string,age:int,salary:int)")
    } finally {
        $sw.Dispose()
    }
    Invoke-DemoRunMdb -scriptPath $scriptPath | Out-Null
    Remove-Item -Force $scriptPath
}

function Run-TxnBench([string]$table, [int]$rows) {
    $records = @()
    $txnTable = $table + "_txn"
    New-TxnBenchTable -table $txnTable
    for ($mi = 0; $mi -lt $walModes.Count; $mi++) {
        $mode = $walModes[$mi]
        $modePct = [int](($mi * 100.0) / [math]::Max($walModes.Count, 1))
        Write-Progress -Id 4 -Activity "Txn benchmark" -Status ("WALSYNC " + $mode) -PercentComplete $modePct
        $scriptPath = Join-Path $env:TEMP ("scale_txn_" + $txnTable + "_" + $mode + ".mdb")
        $lines = New-Object System.Collections.Generic.List[string]
        $lines.Add("USE($txnTable)")
        $lines.Add("WALSYNC $mode")
        $baseId = [int](Get-Date -UFormat %s)
        $baseId = ($baseId * 1000000) + ($rows * 10)
        if ($mode -eq "normal") { $baseId += 200000000 }
        elseif ($mode -eq "off") { $baseId += 400000000 }
        for ($i = 0; $i -lt $TxnPerMode; $i++) {
            $id = $baseId + $i
            $lines.Add("BEGIN")
            $lines.Add("INSERT($id,txn_$mode`_$i,ENG,30,10000)")
            $lines.Add("COMMIT")
        }
        $sw = [System.IO.StreamWriter]::new($scriptPath, $false, $utf8NoBom)
        try {
            foreach ($line in $lines) {
                $sw.WriteLine($line)
            }
        } finally {
            $sw.Dispose()
        }

        $timer = [System.Diagnostics.Stopwatch]::StartNew()
        Invoke-DemoRunMdb -scriptPath $scriptPath -tableArg $txnTable | Out-Null
        $timer.Stop()
        Remove-Item -Force $scriptPath
        $totalMs = [math]::Round($timer.Elapsed.TotalMilliseconds, 3)
        $records += [pscustomobject]@{
            rows = $rows
            table = $table
            phase = "txn"
            metric = ("WALSYNC " + $mode)
            total_ms = $totalMs
            avg_ms = [math]::Round($totalMs / [math]::Max($TxnPerMode, 1), 3)
            tps = [math]::Round(($TxnPerMode * 1000.0) / [math]::Max($totalMs, 1), 3)
        }
    }
    Write-Progress -Id 4 -Activity "Txn benchmark" -Completed
    return ,$records
}

$all = @()
Write-Host ("Million-scale bench start. sizes={0}" -f ($Sizes -join ","))
$table = "qa_scale_" + $stamp
New-BaseTable -table $table
$currentRows = 0
$sortedSizes = @($Sizes | Sort-Object)
for ($si = 0; $si -lt $sortedSizes.Count; $si++) {
    $size = $sortedSizes[$si]
    $overallPct = [int](($si * 100.0) / [math]::Max($sortedSizes.Count, 1))
    Write-Progress -Id 1 -Activity "Million-scale benchmark" -Status ("Target rows: " + $size) -PercentComplete $overallPct
    if ($size -le $currentRows) { continue }
    $stageRows = $size - $currentRows
    Write-Host ("[stage {0}/{1}] build table={2} append={3}..{4}" -f ($si + 1), $sortedSizes.Count, $table, ($currentRows + 1), $size)
    $buildSec = Append-Rows -table $table -startId ($currentRows + 1) -endId $size
    $currentRows = $size
    $all += [pscustomobject]@{
        rows = $size
        table = $table
        phase = "build"
        metric = "dataset_build"
        total_ms = [math]::Round($buildSec * 1000.0, 3)  # append time for this stage
        avg_ms = [math]::Round($buildSec * 1000.0 / [math]::Max($stageRows, 1), 6)
        tps = [math]::Round($stageRows / [math]::Max($buildSec, 0.001), 3)
    }
    Write-Host ("[stage {0}/{1}] query table={2} rows={3}" -f ($si + 1), $sortedSizes.Count, $table, $size)
    $all += Run-QueryBench -table $table -rows $size
    Write-Host ("[stage {0}/{1}] txn table={2} rows={3}" -f ($si + 1), $sortedSizes.Count, $table, $size)
    $all += Run-TxnBench -table $table -rows $size
}
Write-Progress -Id 1 -Activity "Million-scale benchmark" -Completed

$all | ConvertTo-Json -Depth 5 | Set-Content -Path $resultJson
$all | Export-Csv -Path $resultCsv -NoTypeInformation -Encoding UTF8

Write-Host ("Million-scale bench done. json={0}" -f $resultJson)
Write-Host ("Million-scale bench done. csv={0}" -f $resultCsv)
