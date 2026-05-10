# Run ctest with common NEWDB_* cleared (PowerShell child process inherits cleaned env).
# Usage:
#   .\newdb\scripts\run_newdb_tests_cleanenv.ps1 -BuildDir E:\db\DB\newdb\build -Filter 'TxnEmbeddedContract|TxnUndoMetrics'
#   .\newdb\scripts\run_newdb_tests_cleanenv.ps1 -BuildDir E:\db\DB\newdb\build

param(
    [Parameter(Mandatory = $true)][string]$BuildDir,
    [string]$Filter = ""
)

$names = @(
    "NEWDB_TXN_ISOLATION_READPATH",
    "NEWDB_TXN_STMT_SAVEPOINT",
    "NEWDB_TXN_TRACE",
    "NEWDB_VISCHK",
    "NEWDB_WHERE_RESERVE_PREDICATE",
    "NEWDB_WHERE_RESERVE_RANGE",
    "NEWDB_LAZY_HEAP"
)
foreach ($n in $names) {
    Set-Item -Path "Env:$n" -Value $null -ErrorAction SilentlyContinue
}

Push-Location $BuildDir
try {
    if ($Filter -ne "") {
        ctest --output-on-failure -R $Filter
    }
    else {
        ctest -L newdb --output-on-failure
    }
}
finally {
    Pop-Location
}
