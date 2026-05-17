#requires -version 5.1
<#
.SYNOPSIS
  Cold backup: copy data_dir + session_dir per POLICY §4.0.2 (no single-file .db promise).

.PARAMETER OutDir
  Output root; creates OutDir\data_dir and OutDir\session_dir.
#>
param(
  [Parameter(Mandatory = $true)][string]$DataDir,
  [string]$SessionDir = "",
  [Parameter(Mandatory = $true)][string]$OutDir
)

$ErrorActionPreference = "Stop"
if (-not $SessionDir) {
  $SessionDir = Join-Path $DataDir "embed_session"
}
if (-not (Test-Path -LiteralPath $DataDir)) { throw "data_dir not found: $DataDir" }
if (-not (Test-Path -LiteralPath $SessionDir)) { throw "session_dir not found: $SessionDir" }

$outData = Join-Path $OutDir "data_dir"
$outSess = Join-Path $OutDir "session_dir"
New-Item -ItemType Directory -Path $OutDir -Force | Out-Null
if (Test-Path -LiteralPath $outData) { Remove-Item -LiteralPath $outData -Recurse -Force }
if (Test-Path -LiteralPath $outSess) { Remove-Item -LiteralPath $outSess -Recurse -Force }
Copy-Item -LiteralPath $DataDir -Destination $outData -Recurse -Force
Copy-Item -LiteralPath $SessionDir -Destination $outSess -Recurse -Force
Write-Host "[BACKUP_BUNDLE] ok out=$OutDir"
