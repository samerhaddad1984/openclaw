param(
  [int]$DaysToKeep = 14,
  [int]$MaxTotalMB = 200,
  [int]$MinFilesToKeep = 20
)

$ErrorActionPreference = "Stop"

function Ensure-Dir($p) {
  if (-not (Test-Path -LiteralPath $p)) {
    New-Item -ItemType Directory -Path $p | Out-Null
  }
}

$root = (Get-Location).Path
$sys  = Join-Path $root ".ledgerlink_system"
$sysLogs = Join-Path $sys "logs"
Ensure-Dir $sys
Ensure-Dir $sysLogs

$logFile = Join-Path $sysLogs "log-rotation.log"

function Log($msg) {
  $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
  Add-Content -LiteralPath $logFile -Value "[$ts] $msg"
}

function Get-Files($dir) {
  if (-not (Test-Path -LiteralPath $dir)) { return @() }
  Get-ChildItem -LiteralPath $dir -File -Recurse -ErrorAction SilentlyContinue
}

function Rotate-Folder($dir) {
  if (-not (Test-Path -LiteralPath $dir)) {
    Log "SKIP (missing): $dir"
    return
  }

  Log "ROTATE: $dir"

  $files = Get-Files $dir | Sort-Object LastWriteTimeUtc -Descending

  if ($files.Count -eq 0) {
    Log "  no files"
    return
  }

  # 1) Age-based deletion (keep newest MinFilesToKeep regardless of age)
  $cutoff = (Get-Date).ToUniversalTime().AddDays(-$DaysToKeep)

  $protected = @{}
  $files | Select-Object -First $MinFilesToKeep | ForEach-Object { $protected[$_.FullName] = $true }

  foreach ($f in $files) {
    if ($protected.ContainsKey($f.FullName)) { continue }
    if ($f.LastWriteTimeUtc -lt $cutoff) {
      try {
        Remove-Item -LiteralPath $f.FullName -Force
        Log "  delete(old): $($f.FullName)"
      } catch {
        Log "  FAIL delete(old): $($f.FullName) :: $($_.Exception.Message)"
      }
    }
  }

  # Refresh list after age deletion
  $files2 = Get-Files $dir | Sort-Object LastWriteTimeUtc -Descending
  if ($files2.Count -eq 0) { return }

  # 2) Size-based deletion (keep newest MinFilesToKeep)
  $maxBytes = [int64]$MaxTotalMB * 1024 * 1024
  $total = ($files2 | Measure-Object -Property Length -Sum).Sum
  if (-not $total) { $total = 0 }

  if ($total -le $maxBytes) {
    Log "  size OK: $([math]::Round($total/1MB,2)) MB <= $MaxTotalMB MB"
    return
  }

  Log "  size HIGH: $([math]::Round($total/1MB,2)) MB > $MaxTotalMB MB (trimming oldest)"

  $oldestFirst = $files2 | Sort-Object LastWriteTimeUtc -Ascending

  foreach ($f in $oldestFirst) {
    if ($files2.Count -le $MinFilesToKeep) {
      Log "  stop: reached MinFilesToKeep=$MinFilesToKeep"
      break
    }
    if ($total -le $maxBytes) { break }

    try {
      $len = $f.Length
      Remove-Item -LiteralPath $f.FullName -Force
      $total -= $len
      $files2 = $files2 | Where-Object { $_.FullName -ne $f.FullName }
      Log "  delete(size): $($f.FullName) (freed $([math]::Round($len/1MB,2)) MB)"
    } catch {
      Log "  FAIL delete(size): $($f.FullName) :: $($_.Exception.Message)"
    }
  }

  Log "  final size: $([math]::Round($total/1MB,2)) MB"
}

$targets = @(
  (Join-Path $root "logs"),
  (Join-Path $root "run_logs"),
  (Join-Path $root ".ledgerlink_system\logs")
)

foreach ($t in $targets) {
  Rotate-Folder $t
}

Log "DONE"
