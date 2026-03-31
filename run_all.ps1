$ErrorActionPreference = "Stop"

$logDir = "D:\Agents\OtoCPAAi\src\agents\data\logs"
if (!(Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir | Out-Null
}

$runLog = Join-Path $logDir "run_all.log"

function Write-Log {
    param([string]$Text)
    $line = $Text + [Environment]::NewLine
    [System.IO.File]::AppendAllText($runLog, $line, [System.Text.Encoding]::UTF8)
}

Write-Log ""
Write-Log "========================================"
Write-Log ("RUN START: " + (Get-Date).ToString("yyyy-MM-dd HH:mm:ss"))
Write-Log "========================================"

Write-Log "Step 1: Email -> SharePoint"
$step1 = py -3.11 D:\Agents\OtoCPAAi\src\agents\tools\otocpa_runner.py 2>&1
$step1 | ForEach-Object { Write-Log $_ }
$step1

Write-Log "Step 2: SharePoint -> Extraction / Routing / Draft CSV"
$step2 = py -3.11 D:\Agents\OtoCPAAi\src\agents\tools\sharepoint_processor.py 2>&1
$step2 | ForEach-Object { Write-Log $_ }
$step2

Write-Log "Step 3: Posting Builder"
$step3 = py -3.11 D:\Agents\OtoCPAAi\src\agents\tools\posting_builder.py 2>&1
$step3 | ForEach-Object { Write-Log $_ }
$step3

Write-Log ("RUN END: " + (Get-Date).ToString("yyyy-MM-dd HH:mm:ss"))
Write-Log ""