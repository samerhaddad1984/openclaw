param(
    [string]$InstallRoot = "C:\LedgerLinkAI",
    [int]$LoopSeconds = 60,
    [int]$BatchLimit = 20
)

$ErrorActionPreference = "Stop"

function Write-Step {
    param([string]$Message)
    Write-Host ""
    Write-Host "============================================================" -ForegroundColor Cyan
    Write-Host $Message -ForegroundColor Cyan
    Write-Host "============================================================" -ForegroundColor Cyan
}

function Ensure-Path {
    param([string]$Path, [string]$Description)
    if (-not (Test-Path $Path)) {
        throw "$Description not found: $Path"
    }
}

$venvPython = Join-Path $InstallRoot ".venv\Scripts\python.exe"
$queueScript = Join-Path $InstallRoot "scripts\run_openclaw_queue.py"
$doctorScript = Join-Path $InstallRoot "src\agents\core\software_doctor.py"

$logDir = Join-Path $InstallRoot "logs"
$queueLog = Join-Path $logDir "queue_worker.log"
$errorLog = Join-Path $logDir "queue_worker_error.log"

Ensure-Path -Path $InstallRoot -Description "Install root"
Ensure-Path -Path $venvPython -Description "Virtual environment Python"
Ensure-Path -Path $queueScript -Description "Queue script"
Ensure-Path -Path $doctorScript -Description "Software doctor script"

if (-not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir | Out-Null
}

Set-Location $InstallRoot

Write-Step "LedgerLink background worker started"
Write-Host "Install root : $InstallRoot"
Write-Host "Loop seconds : $LoopSeconds"
Write-Host "Batch limit  : $BatchLimit"
Write-Host "Queue log    : $queueLog"
Write-Host "Error log    : $errorLog"

while ($true) {

    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

    try {

        Set-Location $InstallRoot

        Add-Content -Path $queueLog -Value "[$timestamp] Running software doctor..."

        & $venvPython $doctorScript --json-only *>> $queueLog

        $queueTimestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

        Add-Content -Path $queueLog -Value "[$queueTimestamp] Running OpenClaw queue..."

        & $venvPython $queueScript --limit $BatchLimit *>> $queueLog

        $doneTimestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

        Add-Content -Path $queueLog -Value "[$doneTimestamp] Cycle completed successfully."

    }
    catch {

        $errorTimestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

        Add-Content -Path $errorLog -Value "[$errorTimestamp] Worker cycle failed: $($_.Exception.Message)"

    }

    Start-Sleep -Seconds $LoopSeconds

}