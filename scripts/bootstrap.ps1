Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not (Test-Path "pyproject.toml")) {
    Write-Error "Run this script from the repository root (pyproject.toml not found)."
}

function Test-Uv {
    return [bool](Get-Command uv -ErrorAction SilentlyContinue)
}

if (-not (Test-Uv)) {
    Write-Host "uv not found. Installing..."
    powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
}

if (-not (Test-Uv)) {
    $localUvPath = Join-Path $HOME ".local\bin"
    if (Test-Path $localUvPath) {
        $env:Path = "$localUvPath;$env:Path"
        
        # Persist to User PATH
        $currentPath = [Environment]::GetEnvironmentVariable("Path", "User")
        if ($currentPath -notlike "*$localUvPath*") {
            Write-Host "Adding $localUvPath to User PATH persistently..."
            [Environment]::SetEnvironmentVariable("Path", "$currentPath;$localUvPath", "User")
        }
    }
}

if (-not (Test-Uv)) {
    Write-Error "uv is still not available on PATH. Restart your shell and retry."
}

uv --version
uv sync

Write-Host "Installing Playwright browser binaries..."
uv run playwright install chromium

Write-Host ""
Write-Host "Launching onboarding..."
uv run broodmind configure

Write-Host ""
Write-Host "Bootstrap complete."
Write-Host "Next steps:"
Write-Host "  uv run broodmind start"
