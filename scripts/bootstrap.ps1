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
    }
}

if (-not (Test-Uv)) {
    Write-Error "uv is still not available on PATH. Restart your shell and retry."
}

uv --version
uv sync

Write-Host ""
Write-Host "Bootstrap complete."
Write-Host "Next steps:"
Write-Host "  uv run broodmind configure"
Write-Host "  uv run broodmind start"
