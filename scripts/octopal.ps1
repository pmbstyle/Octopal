# Octopal Installation Script for Windows (PowerShell)
$ErrorActionPreference = "Stop"
$RepoUrl = "https://github.com/pmbstyle/Octopal.git"
$LatestReleaseApiUrl = "https://api.github.com/repos/pmbstyle/Octopal/releases/latest"

function Install-Git {
    Write-Host "git not found. Attempting to install..." -ForegroundColor Yellow
    if (Get-Command winget -ErrorAction SilentlyContinue) {
        winget install --exact --id Git.Git --accept-package-agreements --accept-source-agreements
    }
    elseif (Get-Command choco -ErrorAction SilentlyContinue) {
        choco install git -y
    }
    else {
        throw "Error: Could not auto-install git. Please install git (https://git-scm.com/) manually and try again."
    }

    # Refresh PATH for the current session
    $env:Path = [System.Environment]::GetEnvironmentVariable("Path", "Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path", "User")
}

function Get-LatestReleaseTag {
    try {
        $release = Invoke-RestMethod -Uri $LatestReleaseApiUrl -Headers @{
            "Accept" = "application/vnd.github+json"
            "User-Agent" = "octopal-installer"
        }

        if ($release.tag_name) {
            return $release.tag_name.Trim()
        }
    }
    catch {
        Write-Host "Could not fetch the latest GitHub release. Falling back to the latest tag..." -ForegroundColor Yellow
    }

    $latestTagRef = git ls-remote --tags --sort=-version:refname --refs $RepoUrl | Select-Object -First 1

    if (-not $latestTagRef) {
        throw "Error: Could not determine the latest Octopal release tag."
    }

    if ($latestTagRef -match "refs/tags/(.+)$") {
        return $matches[1]
    }

    throw "Error: Could not parse the latest Octopal release tag."
}

if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
    Install-Git
}

$releaseTag = Get-LatestReleaseTag

Write-Host "Installing Octopal $releaseTag..." -ForegroundColor Cyan
git -c advice.detachedHead=false clone --branch $releaseTag --depth 1 $RepoUrl octopal
Set-Location octopal

if (Test-Path ".\scripts\bootstrap.ps1") {
    Write-Host "Running bootstrap script..." -ForegroundColor Cyan
    .\scripts\bootstrap.ps1
}
else {
    Write-Error "Error: bootstrap script not found in repository."
    exit 1
}
