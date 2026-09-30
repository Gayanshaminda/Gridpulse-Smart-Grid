$ErrorActionPreference = 'Stop'
Set-Location (Split-Path -Parent $PSScriptRoot)
docker compose stop
Write-Host 'Services stopped. Readings, checkpoints and bills are retained.'

