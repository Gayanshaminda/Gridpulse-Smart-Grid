$ErrorActionPreference = 'Stop'
Set-Location (Split-Path -Parent $PSScriptRoot)
docker info --format '{{.ServerVersion}}'
if ($LASTEXITCODE -ne 0) { throw 'Start Docker Desktop with Linux containers, then run this script again.' }
docker compose up -d --build
if ($LASTEXITCODE -ne 0) { throw 'Startup failed. See the Docker output above.' }
Write-Host 'Dashboard: http://localhost:8501'
Write-Host 'Airflow: http://localhost:8080 (admin / admin)'
Write-Host 'Metrics API: http://localhost:8000/docs'

