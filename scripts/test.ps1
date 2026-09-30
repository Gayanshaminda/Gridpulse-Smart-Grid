$ErrorActionPreference = 'Stop'
Set-Location (Split-Path -Parent $PSScriptRoot)
docker compose run --rm --no-deps stream python -m pytest tests -q
if ($LASTEXITCODE -ne 0) { throw 'Tests failed.' }

