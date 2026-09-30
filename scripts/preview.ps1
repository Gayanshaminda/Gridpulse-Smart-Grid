$ErrorActionPreference = 'Stop'
Set-Location (Split-Path -Parent $PSScriptRoot)
$env:DATABASE_URL = 'sqlite:///data/preview.db'
$env:DEMO_MODE = 'true'
$env:PYTHONPATH = (Get-Location).Path
if (Test-Path '.venv311/Scripts/python.exe') { $pythonPath = '.venv311/Scripts/python.exe' }
elseif (Test-Path '.venv/Scripts/python.exe') { $pythonPath = '.venv/Scripts/python.exe' }
else { throw 'Create a Python 3.11 virtual environment and install requirements.txt first.' }
& $pythonPath -m grid.local_demo
if ($LASTEXITCODE -ne 0) { throw 'Preview data creation failed.' }
& $pythonPath -m streamlit run dashboard/app.py --server.address=127.0.0.1 --server.port=8502

