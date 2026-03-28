# Start Redis and all services (API on http://localhost:8000, dashboard at http://localhost:8000/).
# Requires Docker Desktop (or Docker Engine) with Compose V2.
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
docker compose up --build
