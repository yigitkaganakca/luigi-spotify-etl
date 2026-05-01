# tasks.ps1 - PowerShell task runner mirroring the Makefile targets.
# Usage:  ./tasks.ps1 <target>
# Targets: help build up down restart run rerun pg-shell pg-count es-count kibana-setup logs ps clean

[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [string]$Target = "help"
)

$ErrorActionPreference = "Stop"

$Worker         = "luigi-worker"
$PipelineModule = "pipeline.tasks"
$LuigiWorkers   = 8
$PgUser         = if ($env:POSTGRES_USER) { $env:POSTGRES_USER } else { "luigi" }
$PgDb           = if ($env:POSTGRES_DB)   { $env:POSTGRES_DB }   else { "charts" }

function Ensure-Network {
    $exists = docker network ls --format "{{.Name}}" | Select-String -SimpleMatch -Pattern "^luigi$"
    if (-not $exists) {
        Write-Host "Creating docker network 'luigi'..."
        docker network create luigi | Out-Null
    }
}

function Show-Help {
@"
Available targets:
  build         Build the Luigi worker image
  up            Start the full stack (Postgres, pgAdmin, ES, Kibana, Luigi scheduler + worker)
  down          Stop the stack (keeps volumes)
  restart       down + up
  run           Run the Luigi DAG (RunAll wrapper, parallel workers)
  rerun         Wipe today's run dir, then run
  pg-shell      Open psql against the loaded database
  pg-count      Print row count from Postgres top_tracks
  es-count      Print doc count from Elasticsearch spotify_top_tracks
  kibana-setup  Create the Kibana data view for spotify_top_tracks
  logs          Tail logs from all services
  ps            Show service status
  clean         Stop and wipe containers, volumes, data/output

Examples:
  ./tasks.ps1 up
  ./tasks.ps1 run
  ./tasks.ps1 pg-count
"@ | Write-Host
}

switch ($Target.ToLower()) {
    "help"     { Show-Help }
    "build"    { Ensure-Network; docker compose build }
    "up"       {
        Ensure-Network
        docker compose up -d
        Write-Host ""
        Write-Host "  Luigi UI:    http://localhost:8082"
        Write-Host "  pgAdmin:     http://localhost:5050   (admin@example.com / admin)"
        Write-Host "  Elastic:     http://localhost:9200"
        Write-Host "  Kibana:      http://localhost:5601"
    }
    "down"     { docker compose down }
    "restart"  { docker compose down; docker compose up -d }
    "run"      {
        docker compose exec $Worker python -m luigi --module $PipelineModule RunAll `
            --scheduler-host luigid --workers $LuigiWorkers
    }
    "rerun"    {
        $today = (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd")
        docker compose exec $Worker bash -c "rm -rf /app/data/output/$today"
        docker compose exec $Worker python -m luigi --module $PipelineModule RunAll `
            --scheduler-host luigid --workers $LuigiWorkers
    }
    "pg-shell" { docker compose exec postgres psql -U $PgUser -d $PgDb }
    "pg-count" {
        docker compose exec postgres psql -U $PgUser -d $PgDb `
            -c "SELECT COUNT(*) AS rows FROM top_tracks;"
    }
    "es-count" {
        $resp = Invoke-RestMethod -Uri "http://localhost:9200/spotify_top_tracks/_count"
        $resp | ConvertTo-Json
    }
    "kibana-setup" {
        $body = '{"data_view":{"title":"spotify_top_tracks","name":"Spotify Top Tracks"}}'
        try {
            $resp = Invoke-RestMethod -Method Post `
                -Uri "http://localhost:5601/api/data_views/data_view" `
                -Headers @{ "kbn-xsrf" = "true"; "Content-Type" = "application/json" } `
                -Body $body
            $resp | ConvertTo-Json -Depth 5
        } catch {
            Write-Host ("kibana-setup: " + $_.Exception.Message)
        }
        Write-Host "Open http://localhost:5601 -> Discover to explore the index."
        Write-Host "Then build the dashboard manually (see README section 5)."
    }
    "logs"     { docker compose logs -f --tail=100 }
    "ps"       { docker compose ps }
    "clean"    {
        docker compose down -v
        Remove-Item -Recurse -Force data\output -ErrorAction SilentlyContinue
    }
    default    {
        Write-Host "Unknown target: $Target"
        Show-Help
        exit 1
    }
}
