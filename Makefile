SHELL := /bin/bash

# Default target shows help
.DEFAULT_GOAL := help

PIPELINE_MODULE := pipeline.tasks
WORKER         := luigi-worker
LUIGI_WORKERS  := 8

.PHONY: help up down restart build run rerun logs ps clean kibana-setup pg-shell pg-count es-count

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

build: ## Build the Luigi worker image
	docker compose build

up: ## Boot the full stack (Postgres, pgAdmin, ES, Kibana, Luigi scheduler + worker)
	docker compose up -d
	@echo ""
	@echo "  Luigi UI:    http://localhost:8082"
	@echo "  pgAdmin:     http://localhost:5050   (admin@example.com / admin)"
	@echo "  Elastic:     http://localhost:9200"
	@echo "  Kibana:      http://localhost:5601"

run: ## Run the full Luigi DAG (RunAll wrapper task) with parallel workers
	docker compose exec $(WORKER) python -m luigi --module $(PIPELINE_MODULE) RunAll --scheduler-host luigid --workers $(LUIGI_WORKERS)

rerun: ## Force a fresh run by clearing today's targets, then run
	docker compose exec $(WORKER) bash -c "rm -rf /app/data/output/$$(date -u +%F)"
	$(MAKE) run

kibana-setup: ## Create the Kibana data view for spotify_top_tracks
	@curl -sS -X POST http://localhost:5601/api/data_views/data_view \
		-H "kbn-xsrf: true" \
		-H "Content-Type: application/json" \
		-d '{"data_view":{"title":"spotify_top_tracks","name":"Spotify Top Tracks"}}' \
		| python -m json.tool || true
	@echo "Open http://localhost:5601 -> Discover to explore the index."

pg-shell: ## Open a psql shell against the loaded database
	docker compose exec postgres psql -U $${POSTGRES_USER:-luigi} -d $${POSTGRES_DB:-charts}

pg-count: ## Quick row count from Postgres
	docker compose exec postgres psql -U $${POSTGRES_USER:-luigi} -d $${POSTGRES_DB:-charts} -c "SELECT COUNT(*) AS rows FROM top_tracks;"

es-count: ## Quick doc count from Elasticsearch
	curl -sS "http://localhost:9200/spotify_top_tracks/_count" | python -m json.tool

logs: ## Tail logs from all services
	docker compose logs -f --tail=100

ps: ## Show service status
	docker compose ps

restart: down up ## Restart everything

down: ## Stop the stack (keeps volumes)
	docker compose down

clean: ## Stop and remove containers + volumes + Luigi outputs
	docker compose down -v
	rm -rf data/output
