VERTICAL ?= retail
SCALE ?= demo
COMPOSE_FILE = docker-compose.$(VERTICAL).yml

# Fall back to default docker-compose.yml for retail (backward compat)
ifeq ($(VERTICAL),retail)
  COMPOSE_FILE = docker-compose.yml
endif

.PHONY: up down reset logs status init init-logs app app-logs app-local list help

help: ## Show available commands
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

list: ## List available verticals
	@echo "Available verticals (use VERTICAL=<name>):"
	@echo ""
	@echo "  retail        E-commerce (default) — T-ECD dataset"
	@echo "                5 DBs: Postgres, MongoDB, Redis, ClickHouse, Weaviate"
	@echo ""
	@echo "  stonebreaker  5-source benchmark — exact two-source tasks over retail keys"
	@echo "                5 DBs: Postgres, MongoDB, Redis, ClickHouse, Weaviate"
	@echo ""
	@echo "  tech          Tech/SaaS — UNSW-NB15, StackOverflow, CVE, ecommerce events"
	@echo "                6 DBs: 2x Postgres, ClickHouse, MongoDB, Redis, Weaviate"
	@echo ""
	@echo "  finance       Banking — CiferAI fraud, TroveLedger stocks, SEC filings"
	@echo "                6 DBs: 2x Postgres, ClickHouse, MongoDB, Redis, Weaviate"
	@echo ""
	@echo "  healthcare    Clinical — Synthea 575K patients (multi-table EHR)"
	@echo "                7 DBs: 2x Postgres, 2x MongoDB, ClickHouse, Redis, Weaviate"
	@echo ""
	@echo "  insurance     Insurance — US Accidents, freMTPL2, driver risk"
	@echo "                6 DBs: 2x Postgres, ClickHouse, MongoDB, Redis, Weaviate"
	@echo ""
	@echo "  migration     Redis migration — Financial services Redis-to-Redis migration"
	@echo "                2 DBs: Source Redis + Destination Redis"
	@echo ""
	@echo "  bird          BIRD text-to-SQL benchmark — SQLite imported into Postgres"
	@echo "                1 DB: Postgres replaying validated BIRD SQL"

# ── Start / Stop ─────────────────────────────────────────────────

up: ## Start databases + load data (VERTICAL=retail|stonebreaker|tech|finance|healthcare|insurance|migration|bird)
	docker compose -f $(COMPOSE_FILE) up -d
	@echo ""
	@echo "ADAM Demo — $(VERTICAL) vertical"
	@echo "Compose file: $(COMPOSE_FILE)"
	@echo ""
	@echo "Watch data loading:  make logs VERTICAL=$(VERTICAL)"
	@echo "Start query app:     make app VERTICAL=$(VERTICAL)"

app: ## Start the ADAM query engine (requires Eden API running)
	docker compose -f $(COMPOSE_FILE) --profile app up -d adam-app
	@echo ""
	@echo "ADAM Demo + Query App starting..."
	@echo "  App: localhost:3000  (/metrics, /health, /status)"
	@echo ""
	@echo "Watch app logs: make app-logs VERTICAL=$(VERTICAL)"

down: ## Stop and remove containers
	docker compose -f $(COMPOSE_FILE) --profile app down

reset: ## Stop containers and delete all data volumes
	docker compose -f $(COMPOSE_FILE) --profile app down -v
	@echo "All data volumes removed"

# ── Logs ──────────────────────────────────────────────────────────

logs: ## Tail data-init logs
	docker compose -f $(COMPOSE_FILE) logs -f data-init

init-logs: ## Alias for logs
	docker compose -f $(COMPOSE_FILE) logs -f data-init

app-logs: ## Tail query app logs
	docker compose -f $(COMPOSE_FILE) logs -f adam-app

# ── Development ───────────────────────────────────────────────────

app-local: ## Run the Rust app locally (outside Docker)
	cd app && cargo run -- \
		--vertical $(VERTICAL) \
		--eden-api-url $${EDEN_API_URL:-http://localhost:8000} \
		--eden-org-id $${EDEN_ORG_ID:-adam-demo} \
		--eden-new-org-secret $${EDEN_NEW_ORG_SECRET:-neworgsecret} \
		--queries-per-second $${QUERIES_PER_SECOND:-50} \
		--http-timeout $${HTTP_TIMEOUT:-30} \
		--setup-retries $${SETUP_RETRIES:-5}

init: ## Rebuild and re-run data-init container
	docker compose -f $(COMPOSE_FILE) build data-init
	docker compose -f $(COMPOSE_FILE) up data-init

status: ## Check status of all services
	@docker compose -f $(COMPOSE_FILE) ps

# ── Presets ───────────────────────────────────────────────────────

app-high: ## Start app with high QPS (500 qps, 200 workers)
	QUERIES_PER_SECOND=500 MAX_WORKERS=200 docker compose -f $(COMPOSE_FILE) --profile app up -d adam-app

app-demo: ## Start app with low QPS for live demos
	QUERIES_PER_SECOND=20 docker compose -f $(COMPOSE_FILE) --profile app up -d adam-app
