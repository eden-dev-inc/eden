.PHONY: build run stop clean dev test test-runtime verify-deploy validate-runtime logs health setup check-deps \
       run-redis run-postgres run-all run-datadog run-server-only run-internal \
       scale-clients client-ports

# Default target
all: setup build

CLIENT_REPLICAS ?= 1
SERVER_BASE_URL ?= http://127.0.0.1:3000
CLIENT_BASE_URL ?=
REQUIRE_POSTGRES ?= false
REQUIRE_REDIS ?= false

run: run-redis

# Create required directories and files
setup:
	mkdir -p postgres/data redis/data
	@if [ ! -f docker-compose.yml ]; then \
		echo "docker-compose.yml is missing! Please create it first."; \
		exit 1; \
	fi

# Check if all required files exist
check-files:
	@echo "Checking required files..."
	@test -f docker-compose.yml || (echo "docker-compose.yml missing" && exit 1)
	@test -f Dockerfile || (echo "Dockerfile missing" && exit 1)
	@test -f Cargo.toml || (echo "Cargo.toml missing" && exit 1)
	@echo "All required files found"

# Build all images
build: setup check-files
	docker compose build analytics-server traffic-client

# ===== Profile-based run targets =====

# Clean up stale Docker networks to prevent "network not found" errors
network-cleanup:
	@docker network prune -f > /dev/null 2>&1 || true

# Redis-only mode: server + external clients + Redis
run-redis: setup check-files network-cleanup
	REDIS_ENABLED=true POSTGRES_ENABLED=false INTERNAL_WORKLOAD_ENABLED=false CARGO_FEATURES=redis-string docker compose --profile redis --profile clients up -d --build --scale traffic-client=$(CLIENT_REPLICAS)

# Postgres mode: server + external clients + Postgres
run-postgres: setup check-files network-cleanup
	REDIS_ENABLED=false POSTGRES_ENABLED=true INTERNAL_WORKLOAD_ENABLED=false CARGO_FEATURES=redis-string docker compose --profile postgres --profile clients up -d --build --scale traffic-client=$(CLIENT_REPLICAS)

# Both: server + external clients + Redis + Postgres
run-all: setup check-files network-cleanup
	REDIS_ENABLED=true POSTGRES_ENABLED=true INTERNAL_WORKLOAD_ENABLED=false CARGO_FEATURES=redis-string docker compose --profile redis --profile postgres --profile clients up -d --build --scale traffic-client=$(CLIENT_REPLICAS)

# Redis mode plus Datadog export path
run-datadog: setup check-files network-cleanup
	REDIS_ENABLED=true POSTGRES_ENABLED=false INTERNAL_WORKLOAD_ENABLED=false TELEMETRY_ENABLED=true TELEMETRY_PROVIDER=datadog TELEMETRY_DOGSTATSD_ENDPOINT=datadog-agent:8125 TELEMETRY_OPENTELEMETRY_ENDPOINT=http://datadog-agent:4318 CARGO_FEATURES=redis-string docker compose --profile redis --profile clients --profile datadog up -d --build --scale traffic-client=$(CLIENT_REPLICAS)

# Server only, no external clients
run-server-only: setup check-files network-cleanup
	REDIS_ENABLED=true POSTGRES_ENABLED=false INTERNAL_WORKLOAD_ENABLED=false CARGO_FEATURES=redis-string docker compose --profile redis up -d --build analytics-server redis

# Legacy in-process workload mode inside the server
run-internal: setup check-files network-cleanup
	REDIS_ENABLED=true POSTGRES_ENABLED=false INTERNAL_WORKLOAD_ENABLED=true CARGO_FEATURES=redis-string docker compose --profile redis up -d --build

# ===== Load scenario targets =====

# Ultra-high load scenario (10k+ req/s total)
run-ultra-load: setup
	REDIS_ENABLED=true POSTGRES_ENABLED=false INTERNAL_WORKLOAD_ENABLED=false CLIENT_QUERIES_PER_SECOND=1800 CLIENT_EVENTS_PER_SECOND=720 CARGO_FEATURES=redis-string docker compose --profile redis --profile clients up -d --build --scale traffic-client=$(CLIENT_REPLICAS)

# High load scenario (1000+ req/s total)
run-high-load: setup
	REDIS_ENABLED=true POSTGRES_ENABLED=false INTERNAL_WORKLOAD_ENABLED=false CLIENT_QUERIES_PER_SECOND=580 CLIENT_EVENTS_PER_SECOND=220 CARGO_FEATURES=redis-string docker compose --profile redis --profile clients up -d --build --scale traffic-client=$(CLIENT_REPLICAS)

# Medium load (good for demos)
run-demo-load: setup
	REDIS_ENABLED=true POSTGRES_ENABLED=false INTERNAL_WORKLOAD_ENABLED=false CLIENT_QUERIES_PER_SECOND=180 CLIENT_EVENTS_PER_SECOND=57 CARGO_FEATURES=redis-string docker compose --profile redis --profile clients up -d --build --scale traffic-client=$(CLIENT_REPLICAS)

# Low load scenario for testing
run-low-load: setup
	REDIS_ENABLED=true POSTGRES_ENABLED=false INTERNAL_WORKLOAD_ENABLED=false CLIENT_QUERIES_PER_SECOND=28 CLIENT_EVENTS_PER_SECOND=12 CARGO_FEATURES=redis-string docker compose --profile redis --profile clients up -d --build --scale traffic-client=$(CLIENT_REPLICAS)

# Scale the client pool up or down without rebuilding the stack
scale-clients: setup check-files
	docker compose --profile clients up -d --scale traffic-client=$(CLIENT_REPLICAS) traffic-client

# Show the currently published host ports for each traffic-client replica
client-ports:
	docker compose ps traffic-client

# ===== Lifecycle =====

# Stop all services (all profiles)
stop:
	docker compose --profile redis --profile postgres --profile clients --profile datadog down

# Stop and remove volumes (clean slate)
clean:
	docker compose --profile redis --profile postgres --profile clients --profile datadog down -v --remove-orphans
	docker system prune -f
	sudo rm -rf postgres/data redis/data

# View logs from analytics app
logs:
	docker compose logs -f analytics-server

# View logs from the external traffic clients
logs-clients:
	docker compose logs -f traffic-client

# View logs from all services
logs-all:
	docker compose logs -f

# Run with logs visible (Redis-only by default)
run-logs: setup check-files
	REDIS_ENABLED=true POSTGRES_ENABLED=false INTERNAL_WORKLOAD_ENABLED=false CARGO_FEATURES=redis-string docker compose --profile redis --profile clients up --build --scale traffic-client=$(CLIENT_REPLICAS)

# Run in development mode with overrides
dev: setup
	docker compose -f docker-compose.yml -f docker-compose.override.yml up --build

# ===== Utilities =====

# Check service health
health:
	@echo "=== Service Health Check ==="
	@echo "Analytics Server:"
	@curl -s http://localhost:3000/health || echo "Analytics server not responding"
	@echo ""
	@echo "Traffic Client Replicas:"
	@docker compose ps traffic-client || echo "Traffic client not running"
	@echo ""
	@echo "Datadog Agent (optional):"
	@docker compose ps datadog-agent >/dev/null 2>&1 && docker compose logs --tail=5 datadog-agent 2>/dev/null || echo "Datadog agent not running"

# Show docker stats
stats:
	docker stats --no-stream

# Run tests
test:
	cargo test --lib --bins

test-runtime:
	cargo test --test runtime_coverage -- --test-threads=1

verify-deploy:
	cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test --lib --bins && cargo test --test runtime_coverage -- --test-threads=1

validate-runtime:
	cargo run --bin runtime-validator -- \
		--server-base-url $(SERVER_BASE_URL) \
		--require-postgres $(REQUIRE_POSTGRES) \
		--require-redis $(REQUIRE_REDIS) \
		$(if $(CLIENT_BASE_URL),--client-base-url $(CLIENT_BASE_URL),)

# Check if required tools are installed
check-deps:
	@echo "Checking dependencies..."
	@which docker >/dev/null || (echo "Docker not found" && exit 1)
	@which docker compose >/dev/null || (echo "Docker Compose not found" && exit 1)
	@which curl >/dev/null || (echo "curl not found" && exit 1)
	@echo "All dependencies found"

# Run local development (requires local Postgres/Redis)
dev-local:
	cargo run --bin analytics-server -- \
		--redis-enabled true \
		--postgres-enabled true \
		--events-per-second 50 \
		--queries-per-second 100 \
		--organizations 10 \
		--users-per-org 100 \
		--database-url postgresql://postgres:postgres@localhost:5432/analytics \
		--redis-url redis://localhost:6379

# Format code
fmt:
	cargo fmt

# Run clippy linter
lint:
	cargo clippy --all-targets -- -D warnings

# Full development cycle
dev-cycle: fmt lint test build

# Show helpful information
help:
	@echo "Analytics Server + Traffic Client Demo"
	@echo ""
	@echo "Quick Start (pick a mode):"
	@echo "  make run            - Redis mode with external traffic clients (default)"
	@echo "  CLIENT_REPLICAS=4 make run - Redis mode with 4 traffic-client replicas"
	@echo "  make run-redis      - Redis mode with external traffic clients"
	@echo "  make run-postgres   - PostgreSQL-only mode with external traffic clients"
	@echo "  make run-all        - Dual-backend mode with external traffic clients"
	@echo "  make run-datadog    - Redis mode with Datadog agent export"
	@echo "  make scale-clients  - Change the number of traffic-client replicas"
	@echo "  make client-ports   - Show the published client admin ports"
	@echo ""
	@echo "  make stop           - Stop all services"
	@echo "  make test           - Run unit + binary tests"
	@echo "  make test-runtime   - Run runtime integration tests"
	@echo "  make verify-deploy  - Run the full pre-deploy verification gate"
	@echo "  make validate-runtime - Validate a running stack over HTTP"
	@echo "  make logs           - View application logs"
	@echo "  make health         - Check service health"
	@echo ""
	@echo "Load Testing (Redis-only):"
	@echo "  make run-ultra-load - Ultra load (2000 events/s, 8000 queries/s)"
	@echo "  make run-high-load  - High load (300 events/s, 700 queries/s)"
	@echo "  make run-demo-load  - Demo load (50 events/s, 150 queries/s)"
	@echo "  make run-low-load   - Low load (10 events/s, 20 queries/s)"
	@echo ""
	@echo "Development:"
	@echo "  make dev-local      - Run locally with external DB"
	@echo "  make test           - Run tests"
	@echo "  make clean          - Clean everything"
