.PHONY: build run stop clean dev test logs health setup check-deps

# Default target
all: setup build

# Create required directories and files
setup:
	mkdir -p grafana/dashboards grafana/datasources
	mkdir -p prometheus/data grafana/data postgres/data redis/data
	@if [ ! -f prometheus.yml ]; then \
		echo "Creating prometheus.yml..."; \
		echo "global:" > prometheus.yml; \
		echo "  scrape_interval: 15s" >> prometheus.yml; \
		echo "  evaluation_interval: 15s" >> prometheus.yml; \
		echo "" >> prometheus.yml; \
		echo "scrape_configs:" >> prometheus.yml; \
		echo "  - job_name: 'prometheus'" >> prometheus.yml; \
		echo "    static_configs:" >> prometheus.yml; \
		echo "      - targets: ['localhost:9090']" >> prometheus.yml; \
		echo "" >> prometheus.yml; \
		echo "  - job_name: 'analytics-demo'" >> prometheus.yml; \
		echo "    static_configs:" >> prometheus.yml; \
		echo "      - targets: ['analytics-demo:3000']" >> prometheus.yml; \
		echo "    metrics_path: /metrics" >> prometheus.yml; \
		echo "    scrape_interval: 5s" >> prometheus.yml; \
		echo "    scrape_timeout: 5s" >> prometheus.yml; \
	fi
	@if [ ! -f grafana/datasources/datasource.yml ]; then \
		echo "Creating Grafana datasource config..."; \
		echo "apiVersion: 1" > grafana/datasources/datasource.yml; \
		echo "" >> grafana/datasources/datasource.yml; \
		echo "datasources:" >> grafana/datasources/datasource.yml; \
		echo "  - name: Prometheus" >> grafana/datasources/datasource.yml; \
		echo "    type: prometheus" >> grafana/datasources/datasource.yml; \
		echo "    access: proxy" >> grafana/datasources/datasource.yml; \
		echo "    url: http://prometheus:9090" >> grafana/datasources/datasource.yml; \
		echo "    isDefault: true" >> grafana/datasources/datasource.yml; \
		echo "    editable: false" >> grafana/datasources/datasource.yml; \
	fi
	@if [ ! -f grafana/dashboards/dashboard.yml ]; then \
		echo "Creating Grafana dashboard config..."; \
		echo "apiVersion: 1" > grafana/dashboards/dashboard.yml; \
		echo "" >> grafana/dashboards/dashboard.yml; \
		echo "providers:" >> grafana/dashboards/dashboard.yml; \
		echo "  - name: 'default'" >> grafana/dashboards/dashboard.yml; \
		echo "    orgId: 1" >> grafana/dashboards/dashboard.yml; \
		echo "    folder: ''" >> grafana/dashboards/dashboard.yml; \
		echo "    type: file" >> grafana/dashboards/dashboard.yml; \
		echo "    disableDeletion: false" >> grafana/dashboards/dashboard.yml; \
		echo "    editable: true" >> grafana/dashboards/dashboard.yml; \
		echo "    options:" >> grafana/dashboards/dashboard.yml; \
		echo "      path: /etc/grafana/provisioning/dashboards" >> grafana/dashboards/dashboard.yml; \
	fi
	@if [ ! -f docker-compose.yml ]; then \
		echo "❌ docker-compose.yml is missing! Please create it first."; \
		exit 1; \
	fi

# Check if all required files exist
check-files:
	@echo "Checking required files..."
	@test -f docker-compose.yml || (echo "❌ docker-compose.yml missing" && exit 1)
	@test -f Dockerfile || (echo "❌ Dockerfile missing" && exit 1)
	@test -f Cargo.toml || (echo "❌ Cargo.toml missing" && exit 1)
	@echo "✅ All required files found"

# Build all images
build: setup check-files
	docker-compose build

# Run the full stack
run: setup check-files
	docker-compose up -d

# Run with logs visible
run-logs: setup check-files
	docker-compose up

# Run in development mode with overrides
dev: setup
	docker-compose -f docker-compose.yml -f docker-compose.override.yml up --build

# Run the full stack with high load by default
run: setup check-files
	EVENTS_PER_SECOND=1000 QUERIES_PER_SECOND=2000 docker-compose up -d

# Ultra-high load scenario (10k+ req/s total)
run-ultra-load: setup
	EVENTS_PER_SECOND=2000 QUERIES_PER_SECOND=8000 docker-compose up -d

# High load scenario (1000+ req/s total)
run-high-load: setup
	EVENTS_PER_SECOND=300 QUERIES_PER_SECOND=700 docker-compose up -d

# Medium load (good for demos)
run-demo-load: setup
	EVENTS_PER_SECOND=50 QUERIES_PER_SECOND=150 docker-compose up -d

# Low load scenario for testing
run-low-load: setup
	EVENTS_PER_SECOND=10 QUERIES_PER_SECOND=20 docker-compose up -d

# Stop all services
stop:
	docker-compose down

# Stop and remove volumes (clean slate)
clean:
	docker-compose down -v --remove-orphans
	docker system prune -f
	sudo rm -rf grafana/data prometheus/data postgres/data redis/data

# View logs from analytics app
logs:
	docker-compose logs -f analytics-demo

# View logs from all services
logs-all:
	docker-compose logs -f

# Check service health
health:
	@echo "=== Service Health Check ==="
	@echo "Analytics App:"
	@curl -s http://localhost:3000/health || echo "❌ Analytics app not responding"
	@echo ""
	@echo "Prometheus:"
	@curl -s http://localhost:9090/-/healthy || echo "❌ Prometheus not responding"
	@echo ""
	@echo "Grafana:"
	@curl -s http://localhost:3001/api/health || echo "❌ Grafana not responding"

# Show metrics sample
metrics:
	@echo "=== Sample Metrics ==="
	@curl -s http://localhost:3000/metrics | head -20

# Show docker stats
stats:
	docker stats --no-stream

# Run tests
test:
	cargo test

# Check if required tools are installed
check-deps:
	@echo "Checking dependencies..."
	@which docker >/dev/null || (echo "❌ Docker not found" && exit 1)
	@which docker-compose >/dev/null || (echo "❌ Docker Compose not found" && exit 1)
	@which curl >/dev/null || (echo "❌ curl not found" && exit 1)
	@echo "✅ All dependencies found"

# Open Grafana in browser (macOS/Linux)
grafana:
	@echo "Opening Grafana dashboard..."
	@open http://localhost:3001 2>/dev/null || xdg-open http://localhost:3001 2>/dev/null || echo "Please open http://localhost:3001 manually"

# Open Prometheus in browser (macOS/Linux)
prometheus:
	@echo "Opening Prometheus..."
	@open http://localhost:9090 2>/dev/null || xdg-open http://localhost:9090 2>/dev/null || echo "Please open http://localhost:9090 manually"

# Run local development (requires local Postgres/Redis)
dev-local:
	cargo run -- \
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
	cargo clippy -- -D warnings

# Full development cycle
dev-cycle: fmt lint test build

# Show helpful information
help:
	@echo "Analytics Demo - Database Migration Showcase"
	@echo ""
	@echo "Quick Start:"
	@echo "  make run          - Start full stack"
	@echo "  make logs         - View application logs"
	@echo "  make health       - Check service health"
	@echo "  make grafana      - Open Grafana dashboard"
	@echo ""
	@echo "Load Testing:"
	@echo "  make run-high-load   - High load (300 events/s, 700 queries/s)"
	@echo "  make run-demo-load   - Demo load (50 events/s, 150 queries/s)"
	@echo "  make run-low-load    - Low load (10 events/s, 20 queries/s)"
	@echo ""
	@echo "Development:"
	@echo "  make dev          - Development mode with live reload"
	@echo "  make dev-local    - Run locally with external DB"
	@echo "  make test         - Run tests"
	@echo "  make clean        - Clean everything"