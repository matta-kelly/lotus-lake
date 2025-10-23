.PHONY: help build-dev build-prod up-dev up-prod up-logs-dev up-logs-prod down-dev down-prod down-clean-dev down-clean-prod logs-dev logs-prod rebuild-dev rebuild-prod rebuild-dagster-dev rebuild-dagster-prod rebuild-dashboards-dev rebuild-dashboards-prod rebuild-spark-dev rebuild-spark-prod restart-dagster-dev restart-dagster-prod restart-dashboards-dev restart-dashboards-prod restart-caddy-dev restart-caddy-prod

COMPOSE_DEV = docker compose -f infra/docker-compose.dev.yml
COMPOSE_PROD = docker compose -f infra/docker-compose.prod.yml

help:
	@echo "Lotus Data Stack - Makefile Commands"
	@echo ""
	@echo "=== DEVELOPMENT ==="
	@echo "  make build-dev              Build all images"
	@echo "  make up-dev                 Start services"
	@echo "  make up-logs-dev            Start + follow logs"
	@echo "  make logs-dev               Follow logs (already running)"
	@echo "  make down-dev               Stop services (keeps volumes)"
	@echo "  make down-clean-dev         Stop + remove volumes"
	@echo "  make rebuild-dev            Full rebuild"
	@echo "  make rebuild-dagster-dev    Rebuild just Dagster"
	@echo "  make rebuild-dashboards-dev Rebuild just dashboards"
	@echo "  make rebuild-spark-dev      Rebuild Spark stack"
	@echo "  make restart-dagster-dev    Restart Dagster (no rebuild)"
	@echo "  make restart-dashboards-dev Restart dashboards (no rebuild)"
	@echo ""
	@echo "=== PRODUCTION ==="
	@echo "  make build-prod             Build all images"
	@echo "  make up-prod                Start services"
	@echo "  make up-logs-prod           Start + follow logs"
	@echo "  make logs-prod              Follow logs (already running)"
	@echo "  make down-prod              Stop services (keeps volumes)"
	@echo "  make down-clean-prod        Stop + remove volumes"
	@echo "  make rebuild-prod           Full rebuild"
	@echo "  make rebuild-dagster-prod   Rebuild just Dagster"
	@echo "  make rebuild-dashboards-prod Rebuild just dashboards"
	@echo "  make rebuild-spark-prod     Rebuild Spark stack"
	@echo "  make restart-dagster-prod   Restart Dagster (no rebuild)"
	@echo "  make restart-dashboards-prod Restart dashboards (no rebuild)"

# === DEV TARGETS ===
build-dev:
	@echo "Building dev environment..."
	$(COMPOSE_DEV) build spark-base
	$(COMPOSE_DEV) build

up-dev:
	@echo "Starting dev environment..."
	$(COMPOSE_DEV) up -d

up-logs-dev:
	$(COMPOSE_DEV) up

logs-dev:
	$(COMPOSE_DEV) logs -f

down-dev:
	@echo "Stopping dev environment (volumes preserved)..."
	$(COMPOSE_DEV) down

down-clean-dev:
	@echo "WARNING: Stopping dev and removing volumes..."
	@printf "Are you sure? [y/N] " && read ans && [ $${ans:-N} = y ]
	$(COMPOSE_DEV) down -v

rebuild-dev: down-dev build-dev up-dev

rebuild-dagster-dev:
	@echo "Rebuilding Dagster (dev)..."
	$(COMPOSE_DEV) build dagster
	$(COMPOSE_DEV) up -d --no-deps dagster

rebuild-dashboards-dev:
	@echo "Rebuilding dashboards (dev)..."
	$(COMPOSE_DEV) build custom-dashboards
	$(COMPOSE_DEV) up -d --no-deps custom-dashboards

rebuild-spark-dev:
	@echo "Rebuilding Spark (dev)..."
	$(COMPOSE_DEV) build spark-base spark-master spark-worker
	$(COMPOSE_DEV) up -d --no-deps spark-master spark-worker

restart-dagster-dev:
	$(COMPOSE_DEV) restart dagster

restart-dashboards-dev:
	$(COMPOSE_DEV) restart custom-dashboards

restart-caddy-dev:
	$(COMPOSE_DEV) restart caddy

# === PROD TARGETS ===
build-prod:
	@echo "Building prod environment..."
	$(COMPOSE_PROD) build spark-base
	$(COMPOSE_PROD) build

up-prod:
	@echo "Starting prod environment..."
	$(COMPOSE_PROD) up -d

up-logs-prod:
	$(COMPOSE_PROD) up

logs-prod:
	$(COMPOSE_PROD) logs -f

down-prod:
	@echo "Stopping prod environment (volumes preserved)..."
	$(COMPOSE_PROD) down

down-clean-prod:
	@echo "WARNING: Stopping prod and removing volumes..."
	@printf "Are you sure? [y/N] " && read ans && [ $${ans:-N} = y ]
	$(COMPOSE_PROD) down -v

rebuild-prod: down-prod build-prod up-prod

rebuild-dagster-prod:
	@echo "Rebuilding Dagster (prod)..."
	$(COMPOSE_PROD) build dagster
	$(COMPOSE_PROD) up -d --no-deps dagster

rebuild-dashboards-prod:
	@echo "Rebuilding dashboards (prod)..."
	$(COMPOSE_PROD) build custom-dashboards
	$(COMPOSE_PROD) up -d --no-deps custom-dashboards

rebuild-spark-prod:
	@echo "Rebuilding Spark (prod)..."
	$(COMPOSE_PROD) build spark-base spark-master spark-worker
	$(COMPOSE_PROD) up -d --no-deps spark-master spark-worker

restart-dagster-prod:
	$(COMPOSE_PROD) restart dagster

restart-dashboards-prod:
	$(COMPOSE_PROD) restart custom-dashboards

restart-caddy-prod:
	$(COMPOSE_PROD) restart caddy