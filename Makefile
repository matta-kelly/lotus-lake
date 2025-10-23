.PHONY: help build build-logs up up-logs down down-clean logs rebuild rebuild-service

# Environment: dev or prod (default: dev for safety)
ENV ?= dev
COMPOSE = docker compose -f infra/docker-compose.$(ENV).yml

# Display available commands
help:
	@echo "Lotus Data Stack - Makefile Commands"
	@echo ""
	@echo "Environment: ENV=$(ENV) (use 'make ENV=prod <target>' for production)"
	@echo ""
	@echo "Main Commands:"
	@echo "  make build              Build all images (keeps volumes)"
	@echo "  make up                 Start all services"
	@echo "  make down               Stop services (keeps volumes)"
	@echo "  make down-clean         Stop services AND remove volumes"
	@echo "  make logs               Follow all logs"
	@echo "  make rebuild            Full rebuild (keeps volumes)"
	@echo ""
	@echo "Service-Specific Commands:"
	@echo "  make rebuild-dagster    Rebuild just Dagster"
	@echo "  make rebuild-dashboards Rebuild just custom dashboards"
	@echo "  make rebuild-spark      Rebuild Spark (base, master, worker)"
	@echo "  make restart-dagster    Restart Dagster without rebuild"
	@echo "  make restart-dashboards Restart dashboards without rebuild"
	@echo ""
	@echo "Examples:"
	@echo "  make ENV=dev up         Start dev environment"
	@echo "  make ENV=prod up        Start production environment"
	@echo "  make ENV=prod rebuild-dagster  Rebuild prod Dagster"

# Build base first, then everything else
build:
	@echo "Building for $(ENV) environment..."
	$(COMPOSE) build spark-base
	$(COMPOSE) build

# Build + start + follow logs
build-logs:
	$(COMPOSE) build spark-base
	$(COMPOSE) build
	$(COMPOSE) up
	$(COMPOSE) logs -f

# Start all services
up:
	@echo "Starting $(ENV) environment..."
	$(COMPOSE) up -d

# Start + follow logs
up-logs:
	$(COMPOSE) up
	$(COMPOSE) logs -f

# Stop services (keeps volumes)
down:
	@echo "Stopping $(ENV) environment (volumes preserved)..."
	$(COMPOSE) down

# Stop services AND remove volumes (destructive!)
down-clean:
	@echo "WARNING: Stopping $(ENV) and removing volumes..."
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		$(COMPOSE) down -v; \
	fi

# Follow logs
logs:
	$(COMPOSE) logs -f

# Full rebuild (keeps volumes)
rebuild: down build up
	@echo "Rebuild complete for $(ENV)"

# Rebuild specific services
rebuild-dagster:
	@echo "Rebuilding Dagster for $(ENV)..."
	$(COMPOSE) build dagster
	$(COMPOSE) up -d --no-deps dagster

rebuild-dashboards:
	@echo "Rebuilding custom dashboards for $(ENV)..."
	$(COMPOSE) build custom-dashboards
	$(COMPOSE) up -d --no-deps custom-dashboards

rebuild-spark:
	@echo "Rebuilding Spark stack for $(ENV)..."
	$(COMPOSE) build spark-base spark-master spark-worker
	$(COMPOSE) up -d --no-deps spark-master spark-worker

# Restart without rebuild (fast iteration)
restart-dagster:
	$(COMPOSE) restart dagster

restart-dashboards:
	$(COMPOSE) restart custom-dashboards

restart-caddy:
	$(COMPOSE) restart caddy