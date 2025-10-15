.PHONY: build build-logs up up-logs down logs rebuild

COMPOSE = docker compose -f infra/docker-compose.yml

# Build base first, then everything else
build:
	$(COMPOSE) build spark-base
	$(COMPOSE) build

# Build in correct order + bring up + logs
build-logs:
	$(COMPOSE) build spark-base
	$(COMPOSE) build
	$(COMPOSE) up
	$(COMPOSE) logs -f

# Bring up the stack
up:
	$(COMPOSE) up -d

# Bring up + follow logs immediately
up-logs:
	$(COMPOSE) up
	$(COMPOSE) logs -f

# Tear down the stack
down:
	$(COMPOSE) down -v

# Follow logs
logs:
	$(COMPOSE) logs -f

# Clean rebuild (force fresh images)
rebuild: down build up logs