.PHONY: discover generate apply plan

# Load .env vars
include .env
export

# Interactive: select which source to discover
discover:
	@cd orchestration/resources/airbyte && python3 discover-source.py

# Wipe + regenerate all catalogs
generate:
	@cd orchestration/resources/airbyte && python3 generate-catalog.py

# Generate + terraform plan
plan: generate
	@cd orchestration/resources/airbyte/terraform && terraform plan

# Generate + terraform apply
apply: generate
	@cd orchestration/resources/airbyte/terraform && terraform apply -auto-approve
