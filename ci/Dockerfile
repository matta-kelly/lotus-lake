# ==============================================================================
# Lotus Lake CI Image
# ==============================================================================
# Custom image with all CI tools pre-installed.
# Built and pushed to Forgejo registry.
#
# Build: docker build -t forgejo.datamountainsolutions.com/mkultra/lotus-lake-ci:latest ci/
# Push:  docker push forgejo.datamountainsolutions.com/mkultra/lotus-lake-ci:latest
# ==============================================================================

FROM python:3.11-bookworm

# Versions (update here when upgrading)
ENV HELM_VERSION=3.14.0
ENV TERRAFORM_VERSION=1.5.7

# System packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    git \
    jq \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Docker CLI (for docker build/push in CI jobs, connects to DinD sidecar)
RUN curl -fsSL https://get.docker.com | sh

# Helm
RUN curl -fsSL -o /tmp/helm.tar.gz \
    "https://get.helm.sh/helm-v${HELM_VERSION}-linux-amd64.tar.gz" \
    && tar -zxf /tmp/helm.tar.gz -C /tmp \
    && mv /tmp/linux-amd64/helm /usr/local/bin/helm \
    && rm -rf /tmp/helm.tar.gz /tmp/linux-amd64 \
    && helm version

# Terraform (for orchestration/airbyte/terraform validation)
RUN curl -fsSL -o /tmp/terraform.zip \
    "https://releases.hashicorp.com/terraform/${TERRAFORM_VERSION}/terraform_${TERRAFORM_VERSION}_linux_amd64.zip" \
    && unzip /tmp/terraform.zip -d /usr/local/bin \
    && rm /tmp/terraform.zip \
    && terraform version

# Python packages (from requirements-ci.txt)
COPY requirements-ci.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements-ci.txt \
    && rm /tmp/requirements-ci.txt

# Verify installations
RUN python --version && pip --version && helm version --short && dbt --version && terraform version

# Default working directory
WORKDIR /workspace
