#!/bin/bash
set -euo pipefail

# ==============================================================================
# CI Tool Setup Script
# ==============================================================================
# Installs all tools needed for lotus-lake CI pipelines.
# Edit this file to add/update CI dependencies.
#
# Usage: ./ci/setup.sh [--python-only]
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HELM_VERSION="3.14.0"

echo "=== Installing system packages ==="
apt-get update
apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-venv \
    curl \
    ca-certificates

echo "=== Installing Python packages ==="
pip3 install --break-system-packages -r "$SCRIPT_DIR/requirements-ci.txt"

# Skip binary tools if --python-only flag
if [[ "${1:-}" == "--python-only" ]]; then
    echo "=== Skipping binary tools (--python-only) ==="
    exit 0
fi

echo "=== Installing Helm ${HELM_VERSION} ==="
curl -fsSL -o /tmp/helm.tar.gz "https://get.helm.sh/helm-v${HELM_VERSION}-linux-amd64.tar.gz"
tar -zxf /tmp/helm.tar.gz -C /tmp
mv /tmp/linux-amd64/helm /usr/local/bin/helm
rm -rf /tmp/helm.tar.gz /tmp/linux-amd64

echo "=== Verifying installations ==="
python3 --version
pip3 --version
helm version --short
dbt --version

echo "=== CI setup complete ==="
