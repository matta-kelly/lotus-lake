# ==============================================================================
# lotus-lake Dagster User Code Image
# ==============================================================================

FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY pyproject.toml .
RUN pip install --upgrade pip && pip install --no-cache-dir .

# Copy orchestration code
COPY orchestration/ orchestration/

# Generate dbt manifest at build time
RUN cd orchestration && dbt parse --profiles-dir . --project-dir .

# Dagster gRPC server
EXPOSE 3030

CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "3030", "-m", "orchestration.definitions"]
