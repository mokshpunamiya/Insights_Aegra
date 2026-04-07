ARG PY_VERSION=3.12
FROM python:${PY_VERSION}-slim-bookworm AS base
ARG PY_VERSION

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100

WORKDIR /app

# Create non-root user for runtime
RUN addgroup --system app && adduser --system --ingroup app app

# -----------------------------
# Builder stage
# -----------------------------
FROM base AS builder

# Retrieve the uv binary directly from the official image.
COPY --from=ghcr.io/astral-sh/uv:0.9.26 /uv /bin/uv

# Install system build dependencies required for compiling Python extensions.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy the root workspace configuration
COPY pyproject.toml uv.lock ./

# Copy each project's configuration (needed for uv sync to resolve the workspace)
COPY libs/aegra-api/pyproject.toml libs/aegra-api/README.md ./libs/aegra-api/
COPY libs/aegra-cli/pyproject.toml libs/aegra-cli/README.md ./libs/aegra-cli/
COPY eerly_studio/pyproject.toml eerly_studio/README.md ./eerly_studio/

# Install all workspace dependencies without installing the projects themselves
RUN uv sync --all-packages --frozen --no-dev --no-install-workspace --compile-bytecode

# Copy the actual project source code, including alembic rules and tests
COPY libs/aegra-api/src/ ./libs/aegra-api/src/
COPY libs/aegra-api/alembic.ini ./libs/aegra-api/alembic.ini
COPY libs/aegra-api/alembic/ ./libs/aegra-api/alembic/
COPY libs/aegra-api/tests/ ./libs/aegra-api/tests/

COPY libs/aegra-cli/src/ ./libs/aegra-cli/src/
COPY libs/aegra-cli/tests/ ./libs/aegra-cli/tests/

COPY eerly_studio/ ./eerly_studio/

# Sync again to install the workspace projects themselves
RUN uv sync --all-packages --frozen --no-dev --compile-bytecode

# Let the final stage use the environment created by uv
ENV PATH="/app/.venv/bin:$PATH"

# -----------------------------
# Final, minimal runtime image
# -----------------------------
FROM base AS final

# Install minimal runtime libs and execute custom dependencies script
COPY deployments/docker/install_dependencies.sh /app/install_dependencies.sh
RUN chmod +x /app/install_dependencies.sh \
    && apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && /app/install_dependencies.sh \
    && rm -rf /var/lib/apt/lists/*

# Copy the virtual environment from the builder
COPY --from=builder /app/.venv /app/.venv

# Update PATH to use the virtual environment
ENV PATH="/app/.venv/bin:$PATH"

# Copy runtime assets required by the app and alembic
COPY libs/aegra-api/alembic.ini ./alembic.ini
COPY libs/aegra-api/alembic/ ./alembic/
COPY libs/aegra-api/tests/ ./tests/

COPY eerly_studio/ ./eerly_studio/
COPY aegra.json ./aegra.json
COPY examples/ ./examples/

EXPOSE 8000

# Run as non-root
USER app

# Default command - can be overridden by docker-compose
CMD ["uvicorn", "aegra_api.main:app", "--host", "0.0.0.0", "--port", "8000"]
