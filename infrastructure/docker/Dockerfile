# Use a Python image with uv pre-installed
FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

# Set working directory
WORKDIR /app

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy project definition
COPY pyproject.toml uv.lock ./

# Install dependencies
# We use --frozen to ensure strict adherence to lock file
RUN uv sync --frozen --no-install-project

# Copy the rest of the project
# We copy src specifically as they are the entry points
COPY src/ ./src/
COPY README.md .

# Create directory for data mount
# This is where the PVC will be mounted
RUN mkdir -p /app/data

# Verify structure (optional, valid for debugging)
RUN ls -la /app/src

# Set the environment variable for python path
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV PATH="/app/.venv/bin:$PATH"

# Default command (can be overridden by Kubernetes Job)
CMD ["python", "--version"]
