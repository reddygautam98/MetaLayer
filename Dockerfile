# Multi-stage Production MetaLayer Dockerfile - PostgreSQL Architecture

# Stage 1: Build Dependencies
FROM quay.io/astronomer/astro-runtime:12.1.0-base AS builder

# Switch to root for dependency installation
USER root

# Install build dependencies
COPY packages.txt .
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    apt-transport-https \
    gnupg \
    ca-certificates \
    build-essential \
    && cat packages.txt | xargs apt-get install -y --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* /root/.cache

# Install Python dependencies with security and performance optimizations
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir --compile --no-deps -r requirements.txt && \
    pip check && \
    rm -rf /root/.cache/pip

# Stage 2: Production Image
FROM quay.io/astronomer/astro-runtime:12.1.0-base AS production

# Security: Run as non-root user
USER root

# Install only runtime dependencies (no build tools)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Copy Python packages from builder stage
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy project files with proper structure and security
COPY --chown=astro:astro dags/ /opt/airflow/dags/
COPY --chown=astro:astro include/ /opt/airflow/include/
COPY --chown=astro:astro plugins/ /opt/airflow/plugins/

# Create necessary directories with proper permissions and security
RUN mkdir -p /opt/airflow/logs /opt/airflow/data /opt/airflow/config /opt/airflow/temp /home/astro/.cache && \
    chown -R astro:astro /opt/airflow/ /home/astro && \
    chmod -R 755 /opt/airflow/ && \
    chmod 700 /home/astro

# Add comprehensive production health check
HEALTHCHECK --interval=30s --timeout=15s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/health || \
        (ps aux | grep -v grep | grep -q airflow && exit 0 || exit 1)

# Switch back to astro user for security
USER astro

# Set working directory
WORKDIR /opt/airflow

# Set production environment variables with security focus
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true \
    AIRFLOW__CORE__LOAD_EXAMPLES=false \
    AIRFLOW__WEBSERVER__EXPOSE_CONFIG=false \
    AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=8 \
    AIRFLOW__CORE__PARALLELISM=64 \
    AIRFLOW__CORE__DAG_CONCURRENCY=32 \
    AIRFLOW__CORE__SECURITY=true \
    PYTHONPATH=/opt/airflow \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Default production command
CMD ["airflow", "webserver"]
