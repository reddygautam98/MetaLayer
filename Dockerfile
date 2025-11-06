# =====================================================
# ETL PIPELINE - OPTIMIZED MULTI-STAGE BUILD
# Apache Airflow 2.8.1 with Python 3.11 (Stable Version)
# =====================================================

ARG AIRFLOW_VERSION=2.8.1
ARG PYTHON_VERSION="3.11"

# =====================================================
# STAGE 1: Dependencies Installation
# =====================================================
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION} AS dependencies

USER airflow
COPY --chown=airflow:root requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt

# =====================================================
# STAGE 2: Production Image
# =====================================================
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

# =====================================================
# METADATA & LABELS
# =====================================================
LABEL maintainer="ETL Pipeline Team"
LABEL version="1.0.0"
LABEL description="Production ETL Pipeline with Airflow, PostgreSQL, and Monitoring"

# =====================================================
# ENVIRONMENT VARIABLES
# =====================================================
ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH="${PYTHONPATH}:${AIRFLOW_HOME}/include"
ENV ETL_ENVIRONMENT=docker
ENV PYTHONUNBUFFERED=1

# =====================================================
# SYSTEM DEPENDENCIES
# =====================================================
USER root

# Install system packages for ETL processing
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    netcat-openbsd \
    postgresql-client \
    procps \
    unzip \
    wget \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# =====================================================
# COPY DEPENDENCIES FROM STAGE 1
# =====================================================
USER airflow

# Copy installed packages from dependencies stage
COPY --from=dependencies --chown=airflow:root /home/airflow/.local /home/airflow/.local

# =====================================================
# ETL PIPELINE CODE
# =====================================================
# Copy DAGs
COPY --chown=airflow:root dags/ ${AIRFLOW_HOME}/dags/

# Copy include directory (utilities, SQL, monitoring)
COPY --chown=airflow:root include/ ${AIRFLOW_HOME}/include/

# Copy plugins
COPY --chown=airflow:root plugins/ ${AIRFLOW_HOME}/plugins/

# Copy configuration files
COPY --chown=airflow:root config/ ${AIRFLOW_HOME}/config/

# Copy test data
COPY --chown=airflow:root data/ ${AIRFLOW_HOME}/data/

# Copy scripts
COPY --chown=airflow:root scripts/ ${AIRFLOW_HOME}/scripts/

# Create tests directory (tests will be mounted via volume)
RUN mkdir -p ${AIRFLOW_HOME}/tests/

# =====================================================
# SETUP PERMISSIONS
# =====================================================
USER root

# Set proper ownership and permissions
RUN chown -R airflow:root ${AIRFLOW_HOME} && \
    chmod +x ${AIRFLOW_HOME}/scripts/health_check.py

USER airflow

# Set working directory
WORKDIR ${AIRFLOW_HOME}

# Expose ports
EXPOSE 8080 5555 8793

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=60s --retries=3 \
    CMD python ${AIRFLOW_HOME}/scripts/health_check.py || exit 1

# Default command
CMD ["airflow", "webserver"]