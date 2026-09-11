# Custom Airflow image for the Aurelia data platform.
# Extends the official Airflow image with:
#   - Java (needed by PySpark)
#   - Python deps (PySpark, Great Expectations, MinIO client, httpx, ...)
#
# Build context is the repo root (see docker-compose.yml).

FROM apache/airflow:2.9.3-python3.11

USER root
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        default-jre-headless \
        procps \
        curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# JRE ships a distro-agnostic symlink at /usr/lib/jvm/default-java on Debian.
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
COPY airflow/requirements.txt /tmp/aurelia-requirements.txt
RUN pip install --no-cache-dir -r /tmp/aurelia-requirements.txt
