FROM apache/airflow:2.10.5

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        libpq-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /

# Switch to airflow user before pip install
USER airflow

RUN pip install --no-cache-dir -r /requirements.txt
