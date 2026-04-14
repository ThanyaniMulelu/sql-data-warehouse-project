FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    freetds-dev freetds-bin && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
USER airflow

RUN pip install --no-cache-dir pymssql

# Copy SQL scripts into the image
COPY --chown=airflow:root scripts/ /opt/airflow/sql/scripts/
COPY --chown=airflow:root tests/  /opt/airflow/sql/tests/
