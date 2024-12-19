FROM quay.io/astronomer/astro-runtime:12.3.0

USER root

RUN apt-get update && \
    apt-get install -y python3-venv python3-dev libpq-dev gcc && \
    python3 -m venv /home/astro/sqla && \
    source /home/astro/sqla/bin/activate && \
    pip install --upgrade pip && \
    pip install --no-cache-dir \
    SQLAlchemy==2.0.36 \
    psycopg2 && \
    deactivate && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV AIRFLOW_VAR_POSTGRES_CONN='{"host": "host.docker.internal", "port": 5433, "user": "postgres", "password": "1234", "database": "picoles"}'

ENV AIRFLOW_CONN_POSTGRES_PICOLES='postgresql+psycopg2://postgres:1234@host.docker.internal:5433/picoles'
