FROM apache/airflow:2.10.3-python3.11

# dbt's dependency chain (protobuf, opentelemetry, etc.) conflicts with the
# versions already pinned in the Airflow image, so it's installed into its
# own virtualenv at build time instead of into Airflow's Python environment
# via _PIP_ADDITIONAL_REQUIREMENTS (which also re-resolves on every
# container start and is far too slow for this dependency set).
RUN python -m venv /home/airflow/dbt-venv && \
    /home/airflow/dbt-venv/bin/pip install --no-cache-dir \
        dbt-core==1.8.9 \
        dbt-postgres==1.8.2
