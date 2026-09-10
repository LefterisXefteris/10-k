FROM apache/airflow:2.10.5-python3.12

USER root
RUN mkdir -p /opt/airflow/dags /opt/airflow/data /opt/airflow/extract /opt/airflow/transform \
    && chown -R airflow:0 /opt/airflow/dags /opt/airflow/data /opt/airflow/extract /opt/airflow/transform

COPY --chown=airflow:0 entrypoint.sh /entrypoint-local.sh
RUN chmod +x /entrypoint-local.sh

USER airflow

ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False
ENV AIRFLOW__CORE__EXECUTOR=SequentialExecutor
ENV AIRFLOW__WEBSERVER__SECRET_KEY=local-dev-secret-change-on-deploy
ENV DATA_DIR=/opt/airflow/data
ENV EXTRACT_DIR=/opt/airflow/extract
ENV TRANSFORM_DIR=/opt/airflow/transform
ENV DBT_DUCKDB_PATH=/opt/airflow/transform/warehouse.duckdb
ENV PYTHONPATH=/opt/airflow/extract
RUN pip install --no-cache-dir pyarrow requests "dbt-duckdb>=1.9,<2"

EXPOSE 8080
ENTRYPOINT ["/entrypoint-local.sh"]
