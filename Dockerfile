FROM apache/airflow:2.10.5-python3.12

USER root
RUN mkdir -p /opt/airflow/dags /opt/airflow/data \
    && chown -R airflow:0 /opt/airflow/dags /opt/airflow/data

COPY --chown=airflow:0 entrypoint.sh /entrypoint-local.sh
RUN chmod +x /entrypoint-local.sh

USER airflow

ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False
ENV AIRFLOW__CORE__EXECUTOR=SequentialExecutor
ENV AIRFLOW__WEBSERVER__SECRET_KEY=local-dev-secret-change-on-deploy
ENV PARQUET_PATH=/opt/airflow/data/sec_ai_10k.parquet
RUN pip install --no-cache-dir pyarrow

EXPOSE 8080
ENTRYPOINT ["/entrypoint-local.sh"]
