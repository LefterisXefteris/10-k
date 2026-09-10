#!/usr/bin/env bash
set -euo pipefail

airflow db migrate

airflow users create \
  --username admin \
  --password admin \
  --firstname Lefteris \
  --lastname Gilmaz \
  --role Admin \
  --email lefterisgilmaz@gmail.com \
  || true

airflow scheduler &

# Wait until the DAG is parsed, then run the pipeline once so compose up
# loads parquet into DuckDB without waiting for the daily schedule.
(
  for _ in $(seq 1 30); do
    if airflow dags list 2>/dev/null | grep -q sec_ai_pipeline; then
      airflow dags unpause sec_ai_pipeline || true
      airflow dags trigger sec_ai_pipeline
      exit 0
    fi
    sleep 2
  done
  echo "sec_ai_pipeline was not parsed in time; trigger it from the UI"
) &

exec airflow webserver --port 8080
