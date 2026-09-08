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
exec airflow webserver --port 8080
