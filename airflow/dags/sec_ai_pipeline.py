from datetime import datetime, timedelta
import os
from pathlib import Path
import sys

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

EXTRACT_DIR = Path(os.environ.get("EXTRACT_DIR", "/opt/airflow/extract"))
TRANSFORM_DIR = os.environ.get("TRANSFORM_DIR", "/opt/airflow/transform")
DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data")
sys.path.insert(0, str(EXTRACT_DIR))

import hugging_face
import sec_10k

DBT_VARS = f'{{"landing_dir": "{DATA_DIR}"}}'

with DAG(
    dag_id="sec_ai_pipeline",
    default_args={
        "owner": "Lefteris Gilmaz",
        "email": ["lefterisgilmaz@gmail.com"],
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    description="Extract SEC and Hugging Face parquet, then load DuckDB with dbt",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
) as dag:
    extract_sec = PythonOperator(
        task_id="extract_sec",
        python_callable=sec_10k.fetch_and_save_parquet,
    )
    compact_sec = PythonOperator(
        task_id="compact_sec",
        python_callable=sec_10k.compact_sec_parquet,
    )
    extract_hf = PythonOperator(
        task_id="extract_hf",
        python_callable=hugging_face.main,
        execution_timeout=timedelta(hours=2),
    )
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd '{TRANSFORM_DIR}' && "
            f"dbt run --profiles-dir '{TRANSFORM_DIR}' --vars '{DBT_VARS}'"
        ),
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd '{TRANSFORM_DIR}' && "
            f"dbt test --profiles-dir '{TRANSFORM_DIR}' --vars '{DBT_VARS}'"
        ),
    )

    extract_sec >> compact_sec >> extract_hf >> dbt_run >> dbt_test
