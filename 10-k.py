from datetime import datetime, timedelta
from pathlib import Path
import os
import time
import requests
import pyarrow as pa
import pyarrow.parquet as pq

from airflow import DAG
from airflow.operators.python import PythonOperator

PARQUET_PATH = Path(
    os.environ.get(
        "PARQUET_PATH",
        Path(__file__).resolve().parent / "sec_ai_10k.parquet",
    )
)
USER_AGENT = "Lefteris Gilmaz lefterisgilmaz@gmail.com"
API_URL = "https://efts.sec.gov/LATEST/search-index"
COLUMNS = [
    "file_id",
    "company",
    "cik",
    "form",
    "file_date",
    "period_ending",
    "location",
    "accession",
]
SCHEMA = pa.schema([(name, pa.string()) for name in COLUMNS])


def fetch_hits(start_date, end_date):
    """Call the SEC API for one date range. Pages through results 100 at a time."""
    hits = []
    from_index = 0
    page_size = 100

    while True:
        response = None
        for attempt in range(3):
            response = requests.get(
                API_URL,
                params={
                    "q": '"artificial intelligence"',
                    "forms": "10-K",
                    "startdt": start_date,
                    "enddt": end_date,
                    "from": from_index,
                    "size": page_size,
                },
                headers={"User-Agent": USER_AGENT},
                timeout=30,
            )
            if response.status_code == 200:
                break
            time.sleep(2)

        response.raise_for_status()
        page = response.json().get("hits", {}).get("hits", [])
        if not page:
            break
        hits.extend(page)
        if len(page) < page_size:
            break
        from_index += page_size
        time.sleep(0.4)

    return hits


def hits_to_rows(hits):
    rows = []
    for hit in hits:
        src = hit.get("_source", {})
        rows.append(
            {
                "file_id": hit.get("_id"),
                "company": ", ".join(src.get("display_names") or []),
                "cik": ", ".join(src.get("ciks") or []),
                "form": src.get("form"),
                "file_date": src.get("file_date"),
                "period_ending": src.get("period_ending"),
                "location": ", ".join(src.get("biz_locations") or []),
                "accession": src.get("adsh"),
            }
        )
    return rows


def read_rows():
    if not PARQUET_PATH.exists():
        return []
    return pq.read_table(PARQUET_PATH).to_pylist()


def write_rows(rows):
    PARQUET_PATH.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows, schema=SCHEMA) if rows else SCHEMA.empty_table()
    pq.write_table(table, PARQUET_PATH)


def existing_file_ids():
    """Read file_ids already stored so we do not add the same row twice."""
    ids = set()
    for row in read_rows():
        ids.add(row["file_id"])
    return ids


def fetch_and_save_parquet(**context):
    today = datetime.now().strftime("%Y-%m-%d")

    if PARQUET_PATH.exists():
        # Later runs: only pull today's new filings and append them
        start_date = today
        already_have = existing_file_ids()
        existing = read_rows()
    else:
        # First run: pull everything from 2026-01-01 until today
        start_date = "2026-01-01"
        already_have = set()
        existing = []

    rows = hits_to_rows(fetch_hits(start_date, today))
    new_rows = [row for row in rows if row["file_id"] not in already_have]
    write_rows(existing + new_rows)

    print(f"Added {len(new_rows)} rows to {PARQUET_PATH}")


with DAG(
    dag_id="sec_ai_10k_daily",
    default_args={
        "owner": "Lefteris Gilmaz",
        "email": ["lefterisgilmaz@gmail.com"],
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Daily SEC 10-K filings that mention artificial intelligence",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    PythonOperator(
        task_id="fetch_sec_data",
        python_callable=fetch_and_save_parquet,
    )


if __name__ == "__main__":
    fetch_and_save_parquet()
