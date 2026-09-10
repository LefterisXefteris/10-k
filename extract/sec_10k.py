from datetime import datetime
from pathlib import Path
import os
import re
import time

import pyarrow as pa
import pyarrow.parquet as pq
import requests

DATA_DIR = Path(
    os.environ.get(
        "DATA_DIR",
        Path(__file__).resolve().parent.parent / "data",
    )
)
USER_AGENT = os.environ.get(
    "SEC_USER_AGENT",
    "Lefteris Gilmaz lefterisgilmaz@gmail.com",
)
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
DATED_PARQUET = re.compile(r"^sec_ai_10k_(\d{4}-\d{2}-\d{2})_\d{4}\.parquet$")
CANONICAL_PARQUET = DATA_DIR / "sec_ai_10k.parquet"


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


def extract_parquet_path(run_at):
    return DATA_DIR / f"sec_ai_10k_{run_at.strftime('%Y-%m-%d_%H%M')}.parquet"


def latest_extract_date():
    """Date of the newest dated extract. Ignores sec_ai_10k.parquet."""
    dates = []
    if not DATA_DIR.exists():
        return None
    for path in DATA_DIR.iterdir():
        match = DATED_PARQUET.match(path.name)
        if match:
            dates.append(match.group(1))
    if not dates:
        return None
    return max(dates)


def write_rows(path, rows):
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    pq.write_table(table, path)


def fetch_and_save_parquet(**context):
    """Write this run's SEC hits to a new parquet. Never touch existing files."""
    run_at = context.get("data_interval_start") or datetime.now()
    run_date = run_at.strftime("%Y-%m-%d")
    out_path = extract_parquet_path(run_at)
    if out_path.exists():
        print(f"{out_path} already exists, leaving it unchanged")
        return str(out_path)

    start_date = latest_extract_date() or run_date
    if start_date > run_date:
        start_date = run_date

    rows = hits_to_rows(fetch_hits(start_date, run_date))
    if not rows:
        print(f"No data for {start_date} to {run_date}, skipping parquet")
        return None

    write_rows(out_path, rows)
    print(f"Wrote {len(rows)} rows to {out_path} ({start_date} to {run_date})")
    return str(out_path)


def compact_sec_parquet(**context):
    """Merge dated extracts into data/sec_ai_10k.parquet for dbt."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    paths = []
    if CANONICAL_PARQUET.exists():
        paths.append(CANONICAL_PARQUET)
    dated = sorted(
        path
        for path in DATA_DIR.iterdir()
        if DATED_PARQUET.match(path.name)
    )
    paths.extend(dated)
    if not paths:
        raise FileNotFoundError(f"No SEC parquet files in {DATA_DIR}")

    by_file_id = {}
    for path in paths:
        for row in pq.read_table(path, schema=SCHEMA).to_pylist():
            file_id = row.get("file_id")
            if file_id:
                by_file_id[file_id] = row

    rows = list(by_file_id.values())
    if not rows:
        raise FileNotFoundError(f"No SEC rows to compact in {DATA_DIR}")

    write_rows(CANONICAL_PARQUET, rows)
    print(f"Compacted {len(rows)} filings into {CANONICAL_PARQUET}")
    return str(CANONICAL_PARQUET)


if __name__ == "__main__":
    fetch_and_save_parquet()
    compact_sec_parquet()
