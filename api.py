import os
from pathlib import Path

import pyarrow.parquet as pq
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

app = FastAPI(title="SEC AI 10-K")
PARQUET_PATH = Path(os.environ.get("PARQUET_PATH", "/data/sec_ai_10k.parquet"))


def read_rows():
    if not PARQUET_PATH.exists():
        return []
    return pq.read_table(PARQUET_PATH).to_pylist()


@app.get("/")
def root():
    return {
        "message": "SEC 10-K filings that mention artificial intelligence",
        "endpoints": ["/data", "/parquet", "/health"],
    }


@app.get("/health")
def health():
    rows = read_rows()
    return {"ok": PARQUET_PATH.exists(), "rows": len(rows), "path": str(PARQUET_PATH)}


@app.get("/data")
def data():
    return read_rows()


@app.get("/parquet")
def parquet_file():
    if not PARQUET_PATH.exists():
        raise HTTPException(status_code=404, detail="Parquet file not found yet")
    return FileResponse(
        PARQUET_PATH,
        media_type="application/vnd.apache.parquet",
        filename="sec_ai_10k.parquet",
    )
