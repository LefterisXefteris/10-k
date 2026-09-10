# SEC AI 10-K

Pipeline that collects SEC 10-K filings mentioning artificial intelligence, matches companies to Hugging Face models, and loads a DuckDB warehouse with dbt.

```
extract  →  data/*.parquet  →  dbt  →  transform/warehouse.duckdb
```

```
.
├── extract/          SEC and Hugging Face extractors
├── data/             landing parquet files
├── transform/        dbt models (DuckDB warehouse)
├── api/              FastAPI service over the filings parquet
├── airflow/          DAG: extract → compact → Hugging Face → dbt
└── docker-compose.yml
```

## Docker pipeline

```bash
docker compose up --build
```

On startup Airflow triggers `sec_ai_pipeline`:

1. Fetch new SEC 10-K hits into a dated parquet under `data/`
2. Compact dated extracts into `data/sec_ai_10k.parquet`
3. Match companies to Hugging Face models → `data/hf_models.parquet`
4. `dbt run` then `dbt test` into `transform/warehouse.duckdb`

The DAG also runs daily. Airflow UI: http://localhost:8080 (admin / admin). API: http://localhost:8000.

Hugging Face can take a while on the first run. Limit it while testing with `HF_MAX_COMPANIES=5` in the Airflow service environment.

## Extract (local)

```bash
python extract/10-k.py
python extract/hugging_face.py
```

Landing files are written to `data/`. `10-k.py` also rebuilds `data/sec_ai_10k.parquet` for dbt.

## Transform (local)

```bash
cd transform
dbt run --profiles-dir .
dbt test --profiles-dir .
```

Inspect the warehouse:

```bash
duckdb transform/warehouse.duckdb -f test.sql
```
