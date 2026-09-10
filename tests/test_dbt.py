import json
import os
import subprocess
import sys
from pathlib import Path

import duckdb
import hugging_face
import pyarrow as pa
import pyarrow.parquet as pq
import sec_10k

ROOT = Path(__file__).resolve().parent.parent

NEXTERA_COMPANY = (
    "NEXTERA ENERGY INC  (NEE, NEE-PN)  (CIK 0000753308), "
    "FLORIDA POWER & LIGHT CO  (CIK 0000037634)"
)
SNOWFLAKE_COMPANY = "Snowflake Inc.  (SNOW)  (CIK 0001640147)"

SEC_ROWS = [
    {
        "file_id": "nextera-10k",
        "company": NEXTERA_COMPANY,
        "cik": "0000753308, 0000037634",
        "form": "10-K",
        "file_date": "2026-01-15",
        "period_ending": "2025-12-31",
        "location": "Juno Beach, FL",
        "accession": "0000753308-26-000001",
    }
]

HF_ROWS = [
    {
        "company": SNOWFLAKE_COMPANY,
        "org": "Snowflake",
        "model_id": "Snowflake/arctic",
        "downloads": 100,
        "likes": 10,
        "pipeline_tag": "text-generation",
        "library_name": "transformers",
        "created_at": "2024-01-01T00:00:00.000Z",
        "tags": "nlp; llm",
    },
    {
        "company": SNOWFLAKE_COMPANY,
        "org": "Snowflake",
        "model_id": "Snowflake/arctic",
        "downloads": 5,
        "likes": 1,
        "pipeline_tag": "text-generation",
        "library_name": "transformers",
        "created_at": "2024-01-01T00:00:00.000Z",
        "tags": "nlp; llm",
    },
    {
        "company": "No Org Inc.  (CIK 0000000001)",
        "org": "",
        "model_id": "nobody/model",
        "downloads": 999,
        "likes": 0,
        "pipeline_tag": None,
        "library_name": None,
        "created_at": None,
        "tags": "",
    },
]


def _write_landing(landing_dir):
    landing_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pylist(SEC_ROWS, schema=sec_10k.SCHEMA),
        landing_dir / "sec_ai_10k.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist(HF_ROWS, schema=hugging_face.SCHEMA),
        landing_dir / "hf_models.parquet",
    )


def test_dbt_build_passes_schema_tests_on_fixture_landing(tmp_path):
    landing_dir = tmp_path / "landing"
    warehouse = tmp_path / "ci.duckdb"
    _write_landing(landing_dir)

    dbt = Path(sys.executable).parent / "dbt"
    result = subprocess.run(
        [
            str(dbt),
            "build",
            "--project-dir",
            str(ROOT / "transform"),
            "--profiles-dir",
            str(ROOT / "transform"),
            "--target-path",
            str(tmp_path / "dbt_target"),
            "--vars",
            json.dumps({"landing_dir": str(landing_dir)}),
        ],
        cwd=ROOT,
        env={**os.environ, "DBT_DUCKDB_PATH": str(warehouse)},
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + "\n" + result.stderr

    con = duckdb.connect(str(warehouse), read_only=True)
    companies = {
        row[0]: row[1]
        for row in con.execute("select cik, name from sec_ai.companies").fetchall()
    }
    assert companies["0000753308"].startswith("NEXTERA ENERGY")
    assert companies["0000037634"].startswith("FLORIDA POWER")
    assert companies["0001640147"].startswith("Snowflake")

    filing_ciks = {
        row[0]
        for row in con.execute(
            "select cik from sec_ai.filing_companies where file_id = 'nextera-10k'"
        ).fetchall()
    }
    assert filing_ciks == {"0000753308", "0000037634"}

    tickers = {
        row[0]
        for row in con.execute("select ticker from sec_ai.company_tickers").fetchall()
    }
    assert tickers == {"NEE", "NEE-PN"}

    models = con.execute(
        "select model_id, cik, org, downloads from sec_ai.models"
    ).fetchall()
    assert models == [("Snowflake/arctic", "0001640147", "Snowflake", 100)]

    tags = {
        row[0]
        for row in con.execute(
            "select tag from sec_ai.model_tags where model_id = 'Snowflake/arctic'"
        ).fetchall()
    }
    assert tags == {"nlp", "llm"}
    con.close()
