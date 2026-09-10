from pathlib import Path

import yaml

import hugging_face
import sec_10k

ROOT = Path(__file__).resolve().parent.parent
SOURCES = ROOT / "transform" / "models" / "sources.yml"


def _source_table(name):
    sources = yaml.safe_load(SOURCES.read_text())
    landing = next(source for source in sources["sources"] if source["name"] == "landing")
    return next(table for table in landing["tables"] if table["name"] == name)


def test_sec_filings_columns_match_extractor():
    columns = [column["name"] for column in _source_table("sec_filings")["columns"]]
    assert columns == sec_10k.COLUMNS


def test_hf_models_columns_match_extractor():
    columns = [column["name"] for column in _source_table("hf_models")["columns"]]
    assert columns == hugging_face.COLUMNS
