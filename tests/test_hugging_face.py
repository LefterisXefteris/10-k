import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


class FakeHttp:
    def __init__(self, payload):
        self._body = json.dumps(payload).encode()

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


def write_companies(path, companies):
    pq.write_table(pa.table({"company": companies}), path)


def test_company_tickers_skips_cik_and_splits_symbols(hf):
    company = "Alphabet Inc.  (GOOG, GOOGL)  (CIK 0001652044)"
    assert hf.company_tickers(company) == ["GOOG", "GOOGL"]


def test_clean_company_name_drops_legal_suffix_ticker_and_cik(hf):
    snowflake = "Snowflake Inc.  (SNOW)  (CIK 0001640147)"
    amazon = "AMAZON COM INC  (AMZN)  (CIK 0001018723)"
    assert hf.clean_company_name(snowflake) == "Snowflake"
    assert hf.clean_company_name(amazon) == "AMAZON"


def test_clean_company_name_does_not_strip_com_from_qualcomm(hf):
    assert hf.clean_company_name("QUALCOMM INC  (QCOM)  (CIK 0000804328)") == "QUALCOMM"


def test_orgs_to_try_uses_known_ticker_mapping(hf):
    meta = "Meta Platforms, Inc.  (META)  (CIK 0001326801)"
    assert hf.orgs_to_try(meta) == ["facebook"]


def test_orgs_to_try_skips_multi_word_and_short_names(hf):
    allegro = "Allegro Microsystems, Inc.  (ALGM)  (CIK 0000866291)"
    tiny = "AI Inc.  (AI)  (CIK 0000000001)"
    assert hf.orgs_to_try(allegro) == []
    assert hf.orgs_to_try(tiny) == []


def test_orgs_to_try_title_cases_all_caps_single_word(hf):
    microsoft = "MICROSOFT CORP  (MSFT)  (CIK 0000789019)"
    assert hf.orgs_to_try(microsoft) == ["Microsoft", "microsoft"]


def test_unique_orgs_keeps_first_seen_order(hf):
    company_orgs = [
        ("A", ["Microsoft", "microsoft"]),
        ("B", ["Microsoft"]),
        ("C", ["google"]),
    ]
    assert hf.unique_orgs(company_orgs) == ["Microsoft", "microsoft", "google"]


def test_models_to_rows_flattens_tags(hf):
    rows = hf.models_to_rows(
        "Snowflake Inc.",
        "Snowflake",
        [
            {
                "id": "Snowflake/arctic",
                "downloads": 10,
                "likes": 2,
                "pipeline_tag": "text-generation",
                "library_name": "transformers",
                "createdAt": "2024-01-01T00:00:00.000Z",
                "tags": ["nlp", "llm"],
            }
        ],
    )
    assert rows == [
        {
            "company": "Snowflake Inc.",
            "org": "Snowflake",
            "model_id": "Snowflake/arctic",
            "downloads": 10,
            "likes": 2,
            "pipeline_tag": "text-generation",
            "library_name": "transformers",
            "created_at": "2024-01-01T00:00:00.000Z",
            "tags": "nlp; llm",
        }
    ]


def test_fetch_models_keeps_only_this_orgs_models(hf, monkeypatch):
    payload = [
        {"id": "Snowflake/arctic"},
        {"id": "someone-else/arctic"},
        {"id": "snowflake/wrong-case"},
    ]
    monkeypatch.setattr(hf, "urlopen", lambda *_a, **_k: FakeHttp(payload))
    models = hf.fetch_models("Snowflake")
    assert [m["id"] for m in models] == ["Snowflake/arctic"]


def test_distinct_companies_returns_sorted_unique_names(hf):
    write_companies(
        hf.PARQUET_IN,
        ["Snowflake Inc.", "", "Microsoft Corp", "Snowflake Inc."],
    )
    assert hf.distinct_companies() == ["Microsoft Corp", "Snowflake Inc."]


def test_distinct_companies_raises_when_parquet_is_missing(hf):
    with pytest.raises(FileNotFoundError, match="Missing"):
        hf.distinct_companies()


def test_write_rows_writes_empty_parquet_with_schema(hf):
    hf.write_rows([])
    table = pq.read_table(hf.PARQUET_OUT)
    assert table.num_rows == 0
    assert table.schema == hf.SCHEMA


def test_main_writes_models_for_matching_org_only(hf, monkeypatch):
    write_companies(
        hf.PARQUET_IN,
        [
            "Snowflake Inc.  (SNOW)  (CIK 0001640147)",
            "Allegro Microsystems, Inc.  (ALGM)  (CIK 0000866291)",
        ],
    )

    def fake_urlopen(req, timeout=30):
        assert "Snowflake" in req.full_url
        return FakeHttp(
            [
                {
                    "id": "Snowflake/arctic",
                    "downloads": 5,
                    "likes": 1,
                    "pipeline_tag": "text-generation",
                    "library_name": "transformers",
                    "createdAt": "2024-01-01T00:00:00.000Z",
                    "tags": ["llm"],
                }
            ]
        )

    monkeypatch.setattr(hf, "urlopen", fake_urlopen)
    hf.main()

    rows = pq.read_table(hf.PARQUET_OUT).to_pylist()
    assert len(rows) == 1
    assert rows[0]["org"] == "Snowflake"
    assert rows[0]["model_id"] == "Snowflake/arctic"


def test_fetch_orgs_keeps_going_when_one_org_fails(hf, monkeypatch):
    def fake_urlopen(req, timeout=30):
        if "BadOrg" in req.full_url:
            raise OSError("hub down")
        return FakeHttp([{"id": "GoodOrg/model"}])

    monkeypatch.setattr(hf, "urlopen", fake_urlopen)
    result = hf.fetch_orgs(["BadOrg", "GoodOrg"])
    assert result["BadOrg"] == []
    assert [m["id"] for m in result["GoodOrg"]] == ["GoodOrg/model"]
