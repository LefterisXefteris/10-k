from datetime import datetime

import pyarrow.parquet as pq
import pytest
import requests


class FakeResponse:
    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}")


def hit(file_id, company="Acme Inc.", cik="0000000001"):
    return {
        "_id": file_id,
        "_source": {
            "display_names": [company],
            "ciks": [cik],
            "form": "10-K",
            "file_date": "2026-01-15",
            "period_ending": "2025-12-31",
            "biz_locations": ["Boise, ID"],
            "adsh": f"adsh-{file_id}",
        },
    }


def write_sec_rows(sec, path, rows):
    sec.write_rows(path, rows)


def test_hits_to_rows_joins_list_fields(sec):
    rows = sec.hits_to_rows(
        [
            {
                "_id": "abc",
                "_source": {
                    "display_names": ["Acme Inc.  (ACME)", "Acme Holdings"],
                    "ciks": ["0001", "0002"],
                    "form": "10-K",
                    "file_date": "2026-01-15",
                    "period_ending": "2025-12-31",
                    "biz_locations": ["Boise, ID", "Austin, TX"],
                    "adsh": "0001-adsh",
                },
            }
        ]
    )
    assert rows == [
        {
            "file_id": "abc",
            "company": "Acme Inc.  (ACME), Acme Holdings",
            "cik": "0001, 0002",
            "form": "10-K",
            "file_date": "2026-01-15",
            "period_ending": "2025-12-31",
            "location": "Boise, ID, Austin, TX",
            "accession": "0001-adsh",
        }
    ]


def test_extract_parquet_path_uses_run_timestamp(sec):
    run_at = datetime(2026, 3, 4, 15, 7)
    assert sec.extract_parquet_path(run_at).name == "sec_ai_10k_2026-03-04_1507.parquet"


def test_latest_extract_date_ignores_canonical_and_returns_newest(sec, data_dir):
    (data_dir / "sec_ai_10k.parquet").write_bytes(b"not-a-dated-extract")
    (data_dir / "sec_ai_10k_2024-01-01_1200.parquet").write_bytes(b"old")
    (data_dir / "sec_ai_10k_2024-06-01_0000.parquet").write_bytes(b"new")
    assert sec.latest_extract_date() == "2024-06-01"


def test_latest_extract_date_is_none_when_folder_is_missing(sec, tmp_path, monkeypatch):
    monkeypatch.setattr(sec, "DATA_DIR", tmp_path / "missing")
    assert sec.latest_extract_date() is None


def test_write_rows_skips_empty_input(sec, data_dir):
    path = data_dir / "sec_ai_10k_2026-01-01_0000.parquet"
    sec.write_rows(path, [])
    assert not path.exists()


def test_fetch_hits_retries_until_success(sec, monkeypatch):
    calls = {"n": 0}

    def fake_get(*_args, **_kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return FakeResponse(503)
        return FakeResponse(200, {"hits": {"hits": [hit("1")]}})

    monkeypatch.setattr(sec.requests, "get", fake_get)
    hits = sec.fetch_hits("2026-01-01", "2026-01-02")
    assert [item["_id"] for item in hits] == ["1"]
    assert calls["n"] == 2


def test_fetch_hits_pages_until_a_short_page(sec, monkeypatch):
    pages = [
        [hit(str(i)) for i in range(100)],
        [hit("last")],
    ]

    def fake_get(*_args, **kwargs):
        from_index = kwargs["params"]["from"]
        page = pages[from_index // 100]
        return FakeResponse(200, {"hits": {"hits": page}})

    monkeypatch.setattr(sec.requests, "get", fake_get)
    hits = sec.fetch_hits("2026-01-01", "2026-01-02")
    assert len(hits) == 101
    assert hits[-1]["_id"] == "last"


def test_fetch_and_save_parquet_leaves_existing_file_unchanged(sec, monkeypatch):
    run_at = datetime(2026, 1, 2, 10, 30)
    path = sec.extract_parquet_path(run_at)
    existing = [
        {
            "file_id": "keep-me",
            "company": "Old Co",
            "cik": "0001",
            "form": "10-K",
            "file_date": "2026-01-01",
            "period_ending": "2025-12-31",
            "location": "Boise, ID",
            "accession": "old",
        }
    ]
    write_sec_rows(sec, path, existing)
    monkeypatch.setattr(sec, "fetch_hits", lambda *_a, **_k: pytest.fail("should not fetch"))

    result = sec.fetch_and_save_parquet(data_interval_start=run_at)
    assert result == str(path)
    assert pq.read_table(path).to_pylist() == existing


def test_fetch_and_save_parquet_skips_write_when_api_returns_nothing(sec, monkeypatch):
    run_at = datetime(2026, 1, 2, 10, 30)
    monkeypatch.setattr(sec, "fetch_hits", lambda *_a, **_k: [])
    result = sec.fetch_and_save_parquet(data_interval_start=run_at)
    assert result is None
    assert not sec.extract_parquet_path(run_at).exists()


def test_fetch_and_save_parquet_writes_hits_for_the_run_date(sec, monkeypatch):
    run_at = datetime(2026, 1, 2, 10, 30)
    monkeypatch.setattr(sec, "fetch_hits", lambda *_a, **_k: [hit("abc", "Snowflake Inc.")])

    result = sec.fetch_and_save_parquet(data_interval_start=run_at)
    path = sec.extract_parquet_path(run_at)
    assert result == str(path)
    rows = pq.read_table(path).to_pylist()
    assert rows[0]["file_id"] == "abc"
    assert rows[0]["company"] == "Snowflake Inc."


def test_compact_sec_parquet_dedupes_by_file_id_with_later_file_winning(sec, data_dir):
    older = [
        {
            "file_id": "1",
            "company": "Old Name",
            "cik": "0001",
            "form": "10-K",
            "file_date": "2026-01-01",
            "period_ending": "2025-12-31",
            "location": "Boise, ID",
            "accession": "old",
        }
    ]
    newer = [
        {
            "file_id": "1",
            "company": "New Name",
            "cik": "0001",
            "form": "10-K",
            "file_date": "2026-02-01",
            "period_ending": "2025-12-31",
            "location": "Boise, ID",
            "accession": "new",
        },
        {
            "file_id": "2",
            "company": "Second Co",
            "cik": "0002",
            "form": "10-K",
            "file_date": "2026-02-01",
            "period_ending": "2025-12-31",
            "location": "Austin, TX",
            "accession": "second",
        },
    ]
    write_sec_rows(sec, data_dir / "sec_ai_10k.parquet", older)
    write_sec_rows(sec, data_dir / "sec_ai_10k_2026-02-01_0000.parquet", newer)

    result = sec.compact_sec_parquet()
    rows = {row["file_id"]: row for row in pq.read_table(result).to_pylist()}
    assert rows["1"]["company"] == "New Name"
    assert rows["2"]["company"] == "Second Co"


def test_compact_sec_parquet_raises_when_no_files_exist(sec):
    with pytest.raises(FileNotFoundError, match="No SEC parquet files"):
        sec.compact_sec_parquet()
