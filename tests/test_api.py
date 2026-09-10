import api
from fastapi.testclient import TestClient

SEC_ROW = {
    "file_id": "abc",
    "company": "Acme Inc.  (ACME)  (CIK 0000000001)",
    "cik": "0000000001",
    "form": "10-K",
    "file_date": "2026-01-15",
    "period_ending": "2025-12-31",
    "location": "Boise, ID",
    "accession": "0001-adsh",
}


def test_root_lists_endpoints(monkeypatch, tmp_path):
    monkeypatch.setattr(api, "PARQUET_PATH", tmp_path / "missing.parquet")
    client = TestClient(api.app)
    response = client.get("/")
    assert response.status_code == 200
    body = response.json()
    assert body["endpoints"] == ["/data", "/parquet", "/health"]


def test_health_and_data_with_parquet(monkeypatch, tmp_path, sec):
    path = tmp_path / "sec_ai_10k.parquet"
    sec.write_rows(path, [SEC_ROW])
    monkeypatch.setattr(api, "PARQUET_PATH", path)
    client = TestClient(api.app)

    health = client.get("/health")
    assert health.status_code == 200
    assert health.json() == {"ok": True, "rows": 1, "path": str(path)}

    data = client.get("/data")
    assert data.status_code == 200
    assert data.json() == [SEC_ROW]


def test_health_when_parquet_is_missing(monkeypatch, tmp_path):
    path = tmp_path / "missing.parquet"
    monkeypatch.setattr(api, "PARQUET_PATH", path)
    client = TestClient(api.app)
    health = client.get("/health")
    assert health.status_code == 200
    assert health.json() == {"ok": False, "rows": 0, "path": str(path)}


def test_parquet_download(monkeypatch, tmp_path, sec):
    path = tmp_path / "sec_ai_10k.parquet"
    sec.write_rows(path, [SEC_ROW])
    monkeypatch.setattr(api, "PARQUET_PATH", path)
    client = TestClient(api.app)
    response = client.get("/parquet")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/vnd.apache.parquet")
    assert response.content == path.read_bytes()


def test_parquet_missing_returns_404(monkeypatch, tmp_path):
    monkeypatch.setattr(api, "PARQUET_PATH", tmp_path / "missing.parquet")
    client = TestClient(api.app)
    response = client.get("/parquet")
    assert response.status_code == 404
    assert response.json()["detail"] == "Parquet file not found yet"
