import pytest


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    return tmp_path


@pytest.fixture
def hf(data_dir, monkeypatch):
    import hugging_face

    monkeypatch.setattr(hugging_face, "DATA_DIR", data_dir)
    monkeypatch.setattr(hugging_face, "PARQUET_IN", data_dir / "sec_ai_10k.parquet")
    monkeypatch.setattr(hugging_face, "PARQUET_OUT", data_dir / "hf_models.parquet")
    monkeypatch.setattr(hugging_face.time, "sleep", lambda *_a, **_k: None)
    return hugging_face


@pytest.fixture
def sec(data_dir, monkeypatch):
    import sec_10k

    monkeypatch.setattr(sec_10k, "DATA_DIR", data_dir)
    monkeypatch.setattr(sec_10k, "CANONICAL_PARQUET", data_dir / "sec_ai_10k.parquet")
    monkeypatch.setattr(sec_10k.time, "sleep", lambda *_a, **_k: None)
    return sec_10k
