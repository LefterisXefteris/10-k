from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def test_dag_imports_and_task_order(tmp_path, monkeypatch):
    monkeypatch.setenv("EXTRACT_DIR", str(ROOT / "extract"))
    monkeypatch.setenv("AIRFLOW_HOME", str(tmp_path / "airflow_home"))
    monkeypatch.setenv("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
    (tmp_path / "airflow_home").mkdir()

    from airflow.models import DagBag

    dagbag = DagBag(dag_folder=str(ROOT / "airflow" / "dags"), include_examples=False)
    assert dagbag.import_errors == {}
    dag = dagbag.dags["sec_ai_pipeline"]

    assert dag.task_dict["extract_sec"].downstream_task_ids == {"compact_sec"}
    assert dag.task_dict["compact_sec"].downstream_task_ids == {"extract_hf"}
    assert dag.task_dict["extract_hf"].downstream_task_ids == {"dbt_run"}
    assert dag.task_dict["dbt_run"].downstream_task_ids == {"dbt_test"}
    assert dag.task_dict["dbt_test"].downstream_task_ids == set()
