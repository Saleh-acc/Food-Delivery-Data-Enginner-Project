from pathlib import Path
import importlib.util
import pytest 
from airflow.models import DagBag

@pytest.fixture()
def dagbag():
    return DagBag(dag_folder="dags/", include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}, \
        f"DAG import errors: {dagbag.import_errors}"
        
def test_dag_has_owner(dagbag):
    for dag in dagbag.dags.values():
        assert dag.owner not in (None, "", "airflow"), \
        f"DAG {dag.dag_id} has placeholder or missing owner"
       