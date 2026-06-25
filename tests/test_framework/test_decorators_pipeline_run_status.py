import pytest
from pipelines.hawk.pipeline import HAWK_APPLICATION_MONITORING



def test_pipeline_reset_table():
    obj = HAWK_APPLICATION_MONITORING()
    obj.reset_mssql_table()

def test_pipeline_run():
    obj = HAWK_APPLICATION_MONITORING()

    obj.run()