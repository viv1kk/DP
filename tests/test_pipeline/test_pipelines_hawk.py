import pytest
from pipelines.hawk.pipeline import HAWK_APPLICATION_MONITORING


# def test_pipeline_mssql_reset():
#     obj = TIBCO_AUDIT_OPERATIONAL_ERRORS()
#     obj.reset_mssql_table()


# def test_pipeline_run():
#     obj = TIBCO_AUDIT_OPERATIONAL_ERRORS()
#     obj.run()


def test_pipeline_extract():
    obj = HAWK_APPLICATION_MONITORING()
    data = obj.extract()

    print(data.keys())


