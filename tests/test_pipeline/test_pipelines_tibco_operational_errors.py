import pytest
from pipelines.tibco.operational_errors.pipeline import TIBCO_AUDIT_OPERATIONAL_ERRORS


# def test_pipeline_mssql_reset():
#     obj = TIBCO_AUDIT_OPERATIONAL_ERRORS()
#     obj.reset_mssql_table()


# def test_pipeline_run():
#     obj = TIBCO_AUDIT_OPERATIONAL_ERRORS()
#     obj.run()


def test_pipeline_extract():
    obj = TIBCO_AUDIT_OPERATIONAL_ERRORS()
    print(obj.es_config)
    obj.extract()


