import pytest
from pipelines.zabbix.host_problems.pipeline import ZABBIX_HOST_PROBLEMS
import pandas as pd
import time
import copy

# def test_pipeline_extract():
#     obj = ZABBIX_HOST_PROBLEMS()
#     data = obj.extract()
#     df = obj.transform(data)
#     obj.load(df)
#     print(df.keys())


# def test_pipeline_mssql_reset():
#     obj = ZABBIX_HOST_PROBLEMS()
#     data = obj.reset_mssql_table()
