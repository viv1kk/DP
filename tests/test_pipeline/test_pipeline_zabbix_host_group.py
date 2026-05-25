import pytest
from pipelines.zabbix.host_group.pipeline import ZABBIX_HOST_GROUP
import pandas as pd

# def test_pipeline_extract():
#     obj = ZABBIX_HOST_GROUP()
#     data = obj.extract()
#     trans = obj.transform(data)
#     print(trans["new_df"].dtypes)


# def test_etl():
#     obj = ZABBIX_HOST_GROUP()
#     print(f"df = {ZABBIX_HOST_GROUP._cache.get('last_cached_df', {})}")
#     obj.etl()
#     print(f"df = {ZABBIX_HOST_GROUP._cache.get('last_cached_df', {}).head()}")
#     obj.etl()
#     print(f"df = {ZABBIX_HOST_GROUP._cache.get('last_cached_df', {}).head()}")

# def test_pipeline_mssql_reset():
#     obj = ZABBIX_HOST_GROUP()
#     data = obj.reset_mssql_table()
#     print(data)