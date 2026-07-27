import pytest
from pipelines.zabbix.service_monitoring.pipeline import ZABBIX_SERVICE_MONITORING
import pandas as pd

def test_pipeline_extract():
    obj = ZABBIX_SERVICE_MONITORING()
    obj.run()
    obj.run()
    obj.run()
    obj.run()
#     data = obj.extract()
#     df = obj.transform(data=data)
#     obj.validate(df=df)
#     df = df.head()
#     obj.load(df=df)
#     print(df.head())


# def test_etl():
#     obj = ZABBIX_HOST_GROUP()
#     print(f"df = {ZABBIX_HOST_GROUP._cache.get('upsert_df', {})}")
#     obj.etl()
#     print(f"df = {ZABBIX_HOST_GROUP._cache.get('upsert_df', {}).head()}")
#     obj.etl()
#     print(f"df = {ZABBIX_HOST_GROUP._cache.get('upsert_df', {}).head()}")

# def test_pipeline_mssql_reset():
#     obj = ZABBIX_SERVICE_MONITORING()
#     data = obj.reset_mssql_table()
#     print(data)