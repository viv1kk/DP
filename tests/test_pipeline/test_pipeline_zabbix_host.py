import pytest
from pipelines.zabbix.hosts.pipeline import ZABBIX_HOSTS
import pandas as pd
import time
import copy
# def test_pipeline_config():
#     obj = ZABBIX_HOSTS()
#     print(obj.zabbix_config)

# def test_pipeline_etl():
#     obj = ZABBIX_HOSTS()
#     data = obj.extract()
#     trans = obj.transform(data)
#     obj.validate(trans)
#     df = copy.deepcopy(trans["upsert_df"])

#     trans["upsert_df"] = trans["upsert_df"].head()
#     obj.load(trans)
    
#     time.sleep(4)
#     trans["is_initial_run"] = False
#     trans["upsert_df"] = trans["upsert_df"].head()
#     trans["upsert_df"] = trans["upsert_df"][
#         trans["upsert_df"]["hostid"] != "10435"
#     ]
#     obj.load(trans)

#     time.sleep(4)
#     trans["is_initial_run"] = False
#     trans["upsert_df"] = trans["upsert_df"].head()
#     obj.load(trans)

#     time.sleep(4)
#     trans["is_initial_run"] = False
#     trans["upsert_df"] = df.head()
#     trans["upsert_df"].loc[
#     trans["upsert_df"]["hostid"] == "10435",
#         "host_name"
#     ] = "vivk"
#     obj.load(trans)
#     print(trans)

# def test_comp():
#     obj = ZABBIX_HOSTS()
#     data = obj.extract()
#     trans = obj.transform(data)
#     obj.validate(trans)
    
#     print(trans["rows_to_delete"].columns)
#     print(trans["upsert_df"].columns)
    


# def test_etl():
#     obj = ZABBIX_HOST_GROUP()
#     print(f"df = {ZABBIX_HOST_GROUP._cache.get('last_cached_df', {})}")
#     obj.etl()
#     print(f"df = {ZABBIX_HOST_GROUP._cache.get('last_cached_df', {}).head()}")
#     obj.etl()
#     print(f"df = {ZABBIX_HOST_GROUP._cache.get('last_cached_df', {}).head()}")

# def test_pipeline_mssql_reset():
#     obj = ZABBIX_HOSTS()
#     data = obj.reset_mssql_table()
#     print(data)