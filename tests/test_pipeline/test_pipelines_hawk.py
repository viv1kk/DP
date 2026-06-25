import pytest
from pipelines.hawk.pipeline import HAWK_APPLICATION_MONITORING



def test_pipeline_reset_table():
    obj = HAWK_APPLICATION_MONITORING()
    obj.reset_mssql_table()

# def test_pipeline_run():
#     obj = HAWK_APPLICATION_MONITORING()

#     obj.run()

# import pickle
# def test_pipeline_pickle():
#     obj = HAWK_APPLICATION_MONITORING()
#     print(HAWK_APPLICATION_MONITORING._cache["last_microagent_data"].shape)
#     e = obj.extract()
#     t = obj.transform(e)

#     with open("t.pkl", "wb") as f:
#         pickle.dump(t, f)
# import pandas as pd

# def test_t_l():
#     with open("t.pkl", "rb") as f:
#         t = pickle.load(f)
#     obj = HAWK_APPLICATION_MONITORING()
#     obj.validate(t)
#     # t["host_status_df"] = pd.DataFrame()
#     obj.load(t)
    # t["microagent_status_df"].to_csv("microagent_status_df.csv")
    # t["raw_df"].to_csv("raw_df.csv")
    # t["host_status_df"].to_csv("host_status_df.csv")



# def test_pipeline_extract():
#     obj = HAWK_APPLICATION_MONITORING()
#     print(HAWK_APPLICATION_MONITORING._cache["last_microagent_data"].shape)
#     e = obj.extract()
#     t = obj.transform(e)
#     print(t["microagent_status_df"].columns)
#     print(t["host_status_df"])
    # obj.run()
    # HAWK_APPLICATION_MONITORING._cache["last_microagent_data"].shape
    # obj.run()
    # HAWK_APPLICATION_MONITORING._cache["last_microagent_data"].shape
    # t.to_csv("check.csv")
    
    # print(t.head(100))
    


