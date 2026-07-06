import pytest
from pipelines.tibco.b2b.pipeline import TIBCO_B2B_ERRORS
import pandas as pd

# def test_pipeline_mssql_reset():
#     obj = TIBCO_B2B_ERRORS()
#     obj.reset_mssql_table()


# def test_pipeline_run():
#     obj = TIBCO_B2B_ERRORS()
#     obj.run()


# def test_pipeline_extract():
#     obj = TIBCO_B2B_ERRORS()
#     print(obj.es_config)
#     print(obj.extract())

# def test_pipeline_transform():
#     obj = TIBCO_B2B_ERRORS()
#     e_data = obj.extract()
#     t_data = obj.transform(e_data)

#     t_data.to_csv("transform.csv")
#     print(t_data.columns)
#     summary = []

#     df = t_data
#     for col in df.columns:
#         max_len = (
#             df[col]
#             .fillna("")
#             .astype(str)
#             .str.len()
#             .max()
#         )

#         summary.append({
#             "Column": col,
#             "Data Type": str(df[col].dtype),
#             "Max String Length": int(max_len)
#         })

#     summary_df = pd.DataFrame(summary)

#     print(summary_df)