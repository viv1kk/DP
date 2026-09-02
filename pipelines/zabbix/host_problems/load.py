import pandas as pd
from config.mssql_config import MSSQL_Config
from config.sql_tbl_config import TableConfig
from connectors.mssql_connector import MSSQL_Connector
from exceptions.RecoverableError import RecoverableError
from framework.decorators import etl_run_status
import logging


def load_data(data:dict, db:MSSQL_Config, table:TableConfig, logger:logging.Logger):
    try:
        df = data["new_df"]
        mssql = MSSQL_Connector(db)
        # "Nothing is alerting" is a real state for this pipeline, not a failed
        # poll -- the transform builds a correctly shaped empty frame for it on
        # purpose -- so emptying the table is the right outcome here.
        mssql.truncate_and_insert_df(df=df, table=table, allow_empty=True)
    
    except Exception as e:
        raise RecoverableError(f"Recoverable Error: load_data -> {e}")