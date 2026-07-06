import pandas as pd
from config.mssql_config import MSSQL_Config
from config.sql_tbl_config import TableConfig
from connectors.mssql_connector import MSSQL_Connector
from exceptions.RecoverableError import RecoverableError
from framework.decorators import etl_run_status
pd.set_option('display.max_columns', None)


def load_data(df:pd.DataFrame, db:MSSQL_Config, table:TableConfig, logger):
    try:
        mssql = MSSQL_Connector(db)
        mssql.truncate_and_insert_df(df=df, table=table)
    
    except Exception as e:
        logger.warning(f"Recoverable error: {e}")
        raise RecoverableError(f"Recoverable Error: load_data -> {e}")