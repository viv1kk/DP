import requests
import pandas as pd
from config.es_config import ESConfig
from connectors.mssql_connector import MSSQL_Connector
from framework.decorators import retry
from config.mssql_config import MSSQL_Config
from connectors.elastic_connector import ELASTIC_CONNECTOR
from exceptions.RecoverableError import RecoverableError
import copy
import logging
from config.sql_tbl_config import TableConfig



def extract_data(base_table:TableConfig, db:MSSQL_Config, logger:logging.Logger):
    try:
        mssql = MSSQL_Connector(db)
        df = mssql.select_query(f"SELECT * FROM [{base_table.sql_table_name}]")        
        
        if "Poll Result" not in df.columns:
            df["Poll Result"] = True
            
        return df
    
    except Exception as e:
        raise RecoverableError(f"{e}")
