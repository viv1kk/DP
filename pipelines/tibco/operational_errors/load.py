import pandas as pd
from config.mssql_config import MSSQL_Config
from config.sql_tbl_config import TableConfig
from connectors.mssql_connector import MSSQL_Connector
from exceptions.RecoverableError import RecoverableError
from framework.decorators import etl_run_status


def delete_old_rows(mssql:MSSQL_Connector, table:TableConfig):
    query = f"""DELETE FROM {table.sql_table_name}
                WHERE logtype IN ('AUDIT_ERROR', 'ADB_N', 'ADB_OPQ', 'ADB_F')"""
    mssql.build_query(query=query)


# @etl_run_status()
def load_data(df:pd.DataFrame, db:MSSQL_Config, table:TableConfig, logger):
    try:
        mssql = MSSQL_Connector(db)

        def transaction(cursor):
            delete_old_rows(mssql, table)
            if(df.empty): return True
            mssql.batch_insert(df=df, table=table)

        mssql.run_queries(transaction)
    
    except Exception as e:
        raise RecoverableError(f"Recoverable Error: load_data -> {e}")