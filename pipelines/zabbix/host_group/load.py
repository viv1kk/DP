import pandas as pd
from config.mssql_config import MSSQL_Config
from config.sql_tbl_config import TableConfig
from connectors.mssql_connector import MSSQL_Connector
from exceptions.RecoverableError import RecoverableError
from framework.decorators import etl_run_status
import logging


def batch_delete_in(mssql:MSSQL_Connector, groupids:list, batch_size=1000):
    total = len(groupids)

    for i in range(0, total, batch_size):
        batch = groupids[i:i + batch_size]

        if not batch:
            continue

        placeholders = ", ".join(["?"] * len(batch))
        delete_query = f"""
            DELETE FROM ZBX_HOST_GROUP
            WHERE groupid IN ({placeholders})
        """

        params = [None if pd.isna(v) else v for v in batch]

        mssql.build_query_with_data(query=delete_query, data=params)

def load_data(data:dict, db:MSSQL_Config, table:TableConfig, logger:logging.Logger):
    try:
        is_initial_run = data["is_initial_run"]
        changed_groupids = data["changed_groupids"]
        upsert_df = data["upsert_df"]

        mssql = MSSQL_Connector(db)

        def transaction(cursor):
            if is_initial_run:
                mssql.truncate_table(table=table)
            elif changed_groupids:
                batch_delete_in(mssql=mssql, groupids=changed_groupids)

            mssql.batch_insert(df=upsert_df, table=table)
                
        mssql.run_queries(transaction)
        # mssql.truncate_and_insert_df(df=df, table=table)
    
    except Exception as e:
        logger.warning(f"Recoverable error: {e}")
        raise RecoverableError(f"Recoverable Error: load_data -> {e}")