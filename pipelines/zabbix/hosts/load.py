import pandas as pd
from config.mssql_config import MSSQL_Config
from config.sql_tbl_config import TableConfig
from connectors.mssql_connector import MSSQL_Connector
from exceptions.RecoverableError import RecoverableError
from framework.decorators import etl_run_status
import logging


def batch_delete(mssql: MSSQL_Connector, rows: pd.DataFrame, table:TableConfig, batch_size=1000):
    total = len(rows)
    delete_query = f"""
        DELETE FROM {table.sql_table_name}
        WHERE [hostid]=? AND [groupid]=?
    """
    for i in range(0, total, batch_size):
        batch = rows.iloc[i:i + batch_size]  # ✅ use iloc
        if len(batch) == 0:
            continue
        params = [
            (
                None if pd.isna(row["hostid"]) else row["hostid"],
                None if pd.isna(row["groupid"]) else row["groupid"]
            )
            for _, row in batch.iterrows()
        ]
        mssql.build_many_query(query=delete_query, data=params)  # ✅ executemany, not execute


def upsert_merge(mssql:MSSQL_Connector, df:pd.DataFrame, table:TableConfig):
    cols = list(df.columns)
    non_keys = [c for c in cols if c not in ["hostid", "groupid"]]

    update_set = ', '.join([f"[{c}] = ?" for c in non_keys])
    col_names = ', '.join([f"[{c}]" for c in cols])
    placeholders = ', '.join(['?'] * len(cols))

    merge_query = f"""
    MERGE {table.sql_table_name} AS target
    USING (SELECT ? AS hostid, ? AS groupid) AS source
    ON target.hostid = source.hostid AND target.groupid = source.groupid
    WHEN MATCHED THEN
        UPDATE SET {update_set}
    WHEN NOT MATCHED THEN
        INSERT ({col_names}) VALUES ({placeholders});
    """

    values = [
        (
            row["hostid"],
            row["groupid"],
            *[row[c] for c in non_keys],
            *[row[c] for c in cols]
        )
        for _, row in df.iterrows()
    ]
    mssql.build_many_query(query=merge_query, data=values)


def load_data(data:dict, db:MSSQL_Config, table:TableConfig, logger:logging.Logger):
    try:
        is_initial_run = data["is_initial_run"]
        rows_to_delete = data["rows_to_delete"]
        upsert_df = data["upsert_df"]

        mssql = MSSQL_Connector(db)

        def transaction(cursor):
            if is_initial_run:
                mssql.truncate_table(table=table)
                # mssql.batch_insert(df=upsert_df, table=table)
            elif not rows_to_delete.empty:
                batch_delete(mssql=mssql, rows=rows_to_delete[["hostid","groupid"]], table=table)
                # pass
            upsert_merge(mssql=mssql, df=upsert_df, table=table)

                
        mssql.run_queries(transaction)
        # mssql.truncate_and_insert_df(df=df, table=table)
    
    except Exception as e:
        raise RecoverableError(f"Recoverable Error: load_data -> {e}")