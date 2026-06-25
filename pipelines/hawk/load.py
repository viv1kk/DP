import pandas as pd
import logging
import copy

from config.mssql_config import MSSQL_Config
from config.sql_tbl_config import TableConfig
from connectors.mssql_connector import MSSQL_Connector
from exceptions.RecoverableError import RecoverableError
from framework.decorators import etl_run_status
from framework.mssql_table_reset import MSSQL_TABLE_MANAGER
pd.set_option('display.max_columns', None)

def _create_temp_table(mssql:MSSQL_Connector, temp_tbl:TableConfig):
    temp_tbl_name = temp_tbl.sql_table_name
    query = f"""IF OBJECT_ID('tempdb..{temp_tbl_name}') IS NOT NULL
            DROP TABLE {temp_tbl_name};"""
    
    mssql.build_query(query)
    mssql_tbl_mgr = MSSQL_TABLE_MANAGER(tbl_config=temp_tbl, mssql=mssql)
    # mssql_tbl_mgr.drop_partitioned_table_and_schema()
    mssql_tbl_mgr.create_table()


def _update_host_table(df:pd.DataFrame, table:TableConfig, mssql:MSSQL_Connector):
    temp_tbl = copy.deepcopy(table)
    temp_tbl_name = f"#temp_{table.sql_table_name}"
    temp_tbl.sql_table_name = temp_tbl_name
    temp_tbl.sql_columnar_index_name = ''
    temp_tbl.sql_pk_constraint_name = f"PK_{temp_tbl_name}"



    def _mark_all_unknown():
        query = f"""
            UPDATE {table.sql_table_name}
            SET host_status = 'UNKNOWN'
            WHERE host_tracked = 'Yes';
        """
        mssql.build_query(query)
    
    
    # --------------------------------------------------
    # Update Existing
    # --------------------------------------------------
    def _update_existing():
        query = f"""
            UPDATE T
            SET T.host_status = S.host_status,
                T.last_poll_at_IST = S.last_poll_at_IST
            FROM {table.sql_table_name} T
            INNER JOIN {temp_tbl_name} S
                ON T.dns = S.dns
                AND T.domain = S.domain
                AND T.host_name = S.host_name;
        """

        mssql.build_query(query)
    
    # --------------------------------------------------
    # Insert New
    # --------------------------------------------------
    def _insert_new():
        query = f"""
            INSERT INTO {table.sql_table_name}
            (dns, domain, host_name,
             host_status, host_tracked,
             last_poll_at_IST)
            SELECT
                S.dns, S.domain, S.host_name,
                S.host_status, S.host_tracked,
                S.last_poll_at_IST
            FROM {temp_tbl_name} S
            LEFT JOIN {table.sql_table_name} T
                ON T.dns = S.dns
                AND T.domain = S.domain
                AND T.host_name = S.host_name
            WHERE T.dns IS NULL;
        """
        mssql.build_query(query)
    
    # --------------------------------------------------
    # Mark Missing as Stopped
    # --------------------------------------------------
    def _mark_missing_stopped_inferred():
        query = f"""
            UPDATE T
            SET T.host_status = 'STOPPED_INFERRED'
            FROM {table.sql_table_name} T
            LEFT JOIN {temp_tbl_name} S
                ON T.dns = S.dns
                AND T.domain = S.domain
                AND T.host_name = S.host_name
            WHERE S.dns IS NULL
              AND T.host_tracked = 'Yes';
        """
        mssql.build_query(query)

    def _update_host_tracked_status():
        query = f"""
            UPDATE TARGET
            SET
                TARGET.host_tracked = 'Yes'
            FROM {table.sql_table_name} AS TARGET
            INNER JOIN {temp_tbl_name} AS SOURCE
                ON TARGET.dns = SOURCE.dns
            AND TARGET.domain = SOURCE.domain
            AND TARGET.host_name = SOURCE.host_name
            WHERE ISNULL(TARGET.host_tracked, 'No') = 'No'
            AND SOURCE.host_tracked = 'Yes';
        """
        mssql.build_query(query)

    if df.empty:
        _mark_all_unknown()
        return

    _create_temp_table(mssql=mssql, temp_tbl=temp_tbl)
    mssql.batch_insert(df=df, table=temp_tbl)
    _update_existing()
    _insert_new()
    _mark_missing_stopped_inferred()
    _update_host_tracked_status()


def _update_application_status_table(df:pd.DataFrame, table:TableConfig, mssql:MSSQL_Connector):
    temp_tbl = copy.deepcopy(table)
    temp_tbl_name = f"#temp_{table.sql_table_name}"
    temp_tbl.sql_table_name = temp_tbl_name
    temp_tbl.sql_columnar_index_name = ''
    temp_tbl.sql_pk_constraint_name = f"PK_{temp_tbl_name}"

    def _mark_all_stopped_inferred():
        query = f"""
            UPDATE {table.sql_table_name}
            SET status = 'STOPPED_INFERRED'
            WHERE application_tracked = 'Yes';
        """
        mssql.build_query(query)


    # TARGET.application_tracked = SOURCE.application_tracked
    def _update_existing():
        query = f"""
            UPDATE TARGET
            SET
                TARGET.type = SOURCE.type,
                TARGET.status = SOURCE.status,
                TARGET.last_poll_at_IST = SOURCE.last_poll_at_IST
            FROM {table.sql_table_name} AS TARGET
            INNER JOIN {temp_tbl_name} AS SOURCE
            ON
                TARGET.dns = SOURCE.dns
            AND TARGET.domain = SOURCE.domain
            AND TARGET.host_name = SOURCE.host_name
            AND TARGET.microagent_name = SOURCE.microagent_name
            AND TARGET.deployment = SOURCE.deployment
            AND TARGET.component = SOURCE.component;
        """
        mssql.build_query(query)

    def _insert_new():
        query = f"""
        INSERT INTO {table.sql_table_name}
        (
            dns,
            domain,
            host_name,
            type,
            deployment,
            component,
            microagent_name,
            status,
            last_poll_at_IST,
            application_tracked
        )
        SELECT
            SOURCE.dns,
            SOURCE.domain,
            SOURCE.host_name,
            SOURCE.type,
            SOURCE.deployment,
            SOURCE.component,
            SOURCE.microagent_name,
            SOURCE.status,
            SOURCE.last_poll_at_IST,
            SOURCE.application_tracked
        FROM {temp_tbl_name} SOURCE
        LEFT JOIN {table.sql_table_name} TARGET
        ON
            TARGET.dns = SOURCE.dns
        AND TARGET.domain = SOURCE.domain
        AND TARGET.host_name = SOURCE.host_name
        AND TARGET.microagent_name = SOURCE.microagent_name
        AND TARGET.deployment = SOURCE.deployment
        AND TARGET.component = SOURCE.component
        WHERE TARGET.dns IS NULL;
        """
        mssql.build_query(query)


    def _mark_missing_stopped_inferred():
        query = f"""
            UPDATE TARGET
            SET
                TARGET.status = 'STOPPED_INFERRED'
            FROM {table.sql_table_name} TARGET
            LEFT JOIN {temp_tbl_name} SOURCE
            ON
                TARGET.dns = SOURCE.dns
            AND TARGET.domain = SOURCE.domain
            AND TARGET.host_name = SOURCE.host_name
            AND TARGET.microagent_name = SOURCE.microagent_name
            AND TARGET.deployment = SOURCE.deployment
            AND TARGET.component = SOURCE.component
            WHERE SOURCE.dns IS NULL;
        """
        mssql.build_query(query)

    def _update_application_tracked_status():
        query = f"""
            UPDATE TARGET
            SET
                TARGET.application_tracked = 'Yes'
            FROM {table.sql_table_name} AS TARGET
            INNER JOIN {temp_tbl_name} AS SOURCE
                ON TARGET.dns = SOURCE.dns
            AND TARGET.domain = SOURCE.domain
            AND TARGET.host_name = SOURCE.host_name
            AND TARGET.deployment = SOURCE.deployment
            AND TARGET.component = SOURCE.component
            WHERE ISNULL(TARGET.application_tracked, 'No') = 'No'
            AND SOURCE.application_tracked = 'Yes';
        """
        mssql.build_query(query)

    if df.empty:
        _mark_all_stopped_inferred()
        return

    _create_temp_table(mssql=mssql, temp_tbl=temp_tbl)
    mssql.batch_insert(df=df, table=temp_tbl)
    _update_existing()
    _insert_new()
    _mark_missing_stopped_inferred()
    _update_application_tracked_status()

def load_data(data:dict[str:pd.DataFrame], db:MSSQL_Config, table:dict[str:TableConfig], logger:logging.Logger):
    try:
        mssql = MSSQL_Connector(db)
        def transaction(cursor):
            _update_host_table(df=data["host_status_df"], table=table["host_status_table"], mssql=mssql)
            _update_application_status_table(df=data["microagent_status_df"], table=table["application_status_table"], mssql=mssql)
        
        mssql.run_queries(transaction)
    except Exception as e:
        # logger.warning(f"Recoverable error: {e}")
        raise RecoverableError(f"Recoverable Error: load_data -> {e}")