import pandas as pd
import hashlib
import json
from dateutil import parser
from config.mssql_config import MSSQL_Config
from config.sql_tbl_config import TableConfig
from connectors.mssql_connector import MSSQL_Connector
from exceptions.RecoverableError import RecoverableError
from datetime import datetime
from zoneinfo import ZoneInfo
import pytz
import numpy as np
from framework.logger import get_logger, format_error
from framework.util import clean_url

logger = get_logger("SAP_SOLMAN")


def add_sid(df:pd.DataFrame)->pd.DataFrame:
    df["SID"] = df["RootComponentName"].str[:3].fillna('')
    return df

def fetch_data_from_table(db: MSSQL_Config, table: TableConfig, columns:list):
    try:
        mssql = MSSQL_Connector(db)
        cols = ", ".join(f"[{col}]" for col in columns)
        df = mssql.select_query(query=f"SELECT {cols} FROM [{table.sql_table_name}]")
        return df
    except Exception as e:
        raise RecoverableError(f"Error fetching data from table:{e} ")

def fetch_sid_poll_aggregate(db: MSSQL_Config, table: TableConfig, columns:list):
    """
    Fetch SID and MIN(Poll_Result) per SID.

    Rows with no URL mapping carry no availability signal, so their Poll Result
    is NULL and ignored by MIN. A SID with no polled URL at all gets NULL.
    """
    
    try:
        df = fetch_data_from_table(db, table, columns)
        # Ensure BIT column becomes integer (0/1/None)
        df["Poll Result"] = df["Poll Result"].astype("float").astype("Int64")
        # Guard against stale rows where a missing URL was polled and stored as False.
        df["Poll Result"] = df["Poll Result"].where(df["link to Fiori Launchpad"].map(clean_url).notna())
    
        # Aggregate → MIN(Poll_Result) per SID (one row per SID, so the merge below cannot duplicate events)
        df_agg = (
            df.groupby("SID", as_index=False)
              .agg(URL_Poll_Result=("Poll Result", "min"))
        )
    
        return df_agg
    except Exception as e:
        raise RecoverableError(f"Error fetching aggregated SID/Poll_Result:{e} ")
    


def fetch_solution(db: MSSQL_Config, table: TableConfig, columns:list):
    """
    Fetch only SID and MIN(Poll_Result) aggregated in SQL Server.
    """
    try:
        df = fetch_data_from_table(db, table, columns)
        return df

    except Exception as e:
        logger.error(f"fetch_solution failed, continuing with no solution data: {format_error(e)}")
        return pd.DataFrame()
    

def enrich_df(df:pd.DataFrame, data:dict, db:MSSQL_Config, url_poll_tbl_config:TableConfig)->pd.DataFrame:
    url_poll_data = fetch_sid_poll_aggregate(
        db=db, 
        table=url_poll_tbl_config, 
        columns=["SID", "Poll Result", "link to Fiori Launchpad"]
    )
    df = df.merge(
        url_poll_data[["SID", "URL_Poll_Result"]],
        on="SID",
        how="left"
    )
    
    
    solution_data = fetch_solution( db=db, 
        table=url_poll_tbl_config, 
        columns=["SID", "Solution"]
    )
    df = df.merge(
        solution_data[["SID", "Solution"]],
        on="SID",
        how="left"
    )

    return df


def add_timestamp_dimensions(df:pd.DataFrame)->pd.DataFrame:
        try:
            FALLBACK_TIME = datetime(1900, 1, 1, 0, 0, 0)

            def parse_sap_timestamp(value):
                try:
                    value = str(value)
                    if not value or 'Date' not in value:
                        return None
                    timestamp_int = int(value.strip('/Date()'))// 1000
                    # dt = datetime.fromtimestamp(timestamp_int // 1000, tz=timezone.utc)
                    # return dt if dt.year >= 1753 else None
                    return timestamp_int
                except Exception as e:
                    return 0


            def safe_parse_sap_timestamp_CET(ts):
                try:
                    if pd.isna(ts) or str(ts).strip() == '':
                        return FALLBACK_TIME
                    # print(ts)
                    parsed = parse_sap_timestamp(ts)
                        # return parsed if isinstance(parsed, datetime) else FALLBACK_TIME
                    # print(parsed)
                    time = datetime.fromtimestamp(parsed, ZoneInfo("Europe/Berlin"))
        
                    return time.replace(tzinfo=None)
                except Exception:
                    return FALLBACK_TIME

            def safe_parse_sap_timestamp_UTC(ts):
                try:
                    if pd.isna(ts) or str(ts).strip() == '':
                        return FALLBACK_TIME
                    # print(ts)
                    parsed = parse_sap_timestamp(ts)
                        # return parsed if isinstance(parsed, datetime) else FALLBACK_TIME
                    # print(parsed)
                    time = datetime.fromtimestamp(parsed, ZoneInfo("UTC"))
        
                    return time.replace(tzinfo=None)
                except Exception:
                    return FALLBACK_TIME
                    
            def safe_parse_sap_timestamp_IST(ts):
                try:
                    if pd.isna(ts) or str(ts).strip() == '':
                        return FALLBACK_TIME
                    # print(ts)
                    parsed = parse_sap_timestamp(ts)
                        # return parsed if isinstance(parsed, datetime) else FALLBACK_TIME
                    # print(parsed)
                    time = datetime.fromtimestamp(parsed, ZoneInfo("Asia/Kolkata"))
        
                    return time.replace(tzinfo=None)
                except Exception:
                    return FALLBACK_TIME

            if 'ValueTimestamp' in df.columns:
                df['ParsedValueTimestamp_UTC'] = pd.to_datetime(df['ValueTimestamp'].apply(safe_parse_sap_timestamp_UTC), utc=True)
                df['ParsedValueTimestamp_CET'] = pd.to_datetime(df['ValueTimestamp'].apply(safe_parse_sap_timestamp_CET), utc=True)
                df['ParsedValueTimestamp_IST'] = pd.to_datetime(df['ValueTimestamp'].apply(safe_parse_sap_timestamp_IST), utc=True)

            return df
        except Exception as e:
            raise RecoverableError(f"Error while parsing timestamp values: {e}")

def parsing_datatype(df:pd.DataFrame)->pd.DataFrame:
    try:
        for col in ['ValueLast', 'ValueMax', 'ValueMin', 'NoOfAlerts', 'ValueCount', 'ValueSum']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        if 'Period' in df.columns:
            df['Period'] = pd.to_numeric(df['Period'], errors='coerce').astype('Int64')

        return df
    except Exception as e:
        raise RecoverableError(f"Error while formatting datatypes: {e}")


def add_max_status_column(df: pd.DataFrame) -> pd.DataFrame:
    try:
        # Validate required columns
        required_cols = {'RootComponentid', 'Availstatus', 'Perfstatus', 'Excpstatus', 'Confstatus'}
        if not required_cols.issubset(df.columns):
            raise ValueError(f"DataFrame must contain columns: {required_cols}")

        # Group by RootComponentid and calculate the max of each status column
        agg_df = df.groupby('RootComponentid', as_index=False)[
            ['Availstatus', 'Perfstatus', 'Excpstatus', 'Confstatus']
        ].max()

        # Compute the max status value per group
        agg_df['MaxStatus'] = agg_df[
            ['Availstatus', 'Perfstatus', 'Excpstatus', 'Confstatus']
        ].max(axis=1)

        # Merge MaxStatus back to the original dataframe
        df = df.merge(agg_df[['RootComponentid', 'MaxStatus']], on='RootComponentid', how='left')

        return df

    except Exception as e:
        raise RecoverableError(f"Error in add_max_status_column: {e}")


def pr1(df):
    df = df[(df['Rating'] > 0) & (df['Category'] == 'AVAIL')]
    p1_systems = set()
    
    # df_grp = (
    #     df.groupby(['RootComponentName', 'MoType', 'MoName'], as_index=False)['Rating']
    #       .max()
    # )
    # df_grp = (
    #     df_grp.groupby(['RootComponentName', 'MoType'])['Rating']
    #     .apply(lambda x: (x == 3).all())       # true only if ALL MoNames are rating 3
    #     .reset_index()
    # )
    # p1_systems.update(df_grp[df_grp['Rating']]['RootComponentName'].unique())



    # Only an explicit failed poll (0) counts; NULL means no URL mapping and must not raise priority.
    url_down = df['URL_Poll_Result'].eq(0).fillna(False).astype(bool)
    p1_systems.update(df[url_down]['RootComponentName'].unique())
    
    return np.array(list(p1_systems))

def pr2(df):
    # filter to AVAIL + PERFORM categories with Rating > 1
    df_f = df[(df['Rating'] > 0) & (df['Category'].isin(['AVAIL', 'PERFORM']))].copy()
    p2_systems = set()
    # group to get the max rating per component
    df_grp1 = (
        df_f[df_f['Category'] == 'AVAIL']
            .groupby(['RootComponentName', 'MoType', 'MoName'], as_index=False)['Rating']
            .max()
    )
    
    # find systems where ANY MoName has Rating = 3  → P2
    p2_systems.update(df_grp1[df_grp1['Rating'] == 3]['RootComponentName'].unique())

    df_grp2 = (
        df_f[df_f['Category'] == 'PERFORM']
            .groupby(['RootComponentName', 'MoType', 'MoName'], as_index=False)['Rating']
          .max()
    )
    df_grp2 = (
        df_grp2.groupby(['RootComponentName', 'MoType'])['Rating']
        .apply(lambda x: (x == 3).all())       # true only if ALL MoNames are rating 3
        .reset_index()
    )
    p2_systems.update(df_grp2[df_grp2['Rating']]['RootComponentName'].unique())
    
    return np.array(list(p2_systems))

def pr3(df):
    df_f = df[(df['Rating'] > 0)].copy()
    p3_systems = set()

    # Any component availability = Amber (rating 2).
    df_grp1 = (
        df_f[df_f['Category'] == 'AVAIL']
            .groupby(['RootComponentName', 'MoType', 'MoName'], as_index=False)['Rating']
            .max()
    )

    p3_systems.update(df_grp1[df_grp1['Rating'] > 1]['RootComponentName'].unique())

    # Any performance = Amber (rating 3)

    df_grp2 = (
        df_f[df_f['Category'] == 'PERFORM']
            .groupby(['RootComponentName', 'MoType', 'MoName'], as_index=False)['Rating']
            .max()
    )
    p3_systems.update(df_grp2[df_grp2['Rating'] == 3]['RootComponentName'].unique())

    # Exception or Configuration category contains Red alerts.

    df_grp3 = (
        df_f[df_f['Category'] == 'EXCEPTION']
            .groupby(['RootComponentName', 'MoType', 'MoName'], as_index=False)['Rating']
          .max()
    )
    df_grp3 = (
        df_grp3.groupby(['RootComponentName', 'MoType'])['Rating']
        .apply(lambda x: (x == 3).all())       # true only if ALL MoNames are rating 3
        .reset_index()
    )
    p3_systems.update(df_grp3[df_grp3['Rating']]['RootComponentName'].unique())

    
    df_grp4 = (
        df_f[df_f['Category'] == 'CONFIGURE']
            .groupby(['RootComponentName', 'MoType', 'MoName'], as_index=False)['Rating']
          .max()
    )
    df_grp4 = (
        df_grp4.groupby(['RootComponentName', 'MoType'])['Rating']
        .apply(lambda x: (x == 3).all())       # true only if ALL MoNames are rating 3
        .reset_index()
    )
    p3_systems.update(df_grp4[df_grp4['Rating']]['RootComponentName'].unique())
    
    
    return np.array(list(p3_systems))
def pr4(df):
    df_f = df[(df['Rating'] > 0)].copy()
    p4_systems = set()
    df_grp2 = (
        df_f[df_f['Category'] == 'PERFORM']
        .groupby(['RootComponentName', 'MoType', 'MoName'], as_index=False)['Rating']
        .max()
    )
    
    p4_systems.update(df_grp2[df_grp2['Rating'] > 1]['RootComponentName'].unique())

    # Exception or Configuration category contains Red alerts.

    df_grp3 = (
        df_f[df_f['Category'] == 'EXCEPTION']
            .groupby(['RootComponentName', 'MoType', 'MoName'], as_index=False)['Rating']
          .max()
    )
    p4_systems.update(df_grp3[df_grp3['Rating'] > 1]['RootComponentName'].unique())

    
    df_grp4 = (
        df_f[df_f['Category'] == 'CONFIGURE']
            .groupby(['RootComponentName', 'MoType', 'MoName'], as_index=False)['Rating']
          .max()
    )
    p4_systems.update(df_grp4[df_grp4['Rating'] > 1]['RootComponentName'].unique())
    
    
    return np.array(list(p4_systems))

def assign_priority(df:pd.DataFrame)->pd.DataFrame:
    df["Priority"] = None
    p1 = pr1(df)
    df.loc[df['RootComponentName'].isin(p1), 'Priority'] = 'P1'

    p2 = pr2(df[~df['RootComponentName'].isin(p1)])
    df.loc[df['RootComponentName'].isin(p2), 'Priority'] = 'P2'

    p3 = pr3(df[~df['RootComponentName'].isin(np.concatenate([p1, p2]))])
    df.loc[df['RootComponentName'].isin(p3), 'Priority'] = 'P3'

    p4 = pr4(df[~df['RootComponentName'].isin(np.concatenate([p1, p2, p3]))])
    df.loc[df['RootComponentName'].isin(p4), 'Priority'] = 'P4'

    return df


def final_datatype_processing(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # 1. Convert NaN → None
    df_clean = df.copy()
    
    # Define column type groups
    int_columns = ['Rating', 'ValueCount', 'Availstatus', 'Perfstatus', 
                   'Excpstatus', 'Confstatus', 'Selfmonstatus', 'NoOfAlerts', 'MaxStatus']
    bigint_columns = ['Period']
    float_columns = ['ValueMax', 'ValueMin', 'ValueSum', 'ValueLast', 
                     'Greentoyellow', 'Yellowtogreen', 'Yellowtored', 
                     'Redtoyellow', 'Threshold', 'Greentored']
    bit_columns = ['IsPropagated']
    datetime_columns = ['ParsedValueTimestamp_UTC', 'ParsedValueTimestamp_UTC_timeslot_5min']
    
    # Clean numeric columns (INT)
    for col in int_columns:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].replace('', None)
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
            df_clean[col] = df_clean[col].astype('Int64')  # Nullable integer
    
    # Clean BIGINT columns
    for col in bigint_columns:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].replace('', None)
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
            df_clean[col] = df_clean[col].astype('Int64')
    
    # Clean FLOAT columns
    for col in float_columns:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].replace('', None)
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
    
    # Clean BIT columns
    for col in bit_columns:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].replace('', None)
            df_clean[col] = df_clean[col].map({
                1: 1, 0: 0, '1': 1, '0': 0, 
                True: 1, False: 0, 'True': 1, 'False': 0
            })
            df_clean[col] = df_clean[col].fillna(0).astype(int)
    
    # Clean DATETIME columns
    for col in datetime_columns:
        if col in df_clean.columns:
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
    
    # Replace NaN and NaT with None for SQL NULL
    df_clean = df_clean.replace({pd.NaT: None, np.nan: None})
    df_clean = df_clean.astype(object)
    df_clean = df_clean.where(pd.notnull(df_clean), None)
    return df_clean

def transform(data:dict, db:MSSQL_Config, url_poll_tbl_config:TableConfig)->pd.DataFrame:
    try:
        df = pd.DataFrame(data=data)
        
        df = add_sid(df)
        df = enrich_df(df, data, db, url_poll_tbl_config)

        if df.empty:
            raise RecoverableError(f"Empty Dataframe!")
        
        df = df.drop_duplicates()
        df = add_timestamp_dimensions(df=df)

        df = parsing_datatype(df=df)

        df["latest_poll_timestamp"] = datetime.now(pytz.timezone("Asia/Kolkata"))
        df.drop_duplicates(subset=['RootComponentid', 'MoContextid', 'EventName'], keep='last', inplace=True)
        cols = ["MoContextid", "TypeId", "EventName", "RootComponentid"]

        df = df[
            ~df[cols]
            .fillna("")                      # handle NaN
            .apply(lambda col: col.astype(str).str.strip() == "")
            .any(axis=1)
        ]

        df = add_max_status_column(df)
        df = assign_priority(df)
        df = final_datatype_processing(df)
        return df
    except Exception as e:
        raise RecoverableError(f"Recoverable Error : {e}")
    
