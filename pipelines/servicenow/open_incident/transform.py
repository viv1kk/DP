import re
import numpy as np
import pandas as pd
from framework.decorators import retry
from exceptions.RecoverableError import RecoverableError
import pytz
import logging
from datetime import datetime


SCHEMA = {
    'short_description': 'str',
    'close_code': 'str',
    'made_sla': 'str',
    'assignment_group': 'str',
    'description': 'str',
    'sys_updated_on': 'datetime64[ns]',
    'close_notes': 'str',
    'cmdb_ci' : 'str',
    'sys_id': 'str',
    'number': 'str',
    'sys_updated_by': 'str',
    'incident_state': 'str',
    'urgency': 'str',  # Keep as string if it contains text like '2 - Medium'
    'sys_created_on': 'datetime64[ns]',
    'state': 'str',
    'reassignment_count': 'int',
    'assigned_to': 'str',
    'closed_at': 'datetime64[ns]',
    'impact': 'str',
    'business_service': 'str',
    'reopen_count': 'int',
    'priority': 'str',
    'opened_at': 'datetime64[ns]',
    'caller_id': 'str',
    'resolved_at': 'datetime64[ns]',
    'category': 'str',
    'subcategory': 'str'
}


def _flatten_data(data:list)->list:
    flat_data = []
    for row in data:
        flat_row = {}
        for k, v in row.items():
            if isinstance(v, dict):
                flat_row[k] = v.get("display_value", None)
            else:
                flat_row[k] = v
        flat_data.append(flat_row)
    
    return flat_data

# def _enforce_schema(df: pd.DataFrame)->pd.DataFrame:
#     for col, dtype in SCHEMA.items():
#         # THIS METHOD IS CONVERTING A CET TIMESTAMP WITH NO TZ INFO TO UTC AS THIS NEEDS TO BE USED FOR API AS WELL FOR FILTER QUERY
#         def convert_to_ist(date_str):
#             try:
#                 # Parse the naive datetime string in format "%Y-%m-%d %H:%M:%S"
#                 dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                
#                 # Set the source timezone to CET
#                 cet_tz = pytz.timezone('Europe/Amsterdam')
#                 ist_tz = pytz.timezone('Asia/Kolkata')
                
#                 # Localize the naive datetime to CET (treat it as CET time)
#                 dt_cet = cet_tz.localize(dt)
#                 # Convert to IST
#                 dt_ist = dt_cet.astimezone(ist_tz)
#                 # Return as naive datetime string (without timezone info)
#                 return dt_ist.replace(tzinfo=None)
#             except Exception as e:
#                 return pd.NaT

#         if dtype.startswith("datetime"):
#             df[col] = pd.to_datetime(df[col].apply(convert_to_ist), utc=True)
            
#         elif dtype == "int":
#             df[col] = pd.to_numeric(df[col], errors="coerce").astype('Int64')  # Nullable int
#         elif col in df.columns:
#             df[col] = df[col].astype(dtype)

#     df = df.replace({np.nan :None})
#     return df


def _enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    def convert_to_ist(date_str):
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")

            cet_tz = pytz.timezone('Europe/Amsterdam')
            ist_tz = pytz.timezone('Asia/Kolkata')

            dt_cet = cet_tz.localize(dt)
            dt_ist = dt_cet.astimezone(ist_tz)

            return dt_ist.replace(tzinfo=None)  # 👈 already naive
        except:
            return pd.NaT

    for col, dtype in SCHEMA.items():
        if col not in df.columns:
            continue

        if dtype.startswith("datetime"):
            df[col] = df[col].apply(convert_to_ist)

        elif dtype == "int":
            df[col] = pd.to_numeric(df[col], errors="coerce")

        else:
            df[col] = df[col].astype(dtype)

    # 🔥 CRITICAL FIXES
    df = df.where(pd.notnull(df), None)  # replace NaN with None

    # ✅ Convert ALL numpy types → Python native
    def normalize(x):
        if isinstance(x, np.integer):
            return int(x)
        if isinstance(x, np.floating):
            return float(x)
        if isinstance(x, np.bool_):
            return bool(x)
        if isinstance(x, pd.Timestamp):
            return x.to_pydatetime()
        return x

    df = df.apply(lambda col: col.map(normalize))

    return df

def _deduplicate_by_sys_id(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fast deduplication: keep only the row with max sys_updated_on per sys_id.
    Preserves column names and order.
    """
    if df.empty:
        return df

    if "sys_id" not in df.columns or "sys_updated_on" not in df.columns:
        raise ValueError("DataFrame must contain 'sys_id' and 'sys_updated_on' columns")

    # Ensure datetime for correct comparison (vectorized, fast)
    df = df.copy()
    df["sys_updated_on"] = pd.to_datetime(df["sys_updated_on"], errors="coerce")

    # Compute max sys_updated_on per sys_id
    max_times = df.groupby("sys_id", as_index=False)["sys_updated_on"].max()

    # Join back to get full rows (avoids sorting the entire df)
    df = df.merge(max_times, on=["sys_id", "sys_updated_on"], how="inner")

    # Preserve original column order
    df = df[df.columns.tolist()]

    return df.reset_index(drop=True)


def _preprocess_data(data)->pd.DataFrame:
    flat_data = _flatten_data(data)    
    df = pd.DataFrame(flat_data)
    df = _enforce_schema(df)
    df = _deduplicate_by_sys_id(df)
    return df
    
    
def _enrich_data(df:pd.DataFrame)->pd.DataFrame:
    monitoring_keywords = {
        'Memory Utilization': {'Memory', 'memory'},
        'CPU Utilization': {'CPU', 'cpu', 'Processor', 'processor'},
        'Disk Utilization': {'Filesystem', 'Disk', 'Capacity'},
        'Tablespace Utilization': {'Tablespace'},
        'Swap Space Utilization': {'Swap'},
        'Availability' : {'Unreachable', 'Availability'}
    }
    def classify_monitoring_dimension(df):
        for dimension, keywords in monitoring_keywords.items():
            if any(re.search(r'\b' + re.escape(keyword) + r'\b', df, re.IGNORECASE) for keyword in keywords):
                return dimension
        return 'Other'  # Default if no match found
    
    df['server_name'] = df['short_description'].str.extract(r'ZABBIX:\s*(\S+)', expand=False).astype(str)
    df['utilization %'] = df['short_description'].str.extract(r'(\d+(?:\.\d+)?\s?%)')
    #df['% Target achievement of resolution time'].astype(float).fillna(0)
    df['utilization %'] = (df['utilization %'].str.replace(' ', '', regex=False).str.replace('%', '', regex=False).astype(float)).fillna(0)
    df['monitoring_dimension'] = df['short_description'].apply(classify_monitoring_dimension)
    df["latest_poll_timestamp"] = datetime.now(pytz.timezone("Asia/Kolkata"))

    return df


def transform(data:list, logger:logging.Logger)->pd.DataFrame:
    try:
        df = _preprocess_data(data=data)
        df = _enrich_data(df=df)
        return df
    
    except Exception as e:
        raise RecoverableError(f"Transform failed: {e}")