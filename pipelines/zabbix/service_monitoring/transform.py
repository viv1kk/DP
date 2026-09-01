import pandas as pd
import numpy as np
from framework.decorators import retry
from exceptions.RecoverableError import RecoverableError
import logging
import time

def host_extraction(df:pd.DataFrame)->pd.DataFrame:
    df["host_name"] = df["hosts"].str[0].str.get("host")
    df.drop(columns=["hosts"], inplace=True, errors="ignore")
    return df


def rename_columns(df:pd.DataFrame)->pd.DataFrame:
    df.rename(
        columns={
            "hostid": "host_id",
            "key_": "item_key",
            "name": "item_name",
            "delay": "item_delay",
            "itemid" : "item_id",
            "status" : "item_status"
        },
        inplace=True
    )
    return df


def type_coercion(df:pd.DataFrame)->pd.DataFrame:
    int_cols = ["item_id", "value_type", "item_status", "lastclock"]
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        # .astype("Int64")
    
    return df


def timestamp_handling(df:pd.DataFrame)->pd.DataFrame:
    now_ist = int(time.time())

    df["last_poll_at_IST"] = now_ist

    def to_tz(series, zone):
        return (
            pd.to_datetime(series, unit="s", utc=True)
            .dt.tz_convert(zone)
            .dt.tz_localize(None)
        )

    df["lastclock_CET"] = to_tz(df["lastclock"], "Europe/Berlin")
    df["lastclock_IST"] = to_tz(df["lastclock"], "Asia/Kolkata")
    df["last_poll_at_IST"] = to_tz(df["last_poll_at_IST"], "Asia/Kolkata")
    return df


def value_split(df:pd.DataFrame)->pd.DataFrame:
    df["lastvalue_num"] = np.where(
        df["value_type"].isin([0, 3]),
        pd.to_numeric(df["lastvalue"], errors="coerce"),
        None
    )

    df["lastvalue_str"] = np.where(
        ~df["value_type"].isin([0, 3]),
        df["lastvalue"],
        None
    )
    return df

def status_handler(df:pd.DataFrame)->pd.DataFrame:
    df["service_status"] = (df["lastvalue_num"] > 0).map({True: "Active", False: "Inactive"})

    value_type_map = {
        0: "numeric_float",
        1: "character",
        2: "log",
        3: "numeric_unsigned",
        4: "text",
        5: "binary"
    }
    df["value_type"] = df["value_type"].map(value_type_map)

    item_status_map = {
        0 : 'enabled item',
        1 : 'disabled item'
    }
    df["item_status"] = df["item_status"].map(item_status_map)
    return df

def final_cleanup(df:pd.DataFrame)->pd.DataFrame:
    df = df.drop_duplicates(subset=["item_id"])
    df.drop(columns=["lastvalue", "lastclock"], inplace=True, errors="ignore")
    df = df.replace({pd.NA: None, np.nan: None})

    final_cols = [
        "item_id",
        "host_id",
        "host_name",
        "item_key",
        "item_name",
        "item_delay",
        "value_type",
        "item_status",
        "lastvalue_num",
        "service_status",
        "lastvalue_str",
        "lastclock_CET",
        "lastclock_IST",
        "last_poll_at_IST"
    ]
 
    return df[final_cols]


def transform(data:list[dict], logger:logging.Logger)->dict:
    try:
        if not data:
            raise RecoverableError("Received empty data!")
        
        df = pd.DataFrame(data)
        df = host_extraction(df)
        df = rename_columns(df)
        df = type_coercion(df)
        df = timestamp_handling(df)
        df = value_split(df)
        df = status_handler(df)
        df = final_cleanup(df)
        
        return df

    
    except Exception as e:
        raise RecoverableError(f"Transform failed: {e}")