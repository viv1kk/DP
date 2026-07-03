import pandas as pd
from exceptions.RecoverableError import RecoverableError
import numpy as np
import time
import pipelines.hawk.pipeline as hawk

def _add_status_to_microagents(microagent_list, m_status):
    try:
        microagent_df = microagent_list
    
        status_df = pd.DataFrame(m_status);      
        status_df.drop(["host_status", "type"], axis=1, errors="ignore", inplace=True)

        df = microagent_df.merge(
            status_df,
            on=["dns", "domain", "host_name", "component", "deployment"],
            how="left",
            suffixes=("", "_status")
        )

        df = df.drop(columns=["microagent_name_status"])

        df["status"] = df["status"].fillna("").replace("", "STOPPED_INFERRED")
        
        # Adding timstamp to keep track of the poll time.
        now_ist = int(time.time())
        df["last_poll_at_IST"] = now_ist
    
        def to_tz(series, zone):
            return (
                pd.to_datetime(series, unit="s", utc=True)
                .dt.tz_convert(zone)
                .dt.tz_localize(None)
            )
        df["last_poll_at_IST"] = to_tz(df["last_poll_at_IST"], "Asia/Kolkata")
        # obj_cols = df.select_dtypes(include=["object"]).columns
        # df[obj_cols] = df[obj_cols].astype(str) 
        df = df.fillna('')
        df.drop_duplicates(inplace=True)
        return df
        
    except Exception as e:
        return pd.DataFrame()

def create_host_status_df(df:pd.DataFrame)->pd.DataFrame:
    try:
        app_cache_df = hawk.HAWK_APPLICATION_MONITORING._cache["last_microagent_data"].copy()

        host_df_fields = ["dns", "domain", "host_name", "host_status", "host_tracked", "last_poll_at_IST"]

        host_df = df[[field for field in host_df_fields if field not in ["host_tracked"]]]
        
        host_df_latest = (
            host_df
            .sort_values("last_poll_at_IST")
            .drop_duplicates(
                subset=["dns", "domain", "host_name"],
                keep="last"
            )
            .reset_index(drop=True)
        )
        
        host_df_latest["host_tracked"] = np.where(
            host_df_latest["host_status"] != "UNKNOWN",
            "Yes",
            "No"
        )

        app_cache_df_host = (
            app_cache_df[["dns", "domain", "host_name"]]
            .drop_duplicates(
                subset=["dns", "domain", "host_name"],
                keep="last"
            )
            .reset_index(drop=True)
        )

        host_df = host_df_latest.merge(
            app_cache_df_host,
            on=["dns", "domain", "host_name"],
            how="left",
            suffixes=("", "_status")
        )

        return host_df
    except:
        return pd.DataFrame(columns=host_df_fields)

def create_microagent_status_df(df:pd.DataFrame)->pd.DataFrame:
    try:
        df["prev_status"] = ""
        m_df_fields = ["dns", "domain", "host_name", "type", 
                    "deployment", "component", "microagent_name", 
                    "status", "application_tracked", "last_poll_at_IST", "prev_status"]
        
        m_df = df[m_df_fields]
        m_df.drop_duplicates(inplace=True)
        return m_df
    except:
        return pd.DataFrame(columns=m_df_fields)

def transform(data:dict)->dict:
    try:
        microagent_list = data["microagent_list"]
        m_status = data["microagent_status"]

        df = _add_status_to_microagents(microagent_list=microagent_list, m_status=m_status)

        host_status_df = create_host_status_df(df.copy())
        microagent_status_df = create_microagent_status_df(df.copy())

        return {
            "raw_df" : df,
            "microagent_status_df" : microagent_status_df,
            "host_status_df" : host_status_df
        }
        
    except Exception as e:
        raise RecoverableError(f"Error : {e}")
    
