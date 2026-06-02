import pandas as pd
from exceptions.RecoverableError import RecoverableError
import numpy as np
import time

def _add_status_to_microagents(microagent_list, m_status):
    try:
        microagent_df = microagent_list
    
        status_df = pd.DataFrame(m_status);      
        status_df.drop(["host_status", "type"], axis=1, errors="ignore", inplace=True)
        df = microagent_df.merge(status_df, on=["dns", "domain", "host_name", "component", "deployment"], how="left")

        # df[df["status"] == ""] = "Unknown"
        
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
        return df
        
    except Exception as e:
        RecoverableError(f"{e}")
    return pd.DataFrame()

def transform(data:dict)->pd.DataFrame:
    try:
        microagent_list = data["microagent_list"]
        m_status = data["microagent_status"]

        df = _add_status_to_microagents(microagent_list=microagent_list, m_status=m_status)
        return df
    except Exception as e:
        raise RecoverableError(f"Recoverable Error: {e}")
    
