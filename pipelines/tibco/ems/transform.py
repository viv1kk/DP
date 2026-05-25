import pandas as pd
import hashlib
import json
from dateutil import parser
from exceptions.RecoverableError import RecoverableError

schema = {
    # string
    "interfaceid": "str",
    "enginename": "str",
    "areaname": "str",
    "businessobject": "str",
    "businessobjectid": "str",
    "checktype": "str",
    "envlevel": "str",
    "envType": "str",
    "queuename": "str",
    "queuestate": "str",
    "_id": "str",

    # integers
    "queuemessagecount": "int",
    "pendingmessagesizekb": "int",
    "messagedeliverycount": "int",

    # datetimes
    "timestamp": "datetime",
    "maxallowedagetimestamp": "datetime",
    "oldestmessagetimestamp": "datetime",
    "normalmessageagetimestamp": "datetime",

    # JSON / list / dict stored as string
    "attributes": "json"
}

def apply_schema(df: pd.DataFrame) -> pd.DataFrame:
    
    df_out = df.copy()

    for col, dtype in schema.items():
        if col not in df_out.columns:
            continue

        if dtype == "str":
            df_out[col] = df_out[col].astype(str)

        elif dtype == "int":
            # nullable int safe for SQL
            df_out[col] = pd.to_numeric(df_out[col], errors="coerce").astype("int64")

        elif dtype == "datetime":
            df_out[col] = pd.to_datetime(df_out[col], errors="coerce", utc=True).dt.tz_convert('Asia/Kolkata')

        elif dtype == "json":
            df_out[col] = df_out[col].apply(
                lambda v: json.dumps(v) if isinstance(v, (dict, list)) else str(v)
            )

        else:
            # fallback — convert to string
            df_out[col] = df_out[col].astype(str)

    return df_out


def transform(data:dict)->dict:
    try:
        df = pd.DataFrame(data=data)
        df = apply_schema(df)
       
        return df
    except Exception as e:
        raise RecoverableError(f"Recoverable Error : {e}")
    
