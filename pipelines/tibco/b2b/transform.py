import pandas as pd
from dateutil import parser
import hashlib
from exceptions.RecoverableError import RecoverableError


base_headers = [
    'correlationid', 'usertransid', 'bctransid', 'domain', 'msgtype',
    'msgversion1', 'msgversion2', 'msgversion3', 'operationdid',
    'eventtype', 'eventmsg', 'source', 'target', 'tpid', 'direction',
    'aux1', 'aux2', 'aux3', 'aux7', 'aux12', 'timestamp', '_id',
    'tpname', 'hostid'
]


def _preprocess_json_data_to_df(data:dict):
    records = []

    for hit in data.get("hits", {}).get("hits", []):
        event_id = hit.get("_id")
        event_data = hit.get("_source", {}).get("elasticEvent", {})

        # Extract base fields excluding 'attributes' and 'states'
        base_fields = {k: v for k, v in event_data.items() if k not in ("attributes", "states")}
        base_fields["_id"] = event_id

        # Process states as comma-separated values
        # states = event_data.get("states", [])
        # base_fields["state"] = ",".join(s.get("state", "") for s in states)
        # base_fields["state_timestamp"] = ",".join(s.get("timestamp", "") for s in states)
        # base_fields["state_description"] = ",".join(s.get("description", "") for s in states)

        records.append(base_fields)

    df = pd.DataFrame(records)
    df = df.reindex(columns=base_headers)

    return df.astype(str).fillna('')


def _parse_iso_timestamps(df, column_name):
    if not df.empty :
        # df['timestamp'] = df['timestamp'].str.replace(r'Z$', '+00:00', regex=True)
        def safe_parse(date_str):
            try:
                return parser.isoparse(date_str)
            except Exception:
                return pd.NaT
    
        df['timestamp_UTC'] = pd.to_datetime(df[column_name].apply(safe_parse), errors='coerce', utc=True)
        df['timestamp_CET'] = df['timestamp_UTC'].dt.tz_convert('CET')
        df['timestamp_IST'] = df['timestamp_UTC'].dt.tz_convert('Asia/Kolkata')
        df.drop(columns=["timestamp"], inplace=True)
    return df

def transform(data:dict)->dict:
    try:
        df = _preprocess_json_data_to_df(data = data)
        df = _parse_iso_timestamps(df, "timestamp")
        return df
    except Exception as e:
        raise RecoverableError(f"Recoverable Error : {e}")
    
