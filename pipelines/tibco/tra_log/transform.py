import pandas as pd
from dateutil import parser
import hashlib
from exceptions.RecoverableError import RecoverableError

def parse_elastic_data_to_df(data:dict)->pd.DataFrame:
    """
    Convert Elasticsearch JSON output to a flattened pandas DataFrame.
    Includes '_id' and all fields from '_source', with nested fields flattened.
    """
    hits = data.get('hits', {}).get('hits', [])

    records = []
    for hit in hits:
        record = {'_id': hit.get('_id')}
        source = hit.get('_source', {})
        record.update(source)
        records.append(record)

    # Flatten nested fields using json_normalize
    df = pd.json_normalize(records, sep='.')
    return df.astype(str).fillna('')

def add_search_column(df:pd.DataFrame)->pd.DataFrame:
    df['search'] = df['_id'] + '|' + df['host.name'] + '|' + df['bw'] + '|' + df['app']
    return df

def parse_iso_timestamps(df:pd.DataFrame, column_name:str)->pd.DataFrame:
    if not df.empty :
        # df['timestamp'] = df['timestamp'].str.replace(r'Z$', '+00:00', regex=True)
        def safe_parse(date_str):
            try:
                return parser.isoparse(date_str)
            except Exception:
                return pd.NaT
    
        df[f'formatted_{column_name}'] = pd.to_datetime(df[column_name].apply(safe_parse), errors='coerce', utc=True)
        df[f'formatted_{column_name}_CET'] = df[f'formatted_{column_name}'].dt.tz_convert('CET')
        df[f'formatted_{column_name}_IST'] = df[f'formatted_{column_name}'].dt.tz_convert("Asia/Kolkata")
        df['event_timeslot_10min'] = df[f'formatted_{column_name}'].dt.floor('10min')
    return df


def transform(data:dict)->pd.DataFrame:
    try:
        df = parse_elastic_data_to_df(data)
        if not df.empty:
            df = parse_iso_timestamps(df, "@timestamp")
            df = add_search_column(df)

            # Update previous_audit_error_ids
            df.drop_duplicates(subset=["_id"], inplace=True)
            df.fillna('')
            df = (
                df.sort_values("@timestamp", ascending=False)
                  .drop_duplicates(subset=["host.id", "app", "error"], keep="first")
                  .reset_index(drop=True)
            )
        return df
    except Exception as e:
        raise RecoverableError(f"Recoverable Error : {e}")
    
