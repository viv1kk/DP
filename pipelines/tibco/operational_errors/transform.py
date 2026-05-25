import pandas as pd
from dateutil import parser
import hashlib
from exceptions.RecoverableError import RecoverableError


base_headers = [
    '_id', 'interfaceid', 'enginename', 'componenttype',
    'machinename', 'businessobject', 'businessobjectid',
    'subbusinessobject', 'correlationid', 'logtype', 'timestamp',
    'Operation_User','Operation_Action','Operation_Timestamp',
    'Operation_SNC_Ticket', 'Operation_Comments'
]


def parse_elastic_data_to_df(data:dict)->list:
    hits = data.get('hits', {}).get('hits', [])
    rows = []
    
    logdetail_keys = set()
    for item in hits:
        logdetails = item.get('_source', {}).get('elasticEvent', {}).get('logdetails', {})
        logdetail_keys.update(logdetails.keys())

    logdetail_keys = sorted(logdetail_keys)
    logdetail_headers = [f'logdetails_{key}' for key in logdetail_keys]
    headers = base_headers + logdetail_headers

    for item in hits:
        elastic_event = item.get('_source', {}).get('elasticEvent', {})
        row = {
            '_id': str(item.get('_id', '')),
            'interfaceid': str(elastic_event.get('interfaceid', '')),
            'enginename': str(elastic_event.get('enginename', '')),
            'componenttype': str(elastic_event.get('componenttype', '')),
            'machinename': str(elastic_event.get('machinename', '')),
            'businessobject': str(elastic_event.get('businessobject', '')),
            'businessobjectid': str(elastic_event.get('businessobjectid', '')),
            'subbusinessobject': str(elastic_event.get('subbusinessobject', '')),
            'correlationid': str(elastic_event.get('correlationid', '')),
            'logtype': str(elastic_event.get('logtype', '')),
            'timestamp': str(elastic_event.get('timestamp', '')),
            'Operation_User' : str(elastic_event.get("Operations", [{}])[0].get("User", '')),
            'Operation_Action' : str(elastic_event.get("Operations", [{}])[0].get("Action", '')),
            'Operation_Timestamp' : str(elastic_event.get("Operations", [{}])[0].get("Timestamp", '')),
            'Operation_SNC_Ticket' : str(elastic_event.get("Operations", [{}])[0].get("SNCTicket", '')),
            'Operation_Comments' : str(elastic_event.get("Operations", [{}])[0].get("Comments", '')),
        }
        for key in logdetail_keys:
            row[f'logdetails_{key}'] = str(elastic_event.get('logdetails', {}).get(key, ''))
        
        rows.append(row)

    df = pd.DataFrame(rows, columns=headers)
    df = df.astype(str).fillna('')
    
    return df


def generate_server_name(df, source_col, new_col):
    df[new_col] = df[source_col].str.split('.').str[0].str.lower()
    return df


def extract_job_number(df: pd.DataFrame, source_col: str, target_col: str) -> pd.DataFrame:
    df[target_col] = (
        df[source_col]
        .str.extract(r"JobNumber\s*-\s*([^\s]+)")[0]   # capture the part after 'JobNumber - '
        .fillna("")                                    # replace NaN with empty string
        .astype(str)                                   # ensure dtype is string
    )
    return df
    
def add_search_column(df):
    df['search_field'] = df['_id']+'|'+ df['correlationid'] + '|' + df['enginename'] + '|' + df['businessobject'] + '|' + df['subbusinessobject'] + '|' + df['logtype'] + '|' + df['interfaceid'] +'|'+ df['machinename']+ '|' + df['Operation_JobNumber'] +'|'+ df['Operation_SNC_Ticket']+'|'+ df['Operation_User']
    return df

def parse_iso_timestamps(df, column_name):
    if not df.empty :
        # df['timestamp'] = df['timestamp'].str.replace(r'Z$', '+00:00', regex=True)
        def safe_parse(date_str):
            try:
                return parser.isoparse(date_str)
            except Exception:
                return pd.NaT
    
        df['formatted_timestamp'] = pd.to_datetime(df[column_name].apply(safe_parse), errors='coerce', utc=True)
        df['minutes_since_created'] = (pd.Timestamp.utcnow() - df["formatted_timestamp"]).dt.total_seconds() / 60
        df['formatted_timestamp_CET'] = df['formatted_timestamp'].dt.tz_convert('CET')
        df['formatted_timestamp_IST'] = df['formatted_timestamp'].dt.tz_convert('Asia/Kolkata')
        df['event_timeslot_10min'] = df['formatted_timestamp'].dt.floor('10min')
    return df

def generate_log_key(df: pd.DataFrame) -> pd.DataFrame:
    # Sort columns alphabetically
    sorted_columns = sorted(df.columns)

    # Convert each row to a single concatenated string (with all values as strings)
    def row_hash(row):
        concatenated = ''.join(str(row[col]) for col in sorted_columns)
        return hashlib.sha256(concatenated.encode('utf-8')).hexdigest()

    # Apply the hash function row-wise
    df['primary_hash'] = df.apply(row_hash, axis=1)
    return df

def transform(data:dict)->dict:
    try:
        df = parse_elastic_data_to_df(data)
        if not df.empty:
            df = generate_server_name(df, "machinename", "servername")
            df = extract_job_number(df, source_col="Operation_Comments", target_col="Operation_JobNumber")
            df = parse_iso_timestamps(df, "timestamp")
            df = add_search_column(df)
            df = generate_log_key(df)

        df.drop_duplicates(subset=["_id"], inplace=True)
        df.fillna('', inplace=True)

        return df
    except Exception as e:
        raise RecoverableError(f"Recoverable Error : {e}")
    
