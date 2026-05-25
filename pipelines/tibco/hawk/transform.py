import pandas as pd
from dateutil import parser
import hashlib
from exceptions.RecoverableError import RecoverableError

def parse_elastic_data_to_df(data:dict)->pd.DataFrame:
    records = []
    for hit in data['hits']['hits']:
        record = hit['_source']['tibco_hawk_index'].copy()
        record['_id'] = hit['_id']
        records.append(record)
    
    df = pd.DataFrame(records)
    return df.astype(str).fillna('')


def transform(data:dict)->dict:
    try:
        df = parse_elastic_data_to_df(data)
        return df
    except Exception as e:
        raise RecoverableError(f"Recoverable Error : {e}")
    
