import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from config.es_config import ESConfig
from framework.decorators import retry
from connectors.elastic_connector import ELASTIC_CONNECTOR
from exceptions.RecoverableError import RecoverableError
import copy


"""
Extracting data from the index (last 1 day)
"""

def get_start_timestamp(days=1):
    ts = datetime.now(timezone.utc) - timedelta(days)
    timestamp_str = ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    return timestamp_str


# @retry()
def extract_data(logger, es_config:ESConfig):
    try:
        all_data = []
        search_after = get_start_timestamp(days=1)
        connector = ELASTIC_CONNECTOR(es_config)

        current_search_after = None

        while True:
            # preparing for the next range.
            base_payload = copy.deepcopy(es_config.payload)


            # Current_search_after store the object used for sorting (kind of a pagination)
            if current_search_after:
                base_payload["search_after"] = current_search_after

            # This is the initial state from which the data fetch will start
            else:
                base_payload["query"]["range"]["@timestamp"]["gt"] = search_after


            es_data = connector.fetch(base_payload, "POST")
            if "error" in es_data.keys():
                raise RecoverableError(f"Elastic Error: {es_data['error']}")

        
            hits = es_data.get("hits", {}).get("hits", [])
            if not hits:
                break
            
            all_data.extend(hits)

            # Set search_after for the next batch
            if len(hits) > 0:
                last_hit = hits[-1]
                current_search_after = last_hit['sort']

            if len(hits) < 10000:
                break

        final_data = {"hits": {"hits": all_data}}

        if all_data == []:
            raise RecoverableError("Empty result! fetched from TIBCO_TRA_LOG")
        return final_data
    except RecoverableError as e:
        raise
    
    except Exception as e:
        raise
