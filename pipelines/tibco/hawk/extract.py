import requests
import pandas as pd
from config.es_config import ESConfig
from framework.decorators import retry
from connectors.elastic_connector import ELASTIC_CONNECTOR
from exceptions.RecoverableError import RecoverableError
import copy

# @retry()
def extract_data(logger, es_config:ESConfig, search_after = None):
    try:
        all_data = []
        current_search_after = search_after
        connector = ELASTIC_CONNECTOR(es_config)

        while True:
            # preparing for the next range.
            base_payload = copy.deepcopy(es_config.payload)
            if current_search_after:
                base_payload["search_after"] = current_search_after
            
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
            raise RecoverableError("Empty result! fetched from TIBCO_HAWK")

        return final_data
    except RecoverableError as e:
        logger.warning(f"Recoverable error: {e}")
        raise
    
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise
