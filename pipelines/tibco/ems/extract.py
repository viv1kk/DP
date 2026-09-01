import requests
import pandas as pd
from config.es_config import ESConfig
from framework.decorators import retry
from connectors.elastic_connector import ELASTIC_CONNECTOR
from exceptions.RecoverableError import RecoverableError
import copy

def extract_data(logger, es_config: ESConfig, search_after=None):
    try:
        all_data = []
        after_key = search_after
        connector = ELASTIC_CONNECTOR(es_config)

        while True:
            # ✅ Always deep copy to avoid mutation bugs
            base_payload = copy.deepcopy(es_config.payload)

            # ✅ Apply pagination correctly
            if after_key:
                base_payload["aggs"]["by_queue"]["composite"]["after"] = after_key

            es_data = connector.fetch(base_payload, "POST")

            if "error" in es_data:
                raise RecoverableError(f"Elastic Error: {es_data['error']}")

            buckets = es_data["aggregations"]["by_queue"]["buckets"]

            if not buckets:
                break

            for bucket in buckets:
                hit = bucket["latest_record"]["hits"]["hits"][0]
                source = hit["_source"]["emscheck"]
                source["_id"] = hit["_id"]
                all_data.append(source)

            # ✅ Correct usage: ES already returns dict
            after_key = es_data["aggregations"]["by_queue"].get("after_key")

            if not after_key:
                break

        if not all_data:
            raise RecoverableError("Empty result! fetched from TIBCO_EMS")

        return all_data

    except RecoverableError as e:
        raise

    except Exception as e:
        raise
