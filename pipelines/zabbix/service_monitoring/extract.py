import logging
import requests
from config.zabbix_config import ZABBIX_CONFIG
from connectors.zabbix_connector import ZABBIX_CONNECTOR
from exceptions.ExtractError import ExtractError
from exceptions.RecoverableError import RecoverableError
import pandas as pd
import pipelines.zabbix.service_monitoring.pipeline as sm
from framework.paths import DATA_DIR
import copy

def enrich_payload(zabbix_config: ZABBIX_CONFIG):
    cached_itemids = sm.ZABBIX_SERVICE_MONITORING._cache.get("itemids", None)

    if cached_itemids is None:
        ids = pd.read_csv(f"{DATA_DIR}/zabbix/service_monitoring/service_monitoring_itemids.csv", encoding='utf-8-sig')
        itemids = (
            ids["itemids"]
            .dropna()
            .astype(str)
            .str.strip()
            .tolist()
            )
        # Cache the LIST, not the DataFrame it came from. Caching `ids` meant
        # every later run read a DataFrame back out and iterating it yielded
        # the column names, so the payload asked Zabbix for the item id
        # "itemids" and matched nothing.
        sm.ZABBIX_SERVICE_MONITORING._cache["itemids"] = itemids
    else:
        itemids = cached_itemids

    if not itemids:
        # Never send an empty itemids filter: the id list is the only thing
        # scoping this query, and an unscoped item.get would pull the whole
        # Zabbix item table.
        raise RecoverableError(
            "No item ids available for ZABBIX_SERVICE_MONITORING "
            "(service_monitoring_itemids.csv produced an empty list)."
        )

    payload = copy.deepcopy(zabbix_config.payload)
    payload["params"]["itemids"] = list(itemids)
    return payload



def extract_data(zabbix_config:ZABBIX_CONFIG, logger:logging.Logger):
    try:
        zabbix_conn = ZABBIX_CONNECTOR(config=zabbix_config)
 
        payload = enrich_payload(zabbix_config=zabbix_config)
        data = zabbix_conn._fetch(payload=payload, method=zabbix_config.api_method)

        if not data or data.get("result", []) == []:
            raise RecoverableError(f"API returned empty result!")

        return data.get("result", [])
    
    except Exception as e:
        raise ExtractError(f"Error in Extract Stage: {type(e).__name__} - {e}")
