import logging
import requests
from config.zabbix_config import ZABBIX_CONFIG
from connectors.zabbix_connector import ZABBIX_CONNECTOR
from exceptions.RecoverableError import RecoverableError


def extract_data(zabbix_config:ZABBIX_CONFIG, logger:logging.Logger):
    try:
        zabbix_conn = ZABBIX_CONNECTOR(config=zabbix_config)
        data = zabbix_conn._fetch(payload=zabbix_config.payload, method=zabbix_config.api_method)
        if not data:
            raise RecoverableError("API returned empty result!")

        return data["result"]
    
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise RecoverableError(f"{e}")
