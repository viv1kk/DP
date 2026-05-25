import logging
import requests
from config.zabbix_config import ZABBIX_CONFIG
from connectors.zabbix_connector import ZABBIX_CONNECTOR
from exceptions.RecoverableError import RecoverableError


def extract_data(zabbix_config:ZABBIX_CONFIG, logger:logging.Logger)->dict:
    try:
        zabbix_conn = ZABBIX_CONNECTOR(config=zabbix_config)
        host_with_group = zabbix_conn._fetch(payload=zabbix_config.payload["host_with_group"], method=zabbix_config.api_method)
        if not host_with_group:
            raise RecoverableError("host_with_group API returned empty result!")

        host_avaliability = zabbix_conn._fetch(payload=zabbix_config.payload["host_availability"], method=zabbix_config.api_method)
        if not host_avaliability:
            raise RecoverableError("host_availability API returned empty result!")

        return {
            "host_with_group" : host_with_group["result"],
            "host_availability" : host_avaliability["result"]
        }
    
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise RecoverableError(f"{e}")
