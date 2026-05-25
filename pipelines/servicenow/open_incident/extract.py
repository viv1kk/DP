import logging
import requests
from config.servicenow_config import SNOW_CONFIG
from connectors.servicenow_connector import SNOW_CONNECTOR
from exceptions.RecoverableError import RecoverableError


def extract_data(snow_config:SNOW_CONFIG, logger:logging.Logger):
    try:
        snow_conn = SNOW_CONNECTOR(config=snow_config)
        data = snow_conn.fetch_incidents("2025-11-01")
        return data
    
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise RecoverableError(f"{e}")
