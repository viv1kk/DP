import logging
import requests
from config.zabbix_config import ZABBIX_CONFIG
from connectors.zabbix_connector import ZABBIX_CONNECTOR
from exceptions.RecoverableError import RecoverableError
import time
import copy

def enrich_active_problem_payload(payload:dict)->dict:
    time_from = int(time.time()) - 24 * 60 * 60 # last 24 hours
    payload["params"]["time_from"] = time_from
    return payload


def enrich_trigger_payload(payload:dict, triggerid_list: list)->dict:
    payload["params"]["triggerids"] = triggerid_list
    return payload


def extract_data(zabbix_config:ZABBIX_CONFIG, logger:logging.Logger)->dict:
    try:
        zabbix_conn = ZABBIX_CONNECTOR(config=zabbix_config)

        active_problem_payload = copy.deepcopy(zabbix_config.payload["active_problems"])
        active_problem_payload = enrich_active_problem_payload(active_problem_payload)
        active_problems = zabbix_conn._fetch(payload=active_problem_payload, method=zabbix_config.api_method)
        if not active_problems:
            raise RecoverableError("active_problems API returned empty result!")
        active_problems = active_problems["result"]

        trigger_payload = copy.deepcopy(zabbix_config.payload["triggers"])
        triggerid_list = list({prob["objectid"] for prob in active_problems})
        trigger_payload = enrich_trigger_payload(trigger_payload, triggerid_list)
        triggers = zabbix_conn._fetch(payload=trigger_payload, method=zabbix_config.api_method)
        if not triggers:
            raise RecoverableError("host_availability API returned empty result!")

        triggers = triggers["result"]

        return {
            "active_problems" : active_problems,
            "triggers" : triggers
        }
    
    except Exception as e:
        raise RecoverableError(f"{e}")
