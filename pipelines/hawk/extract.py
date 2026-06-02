import requests
import pandas as pd
from config.hawk_config import HAWK_CONFIG
from framework.decorators import retry
from connectors.hawk_connector import HAWK_CONNECTOR
from exceptions.RecoverableError import RecoverableError
import copy
import logging
import re

def _identify_microagent_type(param):
    PATTERN = re.compile(
        r"^COM\.TIBCO\.ADAPTER\."
        r"(?P<type>[A-Za-z0-9_-]+)\."
        r"(?P<domain>[A-Za-z0-9_-]+)\."
        r"(?P<deployment>[A-Za-z0-9_-]+(?::[0-9]+)?)\."
        r"(?P<component>[A-Za-z0-9_-]+)(?::(?P<number>[0-9]+))?$"
    )

    def parse_adapter_string(s: str):
        match = PATTERN.match(s)
        return match.groupdict() if match else None

    name = param.get("microagent_name", "")
    domain = param.get("domain", "")

    if not name:
        return None

    match = parse_adapter_string(name)
    if not match:
        return None

    if domain != match.get("domain", ""):
        return None
    match.pop("number", None)
    return match

def enrich_microagents(microagent_list):
    for item in microagent_list:
        result = _identify_microagent_type(item)

        if isinstance(result, dict):
            item.update(result)   # 🔥 This merges new keys into same dict

    return microagent_list

def extract_data(hawk_config: HAWK_CONFIG, logger: logging.Logger):
    try:
        conn = HAWK_CONNECTOR(config=hawk_config)
        domains_list = conn.get_all_domains()
        agent_list = conn.get_all_agents(domains_list)        
        microagent_list = conn.get_all_microagents(agent_list)
        
        return {
            "domains_list" : domains_list,
            "agent_list" : agent_list,
            "microagent_list" : microagent_list
        }
        
        # microagent_list = conn.enrich_microagents(microagent_list)
        
        # df_merged = self.merge_microagent_with_cache(pd.DataFrame(microagent_list), self.cache)
        
        # data = self._get_status_for_all_microagents(df_merged)       

    except RecoverableError as e:
        logger.warning(f"Recoverable error: {e}")
        raise

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise
