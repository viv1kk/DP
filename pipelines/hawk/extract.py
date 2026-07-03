import requests
import pandas as pd
from config.hawk_config import HAWK_CONFIG
from framework.decorators import retry
from connectors.hawk_connector import HAWK_CONNECTOR
from exceptions.RecoverableError import RecoverableError
import copy
import logging
import re
import pipelines.hawk.pipeline as hawk

# def _identify_microagent_type(param):
#     PATTERN = re.compile(
#         r"^COM\.TIBCO\.ADAPTER\."
#         r"(?P<type>[A-Za-z0-9_-]+)\."
#         r"(?P<domain>[A-Za-z0-9_-]+)\."
#         r"(?P<deployment>[A-Za-z0-9_-]+(?::[0-9]+)?)\."
#         r"(?P<component>[A-Za-z0-9_-]+)(?::(?P<number>[0-9]+))?$"
#     )

#     def parse_adapter_string(s: str):
#         match = PATTERN.match(s)
#         return match.groupdict() if match else None

#     name = param.get("microagent_name", "")
#     domain = param.get("domain", "")

#     if not name:
#         return None

#     match = parse_adapter_string(name)
#     if not match:
#         return None

#     if domain != match.get("domain", ""):
#         return None
#     match.pop("number", None)
#     return match


import re


import re


def _identify_microagent_type(param):
    pattern_with_domain = re.compile(
        r"^COM\.TIBCO\.ADAPTER\."
        r"(?P<type>[A-Za-z0-9_-]+)\."
        r"(?P<domain>[A-Za-z0-9_-]+)\."
        r"(?P<deployment>[A-Za-z0-9_-]+(?::[0-9]+)?)\."
        r"(?P<component>[A-Za-z0-9_-]+)"
        r"(?::(?P<number>[0-9]+))?$"
    )

    pattern_without_domain = re.compile(
        r"^COM\.TIBCO\.ADAPTER\."
        r"(?P<type>[A-Za-z0-9_-]+)\."
        r"(?P<deployment>[A-Za-z0-9_-]+(?::[0-9]+)?)\."
        r"(?P<component>[A-Za-z0-9_-]+)"
        r"(?::(?P<number>[0-9]+))?$"
    )

    name = param.get("microagent_name", "")
    expected_domain = param.get("domain", "")

    if not name:
        return None

    match = pattern_with_domain.match(name)

    if match:
        result = match.groupdict()

        if result["domain"] != expected_domain and result["domain"] != "":
            result.pop("domain", None)

    else:
        match = pattern_without_domain.match(name)

        if not match:
            return None

        result = match.groupdict()

    result.pop("number", None)

    return result

def enrich_microagents(microagent_list, logger: logging.Logger):
    try:
        for item in microagent_list:
            result = _identify_microagent_type(item)

            if isinstance(result, dict):
                item.update(result)   # 🔥 This merges new keys into same dict

        return microagent_list

    except Exception as e:
        logger.error(f"Error: {e}")
        return []


def merge_microagent_with_cache(microagent_df, app_cache_df):
        """
        Performs required outer merge on multiple keys,
        then correctly fills host_status at host level.
        """
    
        try:
            # ---------------------------------
            # 1️⃣ Defensive copy & cleanup
            # ---------------------------------
            microagent_df = microagent_df.copy()
            app_cache_df = app_cache_df.copy()
    
            microagent_df = microagent_df.fillna("")
            app_cache_df = app_cache_df.fillna("")
    
            key_cols = ["dns", "domain", "host_name", "deployment", "component"]

            for k in key_cols:
                if k not in microagent_df.columns:
                    return app_cache_df
            # Strip whitespace (common ETL issue)
            for col in key_cols:
                microagent_df[col] = microagent_df[col].astype(str).str.strip()
                app_cache_df[col] = app_cache_df[col].astype(str).str.strip()
    
            # ---------------------------------
            # 2️⃣ Required OUTER MERGE (multi-key)
            # ---------------------------------
            df_merged = microagent_df.merge(
                app_cache_df[key_cols],
                on=key_cols,
                how="outer"
            )
    
            # ---------------------------------
            # 3️⃣ Create host-level status mapping
            # ---------------------------------
            host_status_map = (
                microagent_df[["host_name", "host_status"]]
                .drop_duplicates(subset=["host_name"])
                .set_index("host_name")["host_status"]
            )
    
            # ---------------------------------
            # 4️⃣ Fill missing host_status safely
            # ---------------------------------
            df_merged["host_status"] = (
                df_merged["host_status"]
                .replace("", None)
                .fillna(df_merged["host_name"].map(host_status_map))
            )
    
            # Optional: fallback value
            df_merged["host_status"] = df_merged["host_status"].fillna("UNKNOWN")
            df_merged.fillna("", inplace=True)
            return df_merged
    
        except Exception as e:
            raise Exception(f"Merge failed: {str(e)}")

def extract_data(hawk_config: HAWK_CONFIG, logger: logging.Logger):
    try:
        conn = HAWK_CONNECTOR(config=hawk_config)
        domains_list = conn.get_all_domains()
        agent_list = conn.get_all_agents(domains_list)        
        microagent_list = conn.get_all_microagents(agent_list)
        microagent_list = enrich_microagents(microagent_list, logger)

        microagent_df = pd.DataFrame(microagent_list)
        app_cache_df = hawk.HAWK_APPLICATION_MONITORING._cache["last_microagent_data"]

        df_merged = merge_microagent_with_cache(microagent_df=microagent_df, app_cache_df=app_cache_df)
        microagent_status = conn.get_status_for_all_microagents(df_merged) 

        return {
            "domains_list" : domains_list,
            "agent_list" : agent_list,
            "microagent_list" : df_merged,
            "microagent_status" : microagent_status
        }
        
    except RecoverableError as e:
        logger.warning(f"Recoverable error: {e}")
        return {
            "domains_list" : [],
            "agent_list" : [],
            "microagent_list" : pd.DataFrame(),
            "microagent_status" : []
        }

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise
