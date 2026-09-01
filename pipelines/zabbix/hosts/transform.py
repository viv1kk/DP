import re
import numpy as np
import pandas as pd
from framework.decorators import retry
from exceptions.RecoverableError import RecoverableError
import pytz
import logging
from datetime import datetime
import pipelines.zabbix.hosts.pipeline as zb

def enrich_data(data:list, logger:logging.Logger)->pd.DataFrame:
    try:
        # Step 1: Get Hosts and Groups
        host_groups = data["host_with_group"]
        host_availability = data["host_availability"]

        host_records = []
        for group in host_groups:
            groupid = group.get("groupid")
            group_name = group.get("name")
            for host in group.get("hosts", []):
                host_records.append({
                    "hostid": host["hostid"],
                    "host_name": host["name"],
                    "status": host["status"],
                    "groupid": groupid,
                    "group_name": group_name,
                })
        host_df = pd.DataFrame(host_records)
        host_df["last_poll_at_IST"] = datetime.now(pytz.timezone("Asia/Kolkata"))
        
        # Step 2: Get Availability
        avail_df = pd.DataFrame(host_availability)
        
        # Step 3: Merge
        enriched_df = pd.merge(host_df, avail_df, on="hostid", how="left")
        enriched_df = enriched_df.drop_duplicates(subset=["hostid", "groupid"], keep="first")
        enriched_df["host_name_short"] = (
            enriched_df['host_name']
            .str.lower()
            .str.replace(r'\.nws\.nexperia\.com.*$', '', regex=True)
            .str.replace(r'\.nme\.nexperia\.com.*$', '', regex=True)
            .str.replace(r'\.nexperia\.com.*$', '', regex=True)
            .str.replace(r'\.nxp\.com.*$', '', regex=True)
        )
        enriched_df["host_name_short_for_CMDB"] = (
            enriched_df["host_name_short"].str.replace(r"\..*$", "", regex=True)
        )
        
        enriched_df.fillna("", inplace=True)  # Handle NaNs
        enriched_df.sort_values(by="host_name", inplace=True)
        enriched_df.reset_index(drop=True, inplace=True)

        return enriched_df

    except Exception as e:
        raise RecoverableError(f"Error in enrichment layer: {e}")
    

def get_changed_and_deleted_zabbix_hosts(latest_df: pd.DataFrame)->dict:
    previous_df = zb.ZABBIX_HOSTS._cache.get("last_cached_df", None)

    if previous_df is None or previous_df.empty:
        return {
            "new_df" : latest_df.copy(),
            "upsert_df" : latest_df.copy(), 
            "rows_to_delete" : pd.DataFrame(columns=latest_df.columns),
            "is_initial_run" : True
        }

    compare_columns = [col for col in latest_df.columns if col not in ["hostid", "groupid"]]
    rows_to_upsert = []

    merged = pd.merge(
        latest_df,
        previous_df,
        on=["hostid", "groupid"],
        suffixes=("", "_old"),
        how="outer",
        indicator=True
    )

    for _, row in merged.iterrows():
        if row["_merge"] == "left_only":
            match = latest_df[
                (latest_df["hostid"] == row["hostid"]) &
                (latest_df["groupid"] == row["groupid"])
            ]
            rows_to_upsert.append(match)
        elif row["_merge"] == "both":
            current = latest_df[
                (latest_df["hostid"] == row["hostid"]) &
                (latest_df["groupid"] == row["groupid"])
            ].reset_index(drop=True)

            prev = previous_df[
                (previous_df["hostid"] == row["hostid"]) &
                (previous_df["groupid"] == row["groupid"])
            ].reset_index(drop=True)

            if not current[compare_columns].equals(prev[compare_columns]):
                rows_to_upsert.append(current)

    rows_to_upsert = pd.concat(rows_to_upsert).reset_index(drop=True) if rows_to_upsert else pd.DataFrame(columns=latest_df.columns)

    merged_for_deletes = previous_df.merge(
        latest_df,
        on=["hostid", "groupid"],
        how="left",
        indicator=True
    )

    rows_to_delete = merged_for_deletes[merged_for_deletes["_merge"] == "left_only"]
    rows_to_delete = previous_df.merge(
        rows_to_delete[["hostid", "groupid"]],
        on=["hostid", "groupid"]
    ).reset_index(drop=True)
    

    return {
        "new_df" : latest_df.copy(),
        "upsert_df" : rows_to_upsert, 
        "rows_to_delete" : rows_to_delete,
        "is_initial_run" : False
    }


def transform(data:list, logger:logging.Logger):
    try:
        df = enrich_data(data, logger)
        ret = get_changed_and_deleted_zabbix_hosts(df)
        return ret
    
    except Exception as e:
        raise RecoverableError(f"Transform failed: {e}")