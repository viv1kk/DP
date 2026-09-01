from zoneinfo import ZoneInfo
import pandas as pd
from framework.decorators import retry
from exceptions.RecoverableError import RecoverableError
import logging
from datetime import datetime
from framework.paths import DATA_DIR
import pipelines.zabbix.host_problems.pipeline as zb

# Keys produced for every problem/item pair, plus the two columns derived further
# down. Kept explicit so a poll with no usable rows can still return a correctly
# shaped frame instead of a column-less one.
PROBLEM_COLUMNS = [
    "eventid", "problem_time_CET", "problem_time_IST", "clock", "severity",
    "acknowledged", "suppressed", "host_name", "host_id", "problem_name",
    "item_id", "item_key", "item_name", "item_delay",
    "servicenow_ticket", "servicenow_ticket_link",
]
DERIVED_COLUMNS = ["category", "host_name_short"]


def _log_skipped(logger:logging.Logger, eventids:list, reason:str):
    if not eventids:
        return
    sample = ", ".join(str(e) for e in eventids[:10])
    more = f" (+{len(eventids) - 10} more)" if len(eventids) > 10 else ""
    logger.warning(f"Skipped {len(eventids)} problem(s) - {reason}. eventids: {sample}{more}")


def combine_problems_and_triggers(data:dict, logger:logging.Logger):
    triggers = data["triggers"]
    problems = data["active_problems"]

    trigger_map = {trig["triggerid"]: trig for trig in triggers}

    # Combine problems and triggers
    enriched = []
    orphaned = []
    for prob in problems:
        trigger_id = prob["objectid"]
        trigger = trigger_map.get(trigger_id)
        if trigger is None:
            # trigger.get returned fewer triggers than we asked for: the trigger was
            # removed inside the 24h problem window, or the API account cannot see it.
            orphaned.append(prob.get("eventid"))
            continue
        enriched.append({
            "problem": prob,
            "trigger": trigger
        })

    _log_skipped(logger, orphaned, "no matching trigger returned by trigger.get")

    return enriched

def problems_to_dataframe(results, logger:logging.Logger):
    data = []
    no_host = []
    no_items = []
    for entry in results:
        problem = entry.get("problem", {})
        # The key is always present and may be None, so a .get() default never fires.
        trigger = entry.get("trigger") or {}
        hosts = trigger.get("hosts") or []
        items = trigger.get("items") or []

        # A problem needs a host and at least one item to form a row. Dropping it
        # silently is how a live alert used to disappear from the dashboard.
        if not hosts:
            no_host.append(problem.get("eventid"))
            continue
        if not items:
            no_items.append(problem.get("eventid"))
            continue

        host = hosts[0]
        ticket_id = None
        ticket_link = None

        # 1. Extract from tags (if exists)
        for tag in problem.get("tags", []):
            if tag.get("tag", "").lower() == "__zbx_servicenow_number":
                ticket_id = tag.get("value")
            elif tag.get("tag", "").lower() == "__zbx_servicenow_link":
                ticket_link = tag.get("value")
                


        for item in items:
            data.append({
                "eventid": problem.get("eventid"),
                "problem_time_CET": int(problem.get("clock", 0)),
                "problem_time_IST": int(problem.get("clock", 0)),
                "clock" : problem.get("clock"),
                "severity": problem.get("severity"),
                "acknowledged": problem.get("acknowledged"),
                "suppressed" : problem.get("suppressed"),
                "host_name": host.get("host"),
                "host_id" : host.get("hostid"),
                "problem_name": problem.get("name"),
                "item_id": item.get("itemid"),
                "item_key": item.get("key_"),
                "item_name": item.get("name"),
                "item_delay": item.get("delay"),
                "servicenow_ticket": ticket_id,
                "servicenow_ticket_link": ticket_link
            })

    _log_skipped(logger, no_host, "trigger returned no hosts")
    _log_skipped(logger, no_items, "trigger returned no items")

    if not data:
        # No rows is a legitimate state (nothing is alerting). Return the right shape
        # so validate and the truncate-and-insert load empty the table cleanly.
        if results:
            logger.warning(f"All {len(results)} problem(s) were skipped; the destination table will be emptied.")
        else:
            logger.info("No active problems to report; the destination table will be emptied.")
        return pd.DataFrame(columns=PROBLEM_COLUMNS + DERIVED_COLUMNS)

    df = pd.DataFrame(data)
    df = df.astype({
        "severity": "int",
        "acknowledged": "int",
        "suppressed": "int",
        "clock" : "int",
    })
    def unix_to_cet(unix_timestamp):
        """
        Converts a Unix timestamp to a datetime object in Central European Time (CET).
     
        Args:
            unix_timestamp (int or float): The Unix timestamp to convert.
     
        Returns:
            datetime.datetime: A timezone-aware datetime object in CET.
        """

     
        time = datetime.fromtimestamp(unix_timestamp, ZoneInfo("Europe/Berlin"))
     
        return time.replace(tzinfo=None)
        
    def unix_to_ist(unix_timestamp):
        """
        Converts a Unix timestamp to a datetime object in Central European Time (CET).
     
        Args:
            unix_timestamp (int or float): The Unix timestamp to convert.
     
        Returns:
            datetime.datetime: A timezone-aware datetime object in CET.
        """
        # Create a naive datetime object from the Unix timestamp (assumed UTC)
        time = datetime.fromtimestamp(unix_timestamp, ZoneInfo("Asia/Kolkata"))
     
        return time.replace(tzinfo=None)

    df["problem_time_CET"] = pd.to_datetime(df["problem_time_CET"].apply(unix_to_cet), utc=True)
    df["problem_time_IST"] = pd.to_datetime(df["problem_time_IST"].apply(unix_to_ist), utc=True)
    
    item_name_map =  pd.read_csv(f"{DATA_DIR}/zabbix/host_problems/item_name_mapping.csv", encoding="utf-8", usecols=['name', 'category'])
    df = pd.merge(df, item_name_map[["name", "category"]], left_on="item_name", right_on="name", how="left").drop(columns='name')
    df["category"] = df["category"].fillna("Uncategorized")
    
    
    df["host_name_short"] = (
        df['host_name']
        .str.lower()
        .str.replace(r'\.nws\.nexperia\.com.*$', '', regex=True)
        .str.replace(r'\.nme\.nexperia\.com.*$', '', regex=True)
        .str.replace(r'\.nexperia\.com.*$', '', regex=True)
        .str.replace(r'\.nxp\.com.*$', '', regex=True)
    )
    df = df.fillna('')
    return df

def get_changed_and_deleted_zabbix_problems(latest_df: pd.DataFrame)->dict:
    previous_df = zb.ZABBIX_HOST_PROBLEMS._cache.get("last_cached_df")
    
    if previous_df is None or previous_df.empty:
        return {
            "new_df" : latest_df.copy(),
            "upsert_df" : latest_df.copy(), 
            "rows_to_delete" : pd.DataFrame(columns=latest_df.columns),
            "is_initial_run" : True
        }

    compare_columns = [col for col in latest_df.columns if col not in ["eventid", "item_id"]]
    rows_to_upsert = []
    
    # Merge to detect inserted or updated rows
    merged = pd.merge(
        latest_df,
        previous_df,
        on=["eventid", "item_id"],
        suffixes=("", "_old"),
        how="outer",
        indicator=True
    )

    # Detect inserted or changed rows
    for _, row in merged.iterrows():
        if row["_merge"] == "left_only":
            match = latest_df[
                (latest_df["eventid"] == row["eventid"]) &
                (latest_df["item_id"] == row["item_id"])
            ]
            rows_to_upsert.append(match)
        elif row["_merge"] == "both":
            current = latest_df[
                (latest_df["eventid"] == row["eventid"]) &
                (latest_df["item_id"] == row["item_id"])
            ].reset_index(drop=True)

            prev = previous_df[
                (previous_df["eventid"] == row["eventid"]) &
                (previous_df["item_id"] == row["item_id"])
            ].reset_index(drop=True)

            if not current[compare_columns].equals(prev[compare_columns]):
                rows_to_upsert.append(current)
      
    rows_to_upsert = pd.concat(rows_to_upsert).reset_index(drop=True) if rows_to_upsert else pd.DataFrame(columns=latest_df.columns)

    # Detect deleted rows
    merged_for_deletes = previous_df.merge(
        latest_df,
        on=["eventid", "item_id"],
        how="left",
        indicator=True
    )

    rows_to_delete = merged_for_deletes[merged_for_deletes["_merge"] == "left_only"]
    rows_to_delete = previous_df.merge(
        rows_to_delete[["eventid", "item_id"]],
        on=["eventid", "item_id"]
    ).reset_index(drop=True)

    return {
        "new_df" : latest_df.copy(),
        "upsert_df" : rows_to_upsert, 
        "rows_to_delete" : rows_to_delete,
        "is_initial_run" : False
    }


def transform(data:dict, logger:logging.Logger)->dict:
    try:
        enriched_data = combine_problems_and_triggers(data, logger=logger)
        df = problems_to_dataframe(enriched_data, logger=logger)
        ret = get_changed_and_deleted_zabbix_problems(latest_df=df)
        return ret
    
    except Exception as e:
        logger.error(f"Error occured in Transform Stage: {e}")
        raise RecoverableError(f"{e}")