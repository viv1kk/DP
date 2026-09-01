import pandas as pd
from exceptions.RecoverableError import RecoverableError
import numpy as np
import time
import pipelines.hawk.pipeline as hawk
from framework.logger import get_logger

logger = get_logger("HAWK_APPLICATION_MONITORING")

# Keys the microagent list and the polled status are joined on.
MERGE_KEYS = ["dns", "domain", "host_name", "component", "deployment"]

# Columns the microagent list must carry for the join to mean anything.
# merge_microagent_with_cache() substitutes the cached inventory when the API
# returns nothing usable, and that cache has had microagent_name/host_status
# stripped from it -- so this check is what detects "the end systems could not
# be reached" rather than letting the merge fall over downstream.
REQUIRED_MICROAGENT_COLS = MERGE_KEYS + ["microagent_name", "host_status"]

HOST_STATUS_FIELDS = [
    "dns", "domain", "host_name", "host_status", "host_tracked", "last_poll_at_IST",
]

MICROAGENT_STATUS_FIELDS = [
    "dns", "domain", "host_name", "type", "deployment", "component",
    "microagent_name", "status", "application_tracked", "last_poll_at_IST",
    "prev_status",
]


def _unreachable(reason):
    """The end systems could not be polled.

    An empty frame is a real result here, not a failure: load() turns it into
    'mark every tracked host and application UNKNOWN'. Returned deliberately and
    logged, so this state is never reached by an exception being swallowed.
    """
    logger.warning(f"No usable microagent status ({reason}); treating end systems as unreachable.")
    return pd.DataFrame()


def _add_status_to_microagents(microagent_list, m_status):
    microagent_df = microagent_list
    status_df = pd.DataFrame(m_status)
    status_df.drop(["host_status", "type"], axis=1, errors="ignore", inplace=True)

    if microagent_df is None or microagent_df.empty:
        return _unreachable("no known microagents")

    if status_df.empty:
        return _unreachable("status poll returned no rows")

    missing = [c for c in REQUIRED_MICROAGENT_COLS if c not in microagent_df.columns]
    if missing:
        return _unreachable(f"microagent list is missing {missing}")

    missing = [c for c in MERGE_KEYS + ["status"] if c not in status_df.columns]
    if missing:
        return _unreachable(f"status rows are missing {missing}")

    df = microagent_df.merge(
        status_df,
        on=MERGE_KEYS,
        how="left",
        suffixes=("", "_status"),
    )

    # Only present when microagent_name collided across the merge.
    df = df.drop(columns=["microagent_name_status"], errors="ignore")

    df["status"] = df["status"].fillna("").replace("", "STOPPED_INFERRED")

    # Adding timstamp to keep track of the poll time.
    now_ist = int(time.time())
    df["last_poll_at_IST"] = now_ist

    def to_tz(series, zone):
        return (
            pd.to_datetime(series, unit="s", utc=True)
            .dt.tz_convert(zone)
            .dt.tz_localize(None)
        )

    df["last_poll_at_IST"] = to_tz(df["last_poll_at_IST"], "Asia/Kolkata")
    df = df.fillna('')
    df.drop_duplicates(inplace=True)
    return df


def create_host_status_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=HOST_STATUS_FIELDS)

    app_cache_df = hawk.HAWK_APPLICATION_MONITORING._cache["last_microagent_data"].copy()

    host_df = df[[f for f in HOST_STATUS_FIELDS if f != "host_tracked"]]

    host_df_latest = (
        host_df
        .sort_values("last_poll_at_IST")
        .drop_duplicates(
            subset=["dns", "domain", "host_name"],
            keep="last"
        )
        .reset_index(drop=True)
    )

    host_df_latest["host_tracked"] = np.where(
        host_df_latest["host_status"] != "UNKNOWN",
        "Yes",
        "No"
    )

    app_cache_df_host = (
        app_cache_df[["dns", "domain", "host_name"]]
        .drop_duplicates(
            subset=["dns", "domain", "host_name"],
            keep="last"
        )
        .reset_index(drop=True)
    )

    host_df = host_df_latest.merge(
        app_cache_df_host,
        on=["dns", "domain", "host_name"],
        how="left",
        suffixes=("", "_status")
    )

    return host_df


def create_microagent_status_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=MICROAGENT_STATUS_FIELDS)

    df["prev_status"] = ""
    m_df = df[MICROAGENT_STATUS_FIELDS]
    m_df = m_df.drop_duplicates()
    return m_df


def transform(data: dict) -> dict:
    try:
        microagent_list = data["microagent_list"]
        m_status = data["microagent_status"]

        df = _add_status_to_microagents(microagent_list=microagent_list, m_status=m_status)

        host_status_df = create_host_status_df(df.copy())
        microagent_status_df = create_microagent_status_df(df.copy())

        return {
            "raw_df": df,
            "microagent_status_df": microagent_status_df,
            "host_status_df": host_status_df
        }

    except Exception as e:
        # A failure here is a defect in this code, not an outage. Raising keeps
        # load() from running, so a bug can never repaint the estate UNKNOWN.
        logger.error(f"Transform failed: {type(e).__name__} - {e}")
        raise RecoverableError(f"Error : {e}")
