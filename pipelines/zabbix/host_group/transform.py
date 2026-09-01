import re
import numpy as np
import pandas as pd
from framework.decorators import retry
from exceptions.RecoverableError import RecoverableError
import pytz
import logging
from datetime import datetime
import pipelines.zabbix.host_group.pipeline as zb

def transform(data:list, logger:logging.Logger)->dict:
    try:
        last_cached_df = zb.ZABBIX_HOST_GROUP._cache.get("last_cached_df", None)
        df = pd.json_normalize(data)

        is_initial_run = False
        changed_ids = []

        if last_cached_df is None:
            last_cached_df = df
            changed_ids = list(df['groupid'])
            is_initial_run = True
        else:
            df['hosts'] = df['hosts'].astype(int)
            last_cached_df['hosts'] = last_cached_df['hosts'].astype(int)

            last_cached_df.set_index('groupid', inplace=True)
            df.set_index('groupid', inplace=True)

            all_groupids = set(last_cached_df.index).union(set(df.index))

            for gid in all_groupids:
                if gid not in last_cached_df.index:
                    changed_ids.append(gid)
                elif gid not in df.index:
                    changed_ids.append(gid)
                else:
                    old_row = last_cached_df.loc[gid]
                    new_row = df.loc[gid]
                    if not old_row.equals(new_row):
                        changed_ids.append(gid)

            if changed_ids:
                last_cached_df.drop(index=changed_ids, errors='ignore', inplace=True)
                updated_rows = df.loc[changed_ids]
                last_cached_df = pd.concat([last_cached_df, updated_rows], axis=0)

            last_cached_df.reset_index(inplace=True)

        return {
            "new_df" : last_cached_df,
            "upsert_df" : last_cached_df[last_cached_df['groupid'].isin(changed_ids)], 
            "changed_groupids" : changed_ids,
            "is_initial_run" : is_initial_run
        }

    
    except Exception as e:
        raise RecoverableError(f"Transform failed: {e}")