import pandas as pd
import requests
import urllib3
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

from exceptions.RecoverableError import RecoverableError
from framework.logger import get_logger
from framework.util import clean_url, URL_PLACEHOLDERS

logger = get_logger("SAP_URL_MONITORING")

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)



# MultiThreaded
def poll_urls(urls: set, max_workers: int = 100) -> dict:

    def is_active(url: str) -> bool:
        try:
            res = requests.get(url, timeout=5, verify=False)
            return 100 <= res.status_code < 500
        except Exception:
            return False

    results = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(is_active, url): url for url in urls}

        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                results[url] = future.result()
            except Exception:
                results[url] = False

    return results



# Single Threaded

# def poll_urls(urls: set) -> dict:
#     def is_active(url: str) -> str:
#         try:
#             if url is None:
#                 return True
#             res = requests.get(url, timeout=5, verify=False)
#             return True if 100 <= res.status_code < 500 else False
#         except Exception as e:
#             return False

#     return {url: is_active(url) for url in urls}

def update_poll_result(df: pd.DataFrame, urls: pd.Series, result_map: dict) -> pd.DataFrame:
    # Rows without a URL are not in result_map, so they stay NULL (unknown) instead of False (down).
    df["Poll Result"] = urls.map(result_map).astype(object).where(urls.notna(), None)
    return df


def transform(data:pd.DataFrame)->pd.DataFrame:
    try:
        raw_urls = data["link to Fiori Launchpad"]
        urls = raw_urls.map(clean_url)

        # Anything that is filled in but is not an http(s) URL is treated as "no URL" -- flag it so it can be fixed.
        filled = raw_urls.map(lambda v: pd.notna(v) and str(v).strip().upper() not in URL_PLACEHOLDERS)
        invalid = raw_urls[filled & urls.isna()]
        if not invalid.empty:
            logger.warning(f"Not polling {len(invalid)} non-http(s) URL value(s), Poll Result set to NULL: {sorted(set(invalid.astype(str)))[:10]}")

        # Take unique non-null URLs
        poll_results = poll_urls(set(urls.dropna()))
        df = update_poll_result(data, urls, poll_results)
    
        return df
    
    except Exception as e:
        raise RecoverableError(f"{e}")