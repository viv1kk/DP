import pandas as pd
import requests
import urllib3
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

from exceptions.RecoverableError import RecoverableError

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)



# MultiThreaded
def poll_urls(urls: set, max_workers: int = 100) -> dict:

    def is_active(url: str) -> bool:
        try:
            if url is None:
                return True
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

def update_poll_result(df: pd.DataFrame, result_map: dict) -> pd.DataFrame:
    df["Poll Result"] = df["link to Fiori Launchpad"].map(result_map)
    return df


def transform(data:pd.DataFrame)->pd.DataFrame:
    try:
        # Take unique non-null URLs
        url_set = set(data["link to Fiori Launchpad"])
        
        poll_results = poll_urls(url_set)
        df = update_poll_result(data, poll_results)
    
        return df
    
    except Exception as e:
        raise RecoverableError(f"{e}")