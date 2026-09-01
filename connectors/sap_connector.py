from config.sap_config import SAPConfig
from framework.logger import get_logger
import requests
import pandas as pd
import numpy as np
from requests.auth import HTTPBasicAuth
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from exceptions.RecoverableError import RecoverableError


class SAP_SOLMAN_CONNECTOR:
    def __init__(self, config:SAPConfig):
       self.config = config
       self.logger = get_logger(config.pipeline_name)
       self.session = self.create_session()

    def create_session(self):
        try:
            session = requests.Session()
            adapter = HTTPAdapter(pool_connections=self.config.max_workers, pool_maxsize=self.config.max_workers)
            session.mount('https://', adapter)
            session.mount('http://', adapter)
            session.auth = HTTPBasicAuth(self.config.username, self.config.password)
            session.headers.update(self.config.header)
            response = session.get(self.config.base_url, verify=False, timeout=self.config.timeout)
            response.raise_for_status()
            csrf_token = response.headers.get("x-csrf-token")
            if not csrf_token:
                raise Exception("CSRF token not found in headers.")
            session.headers.update({"X-CSRF-Token": csrf_token})
            return session
        
        except requests.RequestException as e:
            raise RecoverableError(f"Session initialization failed: {e}") 
        
    
    def get_api_data(self, url):
        for attempt in range(self.config.retry_count):
            try:
                response = self.session.get(url, headers={"Accept": "application/json"}, verify=False, timeout=self.config.timeout)
                response.raise_for_status()
                return response.json()
            except requests.RequestException as e:
                self.logger.warning(f"Attempt {attempt + 1} failed for URL: {url} | Error: {e}")
                time.sleep(2)
        self.logger.error(f"All retries failed for URL: {url}")
        raise RecoverableError(f"All retries failed for URL: {url}")  


    def fetch_system_data(self):
        try:
            url = f"{self.config.base_url}SystemListSet"
            data = self.get_api_data(url)
            return data.get('d', {}).get('results', [])
        except Exception as e:
            raise RecoverableError(f"Failed to fetch system data: {e}") 
        
    
    def fetch_components_data(self, uri):
        data = self.get_api_data(uri)
        return data.get('d', {}).get('results', [])

    def fetch_components_data_parallel(self, context_df):
        records = []

        def worker(row):
            uri, ctx_id, root_name = row['Components URI'], row['Contextid'], row['RootComponentName']
            components = self.fetch_components_data(uri)
            return [{
                'MoContextid': c.get('MoContextid', ''),
                'MoName': c.get('MoName', ''),
                'RootComponentid': ctx_id,
                'RootComponentName': root_name,
                'Availstatus': c.get('Availstatus', 0),
                'Perfstatus': c.get('Perfstatus', 0),
                'Excpstatus': c.get('Excpstatus', 0),
                'Confstatus': c.get('Confstatus', 0),
                'Selfmonstatus': c.get('Selfmonstatus', 0),
                'NoOfAlerts': c.get('NoOfAlerts', 0),
            } for c in components]

        with ThreadPoolExecutor(self.config.max_workers) as executor:
            futures = [executor.submit(worker, row) for _, row in context_df.iterrows()]
            for future in as_completed(futures):
                try:
                    records.extend(future.result())
                except Exception as e:
                    raise RecoverableError(f"Component fetch error: {e}")
        df = pd.DataFrame(records)
        return df.dropna(subset=['MoContextid']).query("MoContextid != ''")


    def fetch_event_data(self, mo_context_id, root_component_id, component_meta):
        try:
            url = f"{self.config.base_url}EventListSet?{self.config.sap_client}&search={mo_context_id}&$filter=MoType eq 'HOST_MONITORING'"
            data = self.get_api_data(url)
            results = data.get('d', {}).get('results', [])
            return [{**{k: v for k, v in item.items() if k != '__metadata'},
                    'RootComponentid': root_component_id,
                    'RootComponentName': component_meta.get('RootComponentName', '')} | component_meta for item in results]
        except Exception as e:
            self.logger.error(f"Fetch event error for {mo_context_id}: {e}")
            return []

    def fetch_events_parallel(self, mo_df):
        all_events = []

        def worker(row):
            meta = {k: row[k] for k in ['RootComponentName', 'Availstatus', 'Perfstatus', 'Excpstatus', 'Confstatus', 'Selfmonstatus', 'NoOfAlerts']}
            return self.fetch_event_data(row['MoContextid'], row['RootComponentid'], meta)

        with ThreadPoolExecutor(self.config.max_workers) as executor:
            futures = [executor.submit(worker, row) for _, row in mo_df.iterrows()]
            for future in as_completed(futures):
                try:
                    all_events.extend(future.result())
                except Exception as e:
                    self.logger.error(f"Event fetch thread error: {e}")
        return all_events

    def __repr__(self):
        return f"{self.config}"