from config.servicenow_config import SNOW_CONFIG
from framework.decorators import snow_http_exception_handler
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
import copy
from exceptions.RecoverableError import RecoverableError


class SNOW_CONNECTOR:
    def __init__(self, config:SNOW_CONFIG):
        self.config = config
        self.logger = get_logger(config.pipeline_name)
        self.fetch_since = None

    def _fetch(self, payload=None, method = None)->dict:
        """
        Entry point for elastic queries
        """

        if payload is None:
            payload = self.config.payload
            method = self.config.method
            

        if method == "POST":
            return self._post(payload)

        if method == "GET":
            return self._get(payload)

        self.logger.error("Unsupported HTTP method")
        raise ValueError("Unsupported HTTP method")
    
    @snow_http_exception_handler()
    def _post(self, payload):
        response = requests.post(
            self.config.url,
            headers=self.config.header,
            json=payload,
            auth=HTTPBasicAuth(self.config.username, self.config.password),
            # timeout=self.config.timeout
        )

        response.raise_for_status()
        return response.json()

    @snow_http_exception_handler()
    def _get(self, payload):
        response = requests.get(
            self.config.base_url,
            headers=self.config.header,
            params=payload,
            auth=HTTPBasicAuth(self.config.username, self.config.password),
            # timeout=self.config.timeout
        )
        response.raise_for_status()
        return response.json()
    
    def _parse_payload(self):
        params = {}
        payload = copy.deepcopy(self.config.payload)

        for pair in payload["url_param"].split("&"):
            if "=" in pair:
                key, value = pair.split("=", 1)
                params[key] = value
        return params

    def _prepare_request(self, since=None, fields=None):
        """
        Fetch incidents from ServiceNow, optionally filtered by sys_updated_on.
        Returns a list of incidents.
        """
        params = self._parse_payload()
        params["sysparm_limit"] = str(self.config.page_limit)
        params["sysparm_display_value"] = "true"

        if fields: 
            params["sysparm_fields"] = ",".join(fields)
        else: 
            params["sysparm_fields"] = ",".join(self.config.payload["columns"])

        query = ""
        if self.fetch_since:
            # Add sys_updated_on filter
            query = f"sys_updated_on>{self.fetch_since}"
            parm = params.get("sysparm_query", "")
            if parm:
                query += f"^{parm}"

        elif since:
            query = f"sys_updated_on>{since}"
            parm = params.get("sysparm_query", "")
            if parm:
                query += f"^{parm}"
        else:
            query = params.get("sysparm_query", "")
            
        query += "^ORDERBYsys_updated_on"
        params["sysparm_query"] = query

        return params

    def fetch_incidents(self, since=None, fields=None):
        params = self._prepare_request(since, fields)
        
        page_limit = int(self.config.page_limit)
        all_results = []
        offset = 0
        previous_first_row = None
        while True:
            params["sysparm_offset"] = str(offset)
            try:
                data = self._fetch(payload=params, method=self.config.method)
                if not data:
                    break

                # Page size is the length of the "result" list, not of the
                # response object: len(data) counts top-level JSON keys (1),
                # which is always < page_limit and ended the loop after the
                # first request, silently capping the fetch at one page.
                page = data.get("result", [])
                if not page:
                    break

                # A server that ignores sysparm_offset returns the same full
                # page forever. Detect that exact signature rather than capping
                # the row count, so a genuinely large result set is never
                # truncated. Checked before accumulating, so a repeated page is
                # not counted twice; compares the whole first row, so it holds
                # regardless of which sysparm_fields the caller asked for.
                if page[0] == previous_first_row:
                    self.logger.error(
                        f"ServiceNow returned the same page twice at offset {offset}; "
                        f"sysparm_offset appears to be ignored. Stopping with "
                        f"{len(all_results)} incidents."
                    )
                    break

                all_results.extend(page)

                if len(page) < page_limit:
                    break  # short page -> that was the last one

                previous_first_row = page[0]
                offset += page_limit

            except Exception as e:
                self.logger.error(f"Error fetching incidents: {e}")
                raise RecoverableError(f"Error fetching incidents: {e}")

        self.logger.info(f"Total incidents fetched: {len(all_results)}")
        return all_results

    def __repr__(self):
        return f"{self.config}"