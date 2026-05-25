from config.zabbix_config import ZABBIX_CONFIG
from framework.logger import get_logger
from framework.decorators import zabbix_http_exception_handler
import requests
from requests.auth import HTTPBasicAuth
from exceptions.RecoverableError import RecoverableError

class ZABBIX_CONNECTOR:
    def __init__(self, config:ZABBIX_CONFIG):
        self.config = config
        self.logger = get_logger(config.pipeline_name)

        self._ensure_auth()

    def _ensure_auth(self):
        try:
            if ZABBIX_CONFIG.auth_token == "":
                response = self._post(payload=self.config.auth_payload)

                if response.status_code != 200:
                    raise Exception(f"Auth failed with status {response.status_code}: {response.text}")
                
                auth_response = response.json()
                if isinstance(auth_response, list):
                    ZABBIX_CONFIG.auth_token = auth_response[0].get("result")
                    self.config._fix_auth_header()
                    return ZABBIX_CONFIG.auth_token
                
                elif isinstance(auth_response, dict):
                    ZABBIX_CONFIG.auth_token = auth_response.get("result")
                    self.config._fix_auth_header()
                    return ZABBIX_CONFIG.auth_token

                raise Exception(f"Invalid auth response format: {auth_response}")

        except Exception as e:
            raise RecoverableError(f"Error fetching the Auth token: {e}")    
    
    
    def _fetch(self, payload=None, method=None)->dict:
        """
        Entry point for elastic queries
        """

        if payload is None:
            payload = self.config.payload
            method = self.config.method

        if method == "POST":
            return self._post(payload).json()


        self.logger.error("Unsupported HTTP method")
        raise ValueError("Unsupported HTTP method")
    
    @zabbix_http_exception_handler()
    def _post(self, payload):
        response = requests.post(
            self.config.base_url,
            headers=self.config.auth_header,
            json=payload,
            timeout=self.config.timeout,
            verify=False
            # auth=HTTPBasicAuth(self.config.username, self.config.password),
        )

        response.raise_for_status()
        return response
