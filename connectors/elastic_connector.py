import requests
from config.es_config import ESConfig
from framework.decorators import elastic_http_exception_handler
from framework.logger import get_logger
from requests.auth import HTTPBasicAuth

class ELASTIC_CONNECTOR:
    def __init__(self, config: ESConfig):
        self.config = config
        self.logger = get_logger("TIBCO_AUDIT_OPERATIONAL_ERRORS")

    def fetch(self, payload=None, method = None)->dict:
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


    @elastic_http_exception_handler()
    def _post(self, payload):
        response = requests.post(
            self.config.url,
            headers=self.config.header,
            json=payload,
            auth=HTTPBasicAuth(self.config.username, self.config.password),
            timeout=self.config.timeout
        )

        response.raise_for_status()
        return response.json()


    @elastic_http_exception_handler()
    def _get(self, payload):
        response = requests.get(
            self.config.url,
            header=self.config.header,
            params=payload,
            auth=HTTPBasicAuth(self.config.username, self.config.password),
            timeout=self.config.timeout
        )
        response.raise_for_status()
        return response.json()


    def _fallback(self, payload):
        """
        Dynamic fallback if elastic is down or some unexpected error
        """

        return {
            "hits": {"hits": []},
            "payload": payload,
            "error": "elastic_unavailable"
        }


    def __repr__(self):
        return self.config