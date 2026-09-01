from config.hawk_config import HAWK_CONFIG
from framework.logger import get_logger, format_error
from framework.decorators import hawk_http_exception_handler
import requests
from requests.auth import HTTPBasicAuth
from exceptions.RecoverableError import RecoverableError
import re
import pandas as pd

from concurrent.futures import ThreadPoolExecutor, as_completed

class HAWK_CONNECTOR:
    def __init__(self, config:HAWK_CONFIG):
        self.config = config
        self.logger = get_logger(config.pipeline_name)

    
    def _fetch(self, path: str = "/", payload=None, method=None)->dict:
        """
        Entry point for elastic queries
        """
        url = self.config.base_url+path

        if method is None:
            method = self.config.method

        if method == "POST":
            return self._post(url, payload).json()
        elif method == "GET":
            return self._get(url, payload).json()


        self.logger.error("Unsupported HTTP method")
        raise ValueError("Unsupported HTTP method")
    
    @hawk_http_exception_handler()
    def _get(self, url, payload):
        response = requests.get(
            url,
            headers=self.config.header,
            json=payload,
            timeout=self.config.timeout,
            verify=False,
            auth=HTTPBasicAuth(self.config.username, self.config.password),
        )

        response.raise_for_status()
        return response
    
    @hawk_http_exception_handler()
    def _post(self, url, payload):
        response = requests.post(
            url,
            headers=self.config.header,
            json=payload,
            timeout=self.config.timeout,
            verify=False,
            auth=HTTPBasicAuth(self.config.username, self.config.password),
        )

        response.raise_for_status()
        return response

    def get_all_domains(self):
        path = "/Domain"
        try:
            resp = self._fetch(path=path)
            ret = [item.get("name") for item in resp if item.get("name", "") != ""]
            return ret
        except Exception as e:
            raise RecoverableError(f"Error while fetching all domains. {e}")
        

    def _get_hosts_data(self, param):
        path = f"/{param['domain']}/Agent"
        try:
            return self._fetch(path)
        except Exception as e:
            raise RecoverableError(f"Error while fetching host data. {e}")


    def get_all_agents(self, domains):
        data = []
    
        try:
            for domain in domains:
                param = {"domain": domain}
                host_data = self._get_hosts_data(param)
    
                for _, agent_list in host_data.items():
                    for agent in agent_list:
                        data.append({
                            "dns": agent.get("dns"),
                            "domain": agent.get("domain"),
                            "host_name": agent.get("name"),
                            "host_status": agent.get("state")
                        })
            return data
    
        except Exception as e:
            raise RecoverableError(f"Error in _get_all_agents: {e}")

    def _get_microagents_in_host(self, param):
        path = f"/{param["domain"]}/{param["dns"]}/{param["host_name"]}/Microagent"
        try:
            return self._fetch(path=path)
        except Exception as e:
            # return {}
            raise RecoverableError(f"Error: while Fetching the microagents in host: {param} \n {e}")

    def get_all_microagents(self, agents):
        microagents = []
    
        try:
            for agent in agents:
                dns = agent.get("dns")
                domain = agent.get("domain")
                host_name = agent.get("host_name")        # assuming key is "name"
                host_status = agent.get("host_status")     # assuming key is "state"
    
                param = {
                    "domain": domain,
                    "host_name": host_name,
                    "dns": dns,
                    "host_status": host_status
                }
    
                microagent_data = self._get_microagents_in_host(param)
    
                for m_agent in microagent_data:
                    microagent_name = m_agent.get("nameWithInstance")
    
                    if microagent_name:  # avoids empty string or None
                        microagents.append({
                            "dns": dns,
                            "domain": domain,
                            "host_name": host_name,
                            "host_status": host_status,
                            "microagent_name": microagent_name
                        })
    
            return microagents
    
        except Exception as e:
            raise RecoverableError(f"Error in _get_all_microagents: {e}")
            # return []


    def _get_microagent_status(self, param):
        path = f"/{param["domain"]}/{param["dns"]}/{param["host_name"]}/invoke"
        payload = param.get("payload")
        try:
            return self._fetch(path=path, payload=payload, method="POST")
        except Exception as e:
            return {}


        

    # single-threaded
    # def _get_status_for_all_microagents(self, microagents):
    #     data = []
    
    #     try:
    #         for m_agent in microagents:
    
    #             dns, domain, host_name, m_agent_name = m_agent
    #             m_agent_type = self._identify_microagent_type(m_agent_name)
    
    #             if m_agent_type:
    #                 param = {
    #                     "dns": dns,
    #                     "domain": domain,
    #                     "host_name": host_name,
    #                     "microagent_name": m_agent_name,
    #                     "invoke_method": m_agent_type.get("get_status_method")
    #                 }
    
    #                 status = self._get_microagent_status(param)
    
    #                 # Safe extraction of "Status" value
    #                 status_val = next(
    #                     (st.get("value")
    #                      for st in status.get("data", {}).get("dataElements", [])
    #                      if st.get("name") == "Status"),
    #                     None
    #                 )
    
    #                 row = param.copy()
    #                 row["status"] = status_val
    
    #                 data.append(row)
    
    #         return data
    
    #     except Exception as e:
    #         print(e)
    #         return []

    def _identify_invocation_payload(self, param):
        type_map = {
            "bwengine": {
                "type": "BW",
                "payload": {
                    "microagent": "",
                    "method": "GetExecInfo",
                    "param": {}
                }
            }
        }
    
        # --- REGEX PATTERN ---
        PATTERN = re.compile(
            r"^COM\.TIBCO\.ADAPTER\."
            r"(?P<type>[A-Za-z0-9_-]+)\."
            r"(?P<domain>[A-Za-z0-9_-]+)\."
            r"(?P<deployment>[A-Za-z0-9_-]+(?::[0-9]+)?)\."
            r"(?P<component>[A-Za-z0-9_-]+)(?::(?P<number>[0-9]+))?$"
        )
            
        # --- PARSER ---
        def parse_adapter_string(s: str):
            match = PATTERN.match(s)
            return match.groupdict() if match else None
    
        # --- METHOD 1 ---
        def method1():
            name = param.get("microagent_name")
            component = param.get("component")
            deployment = param.get("deployment")
            
            if len(name) == 0 and len(component) == 0 and len(deployment) == 0:
                return None

            if len(name) > 0:        
                match = parse_adapter_string(name)
                if not match:
                    return None
        
                if param['domain'] != match['domain']:
                    return None    
                return {
                    "microagent": "TIBCORuntimeAgent:0",
                    "method": "getComponentInstanceStatus",
                    "param": {
                        "Deployment": match["deployment"],
                        "Component Instance": match["component"]
                            # .split(":")[0]
                    }
                }
            elif len(component) > 0 and len(deployment) > 0: 
                return {
                    "microagent": "TIBCORuntimeAgent:0",
                    "method": "getComponentInstanceStatus",
                    "param": {
                        "Deployment": param["deployment"],
                        "Component Instance": param["component"]
                    }
                }

            return None
    
        # --- METHOD 2 ---
        def method2():
            name = param.get("microagent_name")
            if not name:
                return None
    
            for key, value in type_map.items():
                if re.search(key, name):
                    # IMPORTANT: copy the dict to avoid mutation of global
                    payload = {
                        "microagent": name,
                        "method": value["payload"]["method"],
                        "param": value["payload"]["param"].copy()
                    }
                    return payload
            return None
    
        # --- EXECUTION FLOW ---
        try:
            payload = method1()
            if payload:
                return payload
    
            # payload = method2()
            # if payload:
            #     return payload
    
            return None
    
        except Exception as e:
            self.logger.error(f"_identify_invocation_payload failed: {format_error(e)}")
            return None
        

    def _get_status_for_single_microagent(self, m_agent: dict):
        """Worker function run in parallel for each microagent."""
        try:
            dns = m_agent.get("dns")
            domain = m_agent.get("domain")
            host_name = m_agent.get("host_name")
            host_status = m_agent.get("host_status")
            m_agent_name = m_agent.get("microagent_name","")
            _type = m_agent.get("type")
            deployment = m_agent.get("deployment")
            component = m_agent.get("component")
    
            m_agent_payload = self._identify_invocation_payload({
                "microagent_name": m_agent_name,
                "deployment": deployment,
                "component": component,
                "domain": domain
            })
    
            status_val = ''

            if m_agent_payload:
                # return None

                param = {
                    "dns": dns,
                    "domain": domain,
                    "host_name": host_name,
                    "payload": m_agent_payload
                }
        
                status = self._get_microagent_status(param)

                status_val = next(
                    (
                        st.get("value")
                        for st in status.get("data", {}).get("dataElements", [])
                        if st.get("name") == "State"
                    ),
                    None
                )
    
            row = {
                "dns": dns,
                "domain": domain,
                "host_name": host_name,
                "host_status": host_status,
                "microagent_name": m_agent_name,
                "status": status_val,
                "type": _type,
                "deployment": deployment,
                "component": component,
                "application_tracked" : "Yes" if status_val else "No"
            }
    
            return row
    
        except Exception as e:
            self.logger.error(f"Status lookup failed for microagent {m_agent.get('microagent_name')}: {format_error(e)}")
            return None
    

    def get_status_for_all_microagents(self, microagents: pd.DataFrame, max_workers=100):
        results = []
    
        try:
            records = microagents.to_dict("records")  # 🔥 Convert DF to list of dicts
    
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_map = {
                    executor.submit(self._get_status_for_single_microagent, row): row
                    for row in records
                }
    
                for future in as_completed(future_map):
                    res = future.result()
                    if res:
                        results.append(res)
    
            return results
    
        except Exception as e:
            self.logger.error(f"get_status_for_all_microagents failed: {format_error(e)}")
            return []