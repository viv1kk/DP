from config import config
import json

class ZABBIX_CONFIG:
    # Class Variable (shared across all instances)
    auth_token = ""

    def __init__(self, file, pipeline_name):
        pipe_conf = config.Config(file).get_pipeline(pipeline_name)
        self.pipeline_name = pipeline_name

        self.base_url = pipe_conf["api"]["base_url"]
        self.auth_api_method = pipe_conf["auth_api"]["method"]
        self.auth_header = json.loads(pipe_conf["auth_api"]["auth_header"])
        self.auth_payload = json.loads(pipe_conf["auth_api"]["auth_payload"])

        self.api_method = pipe_conf["api"]["method"]
        self.payload = json.loads(pipe_conf["api"]["payload"])
        self.retry_count = pipe_conf["api"]["retry_count"]
        self.timeout = pipe_conf["api"]["timeout"]

        self.username = pipe_conf["api_credentials"]["username"]
        self.password = pipe_conf["api_credentials"]["password"]

        # ✅ Set class variable (NOT instance variable)
        ZABBIX_CONFIG.auth_token = pipe_conf["api_credentials"]["auth_token"]

        # SQL SPECIFIC
        self.sql_table_name = pipe_conf["destination"]["sql_table_name"]
        self.sql_pk = pipe_conf["destination"]["sql_pk"]
        self.sql_pk_constraint_name = pipe_conf["destination"]["sql_pk_constraint_name"]
        self.sql_columnar_index_name = pipe_conf["destination"]["sql_columnar_index_name"]
        self.sql_columns = json.loads(pipe_conf["destination"]["sql_columns"])

        self._fix_auth_payload()
        self._fix_auth_header()

    def _fix_auth_payload(self):
        self.auth_payload["params"]["username"] = self.username
        self.auth_payload["params"]["password"] = self.password

    def _fix_auth_header(self):
        self.auth_header["Authorization"] = f"Bearer {ZABBIX_CONFIG.auth_token}"

    def __repr__(self):
        return f"{self.__class__.__name__}({self.__dict__})"