from config import config
import json

class HAWK_CONFIG:
    def __init__(self, file, pipeline_name):
        pipe_conf = config.Config(file).get_pipeline(pipeline_name)
        self.pipeline_name = pipeline_name

        self.base_url = pipe_conf["api"]["base_url"]
        self.method = pipe_conf["api"]["method"]
        self.header = json.loads(pipe_conf["api"]["header"])
        self.timeout = pipe_conf["api"]["timeout"]
        self.retry_count = pipe_conf["api"]["retry_count"]
        self.max_workers = pipe_conf["api"]["max_workers"]

        self.username = pipe_conf["api_credentials"]["username"]
        self.password = pipe_conf["api_credentials"]["password"]

        # SQL SPECIFIC
        self.sql_table_name = pipe_conf["destination"]["sql_table_name"]
        self.sql_pk = pipe_conf["destination"]["sql_pk"]
        self.sql_pk_constraint_name = pipe_conf["destination"]["sql_pk_constraint_name"]
        self.sql_columnar_index_name = pipe_conf["destination"]["sql_columnar_index_name"]
        self.sql_columns = json.loads(pipe_conf["destination"]["sql_columns"])

    def __repr__(self):
        return f"{self.__class__.__name__}({self.__dict__})"
