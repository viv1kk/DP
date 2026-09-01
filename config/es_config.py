from config import config
import json

class ESConfig:
    def __init__(self, file, pipeline_name):
        pipe_conf = config.Config(file).get_pipeline(pipeline_name)

        self.pipeline_name = pipeline_name
        self.url = pipe_conf["index"]["endpoint"]
        self.method = pipe_conf["index"]["method"]
        self.payload = json.loads(pipe_conf["index"]["payload"])
        self.header = json.loads(pipe_conf["index"]["header"])
        self.timeout = pipe_conf["timeout"]
        # Optional: no elastic pipeline declares one today, so the HTTP
        # handler falls back to its default when this is None.
        self.retry_count = pipe_conf.get("retry_count")
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