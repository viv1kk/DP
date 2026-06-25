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
        self.host_status_table = pipe_conf["host_status_table"]
        self.application_status_table = pipe_conf["application_status_table"]

    def __repr__(self):
        return f"{self.__class__.__name__}({self.__dict__})"
