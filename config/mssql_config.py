from config import config
import json

class MSSQL_Config:
    def __init__(self):
        self.sql_conf_file = "mssql_config.yaml"
        self.sql_conf = config.Config(self.sql_conf_file).get()
        
        self.server = self.sql_conf["mssql"]["server"]
        self.driver = self.sql_conf["mssql"]["driver"]
        self.database =  self.sql_conf["mssql"]["database"]
        self.username = self.sql_conf["mssql"]["username"]
        self.password = self.sql_conf["mssql"]["password"]

    def connection_string(self):
        return f"DRIVER={self.driver};SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password}"


    def __repr__(self):
        return f"{self.__class__.__name__}({self.__dict__})"