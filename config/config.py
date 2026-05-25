import yaml
from framework.paths import CONFIG_DIR
from dotenv import load_dotenv
import os

load_dotenv()
class Config:
    def __init__(self, file):
        path = CONFIG_DIR / file
        with open(path) as f:
            self.data = f.read()
        self.data = os.path.expandvars(self.data)
        self.data = yaml.safe_load(self.data)

    def get_pipeline(self, name):
        return self.data["pipelines"].get(name)
    

    def get(self):
        return self.data 