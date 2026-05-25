from connectors.servicenow_connector import SNOW_CONNECTOR
from config.servicenow_config import SNOW_CONFIG

# def test_print_method_runs():
#     obj = SNOW_CONNECTOR(SNOW_CONFIG(file="servicenow_config.yaml", 
#                             pipeline_name="SNOW_OPEN_INCIDENTS"))
#     print(obj)

# def test_fetch_method():
#     obj = SNOW_CONNECTOR(SNOW_CONFIG(file="servicenow_config.yaml", 
#                             pipeline_name="SNOW_OPEN_INCIDENTS"))
#     data = obj.fetch_incidents(since='2026-04-27')
#     print(data, len(data))