import pytest
from connectors.hawk_connector import HAWK_CONNECTOR
from config.hawk_config import HAWK_CONFIG

# def test_fetch():
#     obj = HAWK_CONNECTOR(HAWK_CONFIG(file="hawk_config.yaml", 
#                             pipeline_name="HAWK_APPLICATION_MONITORING"))
    
#     domains = obj.get_all_domains()
    # print(obj.get_hosts_data(param={
    #     "domain" : "XPA_EMEA_QA"
    # }))
    
    # agents = obj.get_all_agents(domains=domains)
    # microagent = obj._get_microagents_in_host(param = agents[0])

    # print(microagent)
    # print(agents)
    # microagents = obj.get_all_microagents(agents)
    # print(microagents)