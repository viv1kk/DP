import requests
import pandas as pd
from config.sap_config import SAPConfig
from framework.decorators import retry
from connectors.sap_connector import SAP_SOLMAN_CONNECTOR
from exceptions.RecoverableError import RecoverableError
import copy
import logging

def create_context_dataframe(system_data):
    try:
        records = [{
            'Contextid': item.get('Contextid', ''),
            'RootComponentName': item.get('Name', ''),
            'Components URI': item.get('Components', {}).get('__deferred', {}).get('uri', '')
        } for item in system_data]
        df = pd.DataFrame(records)
        return df.dropna(subset=['Contextid']).query("Contextid != ''")
    except Exception as e:
        raise RecoverableError(f"Error: {e}")

def extract_data(sap_config: SAPConfig, logger: logging.Logger):
    try:
        sap = SAP_SOLMAN_CONNECTOR(sap_config)
        system_data = sap.fetch_system_data()
        context_df = create_context_dataframe(system_data).drop_duplicates(subset=['Contextid'], keep='last')
        mo_df = sap.fetch_components_data_parallel(context_df)
        all_events = sap.fetch_events_parallel(mo_df)

        return all_events
    except RecoverableError as e:
        logger.warning(f"Recoverable error: {e}")
        raise

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise
