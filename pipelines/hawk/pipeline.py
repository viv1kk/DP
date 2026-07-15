import pandas as pd
import logging
import json

from config.hawk_config import HAWK_CONFIG
from config.mssql_config import MSSQL_Config
from config.sql_tbl_config import TableConfig
from connectors.mssql_connector import MSSQL_Connector
from exceptions.ExtractError import ExtractError
from framework.base_pipeline import BasePipeline
from framework.logger import get_logger
from framework.paths import DATA_DIR
from framework.util import load_excel_to_df
from framework.validation import validate_columns
from framework.mssql_table_reset import MSSQL_TABLE_MANAGER
from framework.decorators import pipeline_run_status, singleton_init_once
from exceptions.RecoverableError import RecoverableError
from .extract import extract_data
from .transform import transform
from .load import load_data

@singleton_init_once
class HAWK_APPLICATION_MONITORING(BasePipeline):
    _cache = {}
    def __init__(self):

        self.pipeline_name = "HAWK_APPLICATION_MONITORING"
        self.hawk_config = HAWK_CONFIG(file="hawk_config.yaml", 
                            pipeline_name=self.pipeline_name)
        self.mssql_config = MSSQL_Config()
        self.logger = get_logger(self.pipeline_name)
        self._build_cache()
        
        self.tbls_config = {
            "application_status_table": TableConfig(
                sql_table_name = self.hawk_config.application_status_table['sql_table_name'],
                sql_pk = self.hawk_config.application_status_table['sql_pk'],
                sql_pk_constraint_name = self.hawk_config.application_status_table['sql_pk_constraint_name'],
                sql_columnar_index_name = self.hawk_config.application_status_table['sql_columnar_index_name'], 
                sql_columns = json.loads(self.hawk_config.application_status_table['sql_columns']),
            ),
            "host_status_table": TableConfig(
                sql_table_name = self.hawk_config.host_status_table['sql_table_name'],
                sql_pk = self.hawk_config.host_status_table['sql_pk'],
                sql_pk_constraint_name = self.hawk_config.host_status_table['sql_pk_constraint_name'],
                sql_columnar_index_name = self.hawk_config.host_status_table['sql_columnar_index_name'], 
                sql_columns = json.loads(self.hawk_config.host_status_table['sql_columns']),
            )
        }

    """
    GET THE DEFAULT DATA FROM EXCEL EXTRACT.
    ALSO CHECK DB TABLE IF IT HAVE ANY EXISTING DATA.
    MERGE THEM AND BUILD THE CACHE.
    """
    def _build_cache(self):
        HAWK_APPLICATION_MONITORING._cache = dict()
        path = f"{DATA_DIR}/hawk/TIBCO HAWK APPLICATIONS FORMATTED.xlsx"
        df = load_excel_to_df(path)
        
        # TODO: ADD DB CHECK

        HAWK_APPLICATION_MONITORING._cache["last_microagent_data"] = df

    @pipeline_run_status()
    def etl(self):
        self.data = self.extract()
        self.transform_data = self.transform(data=self.data)
        self.validate(data=self.transform_data)
        self.load(data=self.transform_data)
                
    def run(self):
        x = 1
        while(x > 0):
            try:
                x-=1
                self.etl()
            
            except ExtractError as e:
                self.logger.error(f"Extract Error: {e}")
                
                def error_handling():
                    self.logger.info("Running the staus update for the case of Extract Error.")
                    data = {
                        "raw_df" : pd.DataFrame(),
                        "microagent_status_df" : pd.DataFrame(),
                        "host_status_df" : pd.DataFrame()
                    }
                    self.load(data)
                
                error_handling()
                return 

            except RecoverableError as e:
                self.logger.error(f"Recoverable Error : {e}")
                return
            except Exception as e:
                return
            
            else:
                data = self.transform_data["raw_df"]
                data.drop(columns=["host_status", "status", "last_poll_at_IST", "microagent_name"], inplace=True, errors="ignore")
                data.drop_duplicates(inplace=True)
                HAWK_APPLICATION_MONITORING._cache["last_microagent_data"] = data

    def extract(self)->dict:
        self.logger.info("Extracting data")
        data = extract_data(hawk_config=self.hawk_config, logger=self.logger)
        return data

    def transform(self, data:dict)->dict:
        self.logger.info("Transforming data")
        df = transform(data=data)
        
        return df


    def validate(self, data:dict[str : pd.DataFrame]):
        # validating whether all the columns are present in both the db and the dataframe.
        try:
            self.logger.info("Validating data")
            sql = MSSQL_Connector(db=self.mssql_config)
            
            application_status_table_df_cols = data['microagent_status_df'].columns
            application_status_table_db_cols = sql.get_columns(table=self.tbls_config['application_status_table'])

            # Give me all columns that exist in the DataFrame but do not exist in the SQL table.
            missing = set(application_status_table_db_cols) - set(application_status_table_df_cols)

            if missing:
                raise ValueError(f"Missing required columns: {missing}")
            
            host_status_table_df_cols = data['host_status_df'].columns
            host_status_table_db_cols = sql.get_columns(table=self.tbls_config['host_status_table'])

            # Give me all columns that exist in the DataFrame but do not exist in the SQL table.
            missing = set(host_status_table_db_cols) - set(host_status_table_df_cols)

            if missing:
                raise ValueError(f"Missing required columns: {missing}")
            
        except Exception as e:
            self.logger.error(f"Unexpected Error: {e}" )
            raise RecoverableError(e)


    def load(self, data:dict[str:pd.DataFrame]):
        db = self.mssql_config
        table = self.tbls_config
        logger = self.logger

        self.logger.info("Loading data")

        load_data(data=data, db=db, table=table, logger=logger)
        # table = self.config.get("destination")["table"]
        # self.logger.info("Loading data")
        # load_data(df, self.db, table)


    def reset_mssql_table(self):
        application_status_table_config = self.tbls_config['application_status_table']
        host_status_table_config = self.tbls_config['host_status_table']
        # columnar index columns are calculated automatically

        tbls = [application_status_table_config, host_status_table_config]

        for tbl in tbls:
            mssql = MSSQL_TABLE_MANAGER(tbl_config=tbl)
            mssql.recreate()
