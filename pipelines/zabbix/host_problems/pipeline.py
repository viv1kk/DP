import pandas as pd
import json
from config.zabbix_config import ZABBIX_CONFIG
from framework.paths import DATA_DIR
from config.mssql_config import MSSQL_Config
from config.sql_tbl_config import TableConfig
from connectors.mssql_connector import MSSQL_Connector
from exceptions.RecoverableError import RecoverableError
from framework.base_pipeline import BasePipeline
from framework.decorators import pipeline_run_status, singleton_init_once
from framework.logger import get_logger
from framework.mssql_table_reset import MSSQL_TABLE_MANAGER
from .extract import extract_data
from .transform import transform
from .load import load_data

@singleton_init_once
class ZABBIX_HOST_PROBLEMS(BasePipeline):
    _cache = {}

    def __init__(self):
        self.pipeline_name = "ZABBIX_HOST_PROBLEMS"
        
        self.zabbix_config = ZABBIX_CONFIG(file="zabbix_config.yaml", 
                            pipeline_name=self.pipeline_name)
        self.mssql_config = MSSQL_Config()
        self.logger = get_logger(self.pipeline_name)

        self.tbl_config = TableConfig(
            sql_table_name = self.zabbix_config.sql_table_name,
            sql_pk = self.zabbix_config.sql_pk,
            sql_pk_constraint_name = self.zabbix_config.sql_pk_constraint_name,
            sql_columnar_index_name = self.zabbix_config.sql_columnar_index_name, 
            sql_columns = self.zabbix_config.sql_columns,
        )

    @pipeline_run_status()
    def etl(self):
        try:
            self.data = self.extract()
            self.transform_data = self.transform(data=self.data)
            self.validate(data=self.transform_data)
            self.load(data=self.transform_data)
        except Exception as e:
            raise RecoverableError(f"{e}")
        
        else:
            ZABBIX_HOST_PROBLEMS._cache["last_cached_df"] = self.transform_data["new_df"]
            pass

    def run(self):
        x = 1
        while(x > 0):
            try:
                x-=1
                self.etl()
            except RecoverableError as e:
                self.logger.exception(f"Recoverable Error : {e}")
                continue
            
            except Exception as e:
                break


    def extract(self)->dict:
        self.logger.info("Extracting data")
        data = extract_data(zabbix_config=self.zabbix_config, logger=self.logger)
        return data

    def transform(self, data:dict)->dict:
        self.logger.info("Transforming data")
        transform_data = transform(data=data, logger=self.logger)
        return transform_data


    def validate(self, data:dict):
        # validating whether all the columns are present in both the db and the dataframe.
        try:
            self.logger.info("Validating data")
            df = data["upsert_df"]
            sql = MSSQL_Connector(db=self.mssql_config)
            df_cols = df.columns
            db_cols = sql.get_columns(table=self.tbl_config)

            # Give me all columns that exist in the DataFrame but do not exist in the SQL table.
            missing = set(db_cols) - set(df_cols)

            if missing:
                raise ValueError(f"Missing required columns: {missing}")
            
        except Exception as e:
            self.logger.error(f"Unexpected Error: {e}" )
            raise RecoverableError(e)


    def load(self, data:dict):
        db = self.mssql_config
        table = self.tbl_config
        logger = self.logger

        self.logger.info("Loading data")
        load_data(data=data, db=db, table=table, logger=logger)


    def reset_mssql_table(self):
        tbl_config = self.tbl_config
        # columnar index columns are calculated automatically

        mssql = MSSQL_TABLE_MANAGER(tbl_config=tbl_config)
        mssql.recreate()