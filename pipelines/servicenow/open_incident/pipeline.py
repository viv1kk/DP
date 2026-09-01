import pandas as pd
import json
from config.servicenow_config import SNOW_CONFIG
from framework.paths import DATA_DIR
from config.mssql_config import MSSQL_Config
from config.sql_tbl_config import TableConfig
from connectors.mssql_connector import MSSQL_Connector
from exceptions.RecoverableError import RecoverableError
from framework.base_pipeline import BasePipeline
from framework.decorators import pipeline_run_status
from framework.logger import get_logger
from framework.mssql_table_reset import MSSQL_TABLE_MANAGER
from .extract import extract_data
from .transform import transform
from .load import load_data


class SNOW_OPEN_INCIDENTS(BasePipeline):
    def __init__(self):
        self.pipeline_name = "SNOW_OPEN_INCIDENTS"
        
        self.snow_config = SNOW_CONFIG(file="servicenow_config.yaml", 
                            pipeline_name=self.pipeline_name)
        self.mssql_config = MSSQL_Config()
        self.logger = get_logger(self.pipeline_name)

        self.tbl_config = TableConfig(
            sql_table_name = self.snow_config.sql_table_name,
            sql_pk = self.snow_config.sql_pk,
            sql_pk_constraint_name = self.snow_config.sql_pk_constraint_name,
            sql_columnar_index_name = self.snow_config.sql_columnar_index_name, 
            sql_columns = self.snow_config.sql_columns,
        )


    @pipeline_run_status()
    def etl(self):
        self.data = self.extract()
        self.df = self.transform(data=self.data)
        self.validate(df=self.df)
        self.load(df=self.df)
                

    def run(self):
        try:
            self.etl()
        except RecoverableError as e:
            self.logger.exception(f"Recoverable Error : {e}")
        except Exception as e:
            self.logger.error(f"Unexpected Error : {e}", exc_info=False)


    def extract(self)->dict:
        self.logger.info("Extracting data")

        # def update_fetchsince(self, df):
        #     if not df.empty:
        #         max_timestamp = df['sys_updated_on'].max()
        #         self.fetch_since = max_timestamp

        # if sn.sync_servicenow_db(df, DBConfig()):
            # sn.update_fetchsince(df)
        data = extract_data(snow_config=self.snow_config, logger=self.logger)
        return data

    def transform(self, data:dict)->pd.DataFrame:
        self.logger.info("Transforming data")
        df = transform(data, logger=self.logger)
        return df


    def validate(self, df:pd.DataFrame):
        # validating whether all the columns are present in both the db and the dataframe.
        try:
            self.logger.info("Validating data")
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


    def load(self, df:pd.DataFrame):
        db = self.mssql_config
        table = self.tbl_config
        logger = self.logger

        self.logger.info("Loading data")
        load_data(df=df, db=db, table=table, logger=logger)


    def reset_mssql_table(self):
        tbl_config = self.tbl_config
        # columnar index columns are calculated automatically

        mssql = MSSQL_TABLE_MANAGER(tbl_config=tbl_config)
        mssql.recreate()