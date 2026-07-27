import pandas as pd

from config.es_config import ESConfig
from config.mssql_config import MSSQL_Config
from config.sql_tbl_config import TableConfig
from connectors.mssql_connector import MSSQL_Connector
from framework.base_pipeline import BasePipeline
from framework.decorators import pipeline_run_status
from framework.logger import get_logger
from framework.validation import validate_columns
from framework.mssql_table_reset import MSSQL_TABLE_MANAGER
from exceptions.RecoverableError import RecoverableError

from .extract import extract_data
from .transform import transform
from .load import load_data


class TIBCO_HAWK(BasePipeline):
    def __init__(self):
        self.pipeline_name = "TIBCO_HAWK"
        
        self.es_config = ESConfig(file="tibco_elastic_config.yaml", 
                            pipeline_name=self.pipeline_name)
        self.mssql_config = MSSQL_Config()
        self.logger = get_logger(self.pipeline_name)

        self.last_search_after = None

        self.tbl_config = TableConfig(
            sql_table_name = self.es_config.sql_table_name,
            sql_pk = self.es_config.sql_pk,
            sql_pk_constraint_name = self.es_config.sql_pk_constraint_name,
            sql_columnar_index_name = self.es_config.sql_columnar_index_name, 
            sql_columns = self.es_config.sql_columns,
        )

    @pipeline_run_status()
    def etl(self):
        self.data = self.extract()
        self.df = self.transform(data=self.data)
        self.validate(df=self.df)

        self.load(df=self.df, db=self.mssql_config, table=self.tbl_config)

    def run(self):
        x = 1
        while(x > 0):
            try:
                self.etl()
                
            except RecoverableError as e:
                x-=1
                self.logger.exception(f"Recoverable Error : {e}")
                continue
            
            except Exception as e:
                break

            else:
                break


    def extract(self)->dict:
        self.logger.info("Extracting data")
        data = extract_data(es_config=self.es_config, search_after=self.last_search_after, logger=self.logger)
        return data

    def transform(self, data:dict)->pd.DataFrame:
        self.logger.info("Transforming data")
        df = transform(data)
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

    def load(self, df:pd.DataFrame, db:MSSQL_Config, table:TableConfig):
        self.logger.info("Loading data")
        load_data(df=df, db=db, table=table, logger=self.logger)


    def reset_mssql_table(self):
        tbl_config = TableConfig(
            sql_table_name = self.es_config.sql_table_name,
            sql_pk = self.es_config.sql_pk,
            sql_pk_constraint_name = self.es_config.sql_pk_constraint_name,
            sql_columnar_index_name = self.es_config.sql_columnar_index_name, 
            sql_columns = self.es_config.sql_columns,
        )
        # columnar index columns are calculated automatically

        mssql = MSSQL_TABLE_MANAGER(tbl_config=tbl_config)
        mssql.recreate()