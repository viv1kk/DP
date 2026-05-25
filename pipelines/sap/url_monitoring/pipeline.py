import pandas as pd
import json
from framework.paths import DATA_DIR
from config.mssql_config import MSSQL_Config
from config.sap_config import SAP_URL_MONITORING_CONFIG
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


class SAP_URL_MONITORING(BasePipeline):
    def __init__(self):
        self.pipeline_name = "SAP_URL_MONITORING"
        
        self.sap_config = SAP_URL_MONITORING_CONFIG(file="sap_config.yaml", 
                            pipeline_name=self.pipeline_name)
        self.mssql_config = MSSQL_Config()
        self.logger = get_logger(self.pipeline_name)

        self.tbls_config = {
            "base_table": TableConfig(
                sql_table_name = self.sap_config.base_table['sql_table_name'],
                sql_pk = self.sap_config.base_table['sql_pk'],
                sql_pk_constraint_name = self.sap_config.base_table['sql_pk_constraint_name'],
                sql_columnar_index_name = self.sap_config.base_table['sql_columnar_index_name'], 
                sql_columns = json.loads(self.sap_config.base_table['sql_columns']),
            ),
            "poll_table": TableConfig(
                sql_table_name = self.sap_config.poll_table['sql_table_name'],
                sql_pk = self.sap_config.poll_table['sql_pk'],
                sql_pk_constraint_name = self.sap_config.poll_table['sql_pk_constraint_name'],
                sql_columnar_index_name = self.sap_config.poll_table['sql_columnar_index_name'], 
                sql_columns = json.loads(self.sap_config.poll_table['sql_columns']),
            )
        }
        self.tbl_config = self.tbls_config["poll_table"]



    @pipeline_run_status()
    def etl(self):
        self.data = self.extract()
        self.df = self.transform(data=self.data)
        self.validate(df=self.df)
        self.load(df=self.df)
                

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
        base_table = self.tbls_config["base_table"]
        data = extract_data(base_table=base_table, db=self.mssql_config, logger=self.logger)
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
            db_cols = sql.get_columns(table=self.tbls_config["poll_table"])

            # Give me all columns that exist in the DataFrame but do not exist in the SQL table.
            missing = set(db_cols) - set(df_cols)

            if missing:
                raise ValueError(f"Missing required columns: {missing}")
            
        except Exception as e:
            self.logger.error(f"Unexpected Error: {e}" )
            raise RecoverableError(e)


    def load(self, df:pd.DataFrame):
        db = self.mssql_config
        table = self.tbls_config["poll_table"]
        logger = self.logger

        self.logger.info("Loading data")
        load_data(df=df, db=db, table=table, logger=logger)


    def reset_mssql_table(self):
        tbl_config = self.tbls_config["poll_table"]
        # columnar index columns are calculated automatically

        mssql = MSSQL_TABLE_MANAGER(tbl_config=tbl_config)
        mssql.recreate()

    def reset_base_mssql_table(self):
        tbl_config = self.tbls_config["base_table"]
        mssql = MSSQL_TABLE_MANAGER(tbl_config=tbl_config)
        mssql.recreate()

        try:
            mssql = MSSQL_Connector(self.mssql_config)

            def transaction(cursor):
                db_cols = mssql.get_columns(tbl_config)
                
                dff = pd.read_csv(f"{DATA_DIR}/sap/{self.sap_config.url_file_name}", encoding=self.sap_config.url_file_encoding)
                dff.drop(columns=["Instance Number"], inplace=True)

                cols_to_insert = [col for col in dff.columns if col in db_cols]
                df_to_insert = dff[cols_to_insert]
                
                if cols_to_insert:
                    mssql.batch_insert(df=df_to_insert, table=tbl_config)
            
            mssql.run_queries(transaction)
        
        except Exception as e:
            self.logger.warning(f"Recoverable error: {e}")
            raise RecoverableError(f"Recoverable Error: load_data -> {e}")