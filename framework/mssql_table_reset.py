from config.mssql_config import MSSQL_Config
from connectors.mssql_connector import MSSQL_Connector
from config.sql_tbl_config import TableConfig


class MSSQL_TABLE_MANAGER:
    def __init__(self, tbl_config:TableConfig, mssql:MSSQL_Connector = None):
        self.tbl_config = tbl_config
        if not mssql:
            self.mssql = MSSQL_Connector(db=MSSQL_Config())
        else: 
            self.mssql = mssql

    def create_table(self):
        column_defs = ",\n    ".join([f"[{name}] {datatype}" for name, datatype in self.tbl_config.sql_columns.items()])
        pk =  ", ".join([f"[{col}]" for col in self.tbl_config.sql_pk])
        if pk:
            pk = f"CONSTRAINT {self.tbl_config.sql_pk_constraint_name} PRIMARY KEY ({pk})"
        self.mssql.build_query(query=f"""
            CREATE TABLE {self.tbl_config.sql_table_name} (
                {column_defs}
                {pk}
            )
        """)


    def drop_partitioned_table_and_schema(self):
        drop_script = f"""
        IF OBJECT_ID('{self.tbl_config.sql_table_name}', 'U') IS NOT NULL
            DROP TABLE {self.tbl_config.sql_table_name};
        """
        self.mssql.build_query(query=drop_script, suppress_error=True)


    # def drop_partitioned_table_and_schema(self):
    #     drop_script = f"""
    #     IF OBJECT_ID('{self.tbl_config.sql_table_name}', 'U') IS NOT NULL
    #         DROP TABLE {self.tbl_config.sql_table_name};
    #     """
    #     self.mssql.build_query(query=drop_script, suppress_error=True)

    def drop_columnstore_index(self):
        query = f"DROP INDEX [{self.tbl_config.sql_columnar_index_name}] ON [{self.tbl_config.sql_table_name}]"
        try:
            self.mssql.build_query(query=query)
        except Exception as e:
            print(f"❌ Error dropping index: {e}")

    def create_nonclustered_columnstore_index(self):
        cols = ", ".join(f"[{col}]" for col in self.tbl_config.sql_columnar_index_columns)
        query = f"CREATE NONCLUSTERED COLUMNSTORE INDEX [{self.tbl_config.sql_columnar_index_name}] ON [{self.tbl_config.sql_table_name}] ({cols})"
        
        try:
            self.mssql.build_query(query=query)
            # print(f"✅ Created nonclustered columnstore index [{tbl.columnar_index_name}] on [{tbl.table_name}] for columns: {cols}")
        except Exception as e:
            print(f"❌ Error creating index: {e}")


    def recreate(self):
        def transaction(cursor):
            self.drop_partitioned_table_and_schema()
            self.create_table()

        self.mssql.run_queries(transaction)
        

    # recreate()