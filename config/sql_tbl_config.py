class TableConfig:
    def __init__(
        self, 
        sql_table_name:str,
        sql_columns:dict = {},

        sql_pk_constraint_name = "",
        sql_pk = [],

        sql_columnar_index_columns = [],
        sql_columnar_index_name = "",

        sql_partition_function_name = "",
        sql_partition_scheme_name = "",
        sql_partition_column = "",
    ):
        self.sql_table_name = sql_table_name
        self.sql_pk = sql_pk

        self.sql_pk_constraint_name = sql_pk_constraint_name
        self.sql_columns = sql_columns

        self.sql_columnar_index_name = sql_columnar_index_name
        self.sql_columnar_index_columns = [x for x, y in sql_columns.items() if y not in ["NVARCHAR(MAX)"]]

        self.sql_partition_function_name = sql_partition_function_name
        self.sql_partition_scheme_name = sql_partition_scheme_name
        self.sql_partition_column = sql_partition_column

    def __repr__(self):
        return f"{self.__class__.__name__}({self.__dict__})"