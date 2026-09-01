import functools
import pyodbc
from config.mssql_config import MSSQL_Config
from config.sql_tbl_config import TableConfig
import pandas as pd


class MSSQL_Connector:
    def __init__(self, db: MSSQL_Config):
        self.db = db
        self.conn = None
        self.cursor = None
        # Nesting bookkeeping. Every decorated call is a "frame"; the outermost
        # one owns the connection. Only the outermost enabled=True frame owns
        # the transaction, so an inner call can never commit half of one.
        self._frame_depth = 0
        self._txn_depth = 0

    def __del__(self):
        self._cleanup()

    # -------------------------------
    # Internal Helpers
    # -------------------------------
    def _get_connection(self):
        if self.conn is None:
            self.conn = pyodbc.connect(self.db.connection_string())
            self.conn.autocommit = False

        if self.cursor is None:
            self.cursor = self.conn.cursor()
            self.cursor.fast_executemany = True

        return self.conn, self.cursor

    def _cleanup(self):
        try:
            if self.cursor:
                self.cursor.close()
        except Exception:
            pass

        try:
            if self.conn:
                self.conn.close()
        except Exception:
            pass

        self.cursor = None
        self.conn = None

    # -------------------------------
    # Transaction Decorator
    # -------------------------------
    def with_transaction(enabled=True):
        def decorator(func):

            @functools.wraps(func)
            def wrapper(self, *args, **kwargs):
                conn, cursor = self._get_connection()

                # Decided BEFORE we descend, so nested calls see false.
                owns_connection = self._frame_depth == 0
                owns_transaction = enabled and self._txn_depth == 0

                self._frame_depth += 1
                if enabled:
                    self._txn_depth += 1

                try:
                    result = func(self, cursor, *args, **kwargs)

                    if owns_transaction:
                        conn.commit()

                    return result

                except Exception:
                    # Rollback only from the frame that owns the transaction.
                    # An inner failure must leave the connection open so this
                    # rollback can actually run -- tearing it down here would
                    # leave the outer frame holding a closed handle, and the
                    # rollback below would silently throw instead of running.
                    if owns_transaction:
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                    raise

                finally:
                    self._frame_depth -= 1
                    if enabled:
                        self._txn_depth -= 1

                    # The outermost frame closes the connection, whether or not
                    # it ran a transaction: a bare select_query() would
                    # otherwise hold its connection open until garbage
                    # collection.
                    if owns_connection:
                        self._cleanup()

            return wrapper
        return decorator

    # -------------------------------
    # Query Builders (NO TRANSACTION)
    # -------------------------------
    @with_transaction(enabled=False)
    def build_query(self, cursor, query, *args, **kwargs):
        suppress_error = kwargs.pop("suppress_error", False)
        try:
            cursor.execute(query, *args)
        except Exception as e:
            if suppress_error:
                self.logger.warning(f"[SUPPRESSED] {e}")
                return None
            raise

    @with_transaction(enabled=False)
    def batch_insert(self, cursor, df:pd.DataFrame, table:TableConfig, batch_size=1000):
        cols = ",".join(f"[{col}]" for col in df.columns)
        placeholders = ",".join(["?"] * len(df.columns))
        insert_sql = f"INSERT INTO {table.sql_table_name} ({cols}) VALUES ({placeholders})"

        total_rows = len(df)
        # for i in range(0, total_rows, batch_size):
        #     batch = df.iloc[i:i+batch_size]
        #     # 👇 generator instead of list → LOW MEMORY
        #     data = (tuple(None if pd.isna(v) else v for v in row) for row in batch.itertuples(index=False, name=None))
        #     self.build_many_query(query=insert_sql, data=data)

        for i in range(0, total_rows, batch_size):
            batch = df.iloc[i:i+batch_size]

            def row_generator():
                for idx, row in enumerate(batch.itertuples(index=False, name=None)):
                    cleaned = tuple(None if pd.isna(v) else v for v in row)
                    try:
                        yield cleaned
                    except Exception as e:
                        print(f"Error while yielding row {i + idx}: {cleaned}")
                        raise

            try:
                self.build_many_query(query=insert_sql, data=row_generator())

            except Exception as e:
                print(f"Batch failed at index range {i}–{i+batch_size}")
                
                # 🔍 fallback to row-by-row to pinpoint exact failure
                for idx, row in enumerate(batch.itertuples(index=False, name=None)):
                    cleaned = tuple(None if pd.isna(v) else v for v in row)
                    try:
                        self.build_many_query(query=insert_sql, data=[cleaned])
                    except Exception as inner_e:
                        print(f"❌ Failed row at global index {i + idx}")
                        print(f"Tuple: {cleaned}")
                        print(f"Error: {inner_e}")
                        raise  # or continue if you want to skip bad rows

                raise


    @with_transaction(enabled=True)
    def truncate_and_insert_df(self, cursor, df:pd.DataFrame, table:TableConfig):
        db_cols = self.get_columns(table)        
        cols_to_insert = [col for col in df.columns if col in db_cols]
        df_to_insert = df[cols_to_insert]

        self.truncate_table(table)

        if cols_to_insert:
            self.batch_insert(df=df_to_insert, table=table)

    @with_transaction(enabled=False)
    def truncate_table(self, cursor, table:TableConfig):
        cursor.execute(f"TRUNCATE TABLE {table.sql_table_name}")

    @with_transaction(enabled=False)
    def build_many_query(self, cursor, query, data):
        cursor.fast_executemany = True
        cursor.executemany(query, data)
    
    @with_transaction(enabled=False)
    def build_query_with_data(self, cursor, query, data):
        cursor.execute(query, data)

    @with_transaction(enabled=False)
    def get_columns(self, cursor, table:TableConfig):
        cursor.execute(f"SELECT TOP 0 * FROM {table.sql_table_name}")
        return [col[0] for col in cursor.description]

    @with_transaction(enabled=False)
    def select_query(self, cursor, query) -> pd.DataFrame:
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        
        return pd.DataFrame.from_records(rows, columns=columns)


    # -------------------------------
    # TRANSACTION EXECUTOR (ONLY ONE ENTRY POINT)
    # -------------------------------
    @with_transaction(enabled=True)
    def run_queries(self, cursor, func):
        """
        func MUST accept cursor
        Example:
            def txn(cursor):
                cursor.execute(...)
        """
        return func(cursor)