import time
import functools

def retry(attempts=3, delay=3):

    def decorator(func):

        @functools.wraps(func)
        def wrapper(*args, **kwargs):

            for i in range(attempts):

                try:
                    return func(*args, **kwargs)

                except Exception:

                    if i == attempts - 1:
                        raise

                    time.sleep(delay)

        return wrapper

    return decorator



import functools
import pyodbc
from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd
from config.mssql_config import MSSQL_Config
from config.sql_tbl_config import TableConfig
from exceptions.RecoverableError import RecoverableError


def etl_run_status(control_table="BACC_ETL_JOB_CONTROL"):

    def decorator(func):

        @functools.wraps(func)
        def wrapper(self, df:pd.DataFrame, db:MSSQL_Config, table:TableConfig, *args, **kwargs):

            ist_now = datetime.now(ZoneInfo("Asia/Kolkata"))

            # --------------------------------------------------
            # 1️⃣ Update LAST ATTEMPT (IST)
            # --------------------------------------------------
            try:
                job_name = table.sql_table_name

                with pyodbc.connect(db.connection_string(), autocommit=True) as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(f"""
                            MERGE {control_table} AS target
                            USING (SELECT ? AS job_name) AS source
                            ON target.job_name = source.job_name
                            WHEN MATCHED THEN
                                UPDATE SET last_attempt_time = ?
                            WHEN NOT MATCHED THEN
                                INSERT (job_name, last_attempt_time)
                                VALUES (?, ?);
                        """, job_name, ist_now, job_name, ist_now)
            except Exception as e:
                raise RecoverableError(f"Error while updating the last attempt status -> {e}")

            error_msg = None

            try:
                # 🔹 Call function independently
                result = func(self, df, db, table, *args, **kwargs)

                ist_success_time = datetime.now(ZoneInfo("Asia/Kolkata"))

                # --------------------------------------------------
                # 2️⃣ Success update
                # --------------------------------------------------
                try:
                    with pyodbc.connect(db.connection_string(), autocommit=True) as conn:
                        with conn.cursor() as cursor:
                            cursor.execute(f"""
                                UPDATE {control_table}
                                SET last_success_time = ?,
                                    last_status = ?,
                                    last_message = ?
                                WHERE job_name = ?
                            """, ist_success_time,
                                 "SUCCESS",
                                 "Completed successfully",
                                 job_name)
                except Exception as e:
                    raise RecoverableError(f"Error while updating the Success attempt status -> {e}")

                return result

            except Exception as e:
                error_msg = str(e)

                # --------------------------------------------------
                # 3️⃣ Failure update (IST)
                # --------------------------------------------------
                try:
                    with pyodbc.connect(db.connection_string(), autocommit=True) as conn:
                        with conn.cursor() as cursor:
                            cursor.execute(f"""
                                UPDATE {control_table}
                                SET last_status = ?,
                                    last_message = ?
                                WHERE job_name = ?
                            """, "FAILED",
                                 error_msg[:1000],
                                 job_name)
                except Exception as e:
                    raise RecoverableError(f"Error while updating the Failure update attempt status -> {e}")

                return False

        return wrapper
    return decorator


def pipeline_run_status(control_table="BACC_ETL_JOB_CONTROL"):

    def decorator(func):

        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):

            ist_now = datetime.now(ZoneInfo("Asia/Kolkata"))

            # TIGHT COUPLING : TO BE REMOVED
            job_name = None 
            db = None
            try:
                job_name = self.tbl_config.sql_table_name
                db = self.mssql_config
            # --------------------------------------------------
            # 1️⃣ Update LAST ATTEMPT (IST)
            # --------------------------------------------------
                with pyodbc.connect(db.connection_string(), autocommit=True) as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(f"""
                            MERGE {control_table} AS target
                            USING (SELECT ? AS job_name) AS source
                            ON target.job_name = source.job_name
                            WHEN MATCHED THEN
                                UPDATE SET last_attempt_time = ?
                            WHEN NOT MATCHED THEN
                                INSERT (job_name, last_attempt_time)
                                VALUES (?, ?);
                        """, job_name, ist_now, job_name, ist_now)
            except Exception as e:
                raise RecoverableError(f"Error while updating the last attempt status -> {e}")

            error_msg = None

            try:
                # 🔹 Call function independently
                func(self, *args, **kwargs)

                ist_success_time = datetime.now(ZoneInfo("Asia/Kolkata"))

                # --------------------------------------------------
                # 2️⃣ Success update
                # --------------------------------------------------
                try:
                    with pyodbc.connect(db.connection_string(), autocommit=True) as conn:
                        with conn.cursor() as cursor:
                            cursor.execute(f"""
                                UPDATE {control_table}
                                SET last_success_time = ?,
                                    last_status = ?,
                                    last_message = ?
                                WHERE job_name = ?
                            """, ist_success_time,
                                 "SUCCESS",
                                 "Completed successfully",
                                 job_name)
                except Exception as e:
                    raise RecoverableError(f"Error while updating the Success attempt status -> {e}")

            except Exception as e:
                error_msg = str(e)
                # --------------------------------------------------
                # 3️⃣ Failure update (IST)
                # --------------------------------------------------
                try:
                    with pyodbc.connect(db.connection_string(), autocommit=True) as conn:
                        with conn.cursor() as cursor:
                            cursor.execute(f"""
                                UPDATE {control_table}
                                SET last_status = ?,
                                    last_message = ?
                                WHERE job_name = ?
                            """, "FAILED",
                                 error_msg[:1000],
                                 job_name)
                except Exception as e:
                    raise RecoverableError(f"Error while updating the Failure update attempt status -> {e}")
                
                raise RecoverableError(f"Error -> {e}")

        return wrapper
    return decorator


import requests
import time
from functools import wraps

def elastic_http_exception_handler(retries=1, delay=2):
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):

            for attempt in range(1, retries + 1):
                try:
                    return func(self, *args, **kwargs)
                except requests.exceptions.ConnectionError:
                    self.logger.error(f"Elastic unreachable (attempt {attempt})")

                except requests.exceptions.Timeout:
                    self.logger.error(f"Request timeout (attempt {attempt})")

                except requests.exceptions.HTTPError as e:
                    status = e.response.status_code
                    self.logger.error(f"HTTP error {status}")

                    if status < 500:
                        break

                except requests.exceptions.RequestException as e:
                    self.logger.error(f"Request error: {e}")
                    break
                
                except Exception as e:
                    self.logger.error(f"Unexpected error: {e}")
                    break
                time.sleep(delay)

            return self._fallback(*args, **kwargs)
        return wrapper
    return decorator


def snow_http_exception_handler(retries=1, delay=2):
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            
            for attempt in range(1, retries + 1):
                try:
                    return func(self, *args, **kwargs)
                except requests.exceptions.ConnectionError:
                    self.logger.error(f"SNOW unreachable (attempt {attempt})")

                except requests.exceptions.Timeout:
                    self.logger.error(f"Request timeout (attempt {attempt})")

                except requests.exceptions.HTTPError as e:
                    status = e.response.status_code
                    self.logger.error(f"HTTP error {status}")

                    if status < 500:
                        break

                except requests.exceptions.RequestException as e:
                    self.logger.error(f"Request error: {e}")
                    break
                
                except Exception as e:
                    self.logger.error(f"Unexpected error: {e}")
                    break
                time.sleep(delay)

            raise RecoverableError("All attempts failed to get data!")
        return wrapper
    return decorator


def zabbix_http_exception_handler(retries=1, delay=2):
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            
            for attempt in range(1, retries + 1):
                try:
                    return func(self, *args, **kwargs)
                except requests.exceptions.ConnectionError:
                    self.logger.error(f"Zabbix unreachable (attempt {attempt})")

                except requests.exceptions.Timeout:
                    self.logger.error(f"Request timeout (attempt {attempt})")

                except requests.exceptions.HTTPError as e:
                    status = e.response.status_code
                    self.logger.error(f"HTTP error {status}")

                    if status < 500:
                        break

                except requests.exceptions.RequestException as e:
                    self.logger.error(f"Request error: {e}")
                    break
                
                except Exception as e:
                    self.logger.error(f"Unexpected error: {e}")
                    break
                time.sleep(delay)

            raise RecoverableError("All attempts failed to get data!")
        return wrapper
    return decorator


import threading
from functools import wraps

def singleton_init_once(cls):
    cls._instance = None
    cls._lock = threading.Lock()  # Class-level lock for thread safety

    # 1. Wrap __new__ to ensure only one instance is created
    orig_new = cls.__new__
    
    @wraps(orig_new)
    def new_new(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:  # Double-checked locking pattern
                if cls._instance is None:
                    cls._instance = super(cls, cls).__new__(cls)
        return cls._instance

    # 2. Wrap __init__ to ensure it only runs once
    orig_init = cls.__init__

    @wraps(orig_init)
    def new_init(self, *args, **kwargs):
        # We check a flag on the instance
        if getattr(self, "_initialized", False):
            return
        
        with self.__class__._lock:
            # Double-check inside the lock
            if not getattr(self, "_initialized", False):
                orig_init(self, *args, **kwargs)
                self._initialized = True

    cls.__new__ = new_new
    cls.__init__ = new_init
    return cls

