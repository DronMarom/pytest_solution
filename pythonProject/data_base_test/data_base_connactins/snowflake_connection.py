import pandas as pd
import snowflake.connector
from pymysql import Error
from  pandas.io.sql import DatabaseError
from sqlalchemy import exc as sa_exc

class SnowFlakeConnection:
    user = None
    password = None
    account = None
    warehouse = None

    def __init__(self, user, password, account, warehouse , database='', schema=''):
        self.user = user
        self.password = password
        self.account = account
        self.warehouse = warehouse
        self.database = database
        self.schema = schema

    def get_connection_to_snowflake(self):
        try:
            db = snowflake.connector.connect(
                user=self.user,
                password=self.password,
                account=self.account,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema
            )

            return db

        except TypeError:

            print(TypeError)
            raise SystemExit


    def run_query(self, sql_query, db):
        try:
            df = pd.read_sql_query(sql_query, db)
            db.close()
            return df
        except DatabaseError as e:
            error_cuse = e.args[0]
            return error_cuse


    @staticmethod
    def run_command_in_snowflake_db(sql_query, db, write_result_to_log):
        try:
            cursor = db.cursor()
            cursor.execute(sql_query)
        except Exception as e:
            error_message = e.args
            write_result_to_log.error(f'failed to add new column to snowflake table {error_message}')


    @staticmethod
    def update_columns_in_snowflake_db(sql_query, db):
        try:
            cursor = db.cursor()
            cursor.execute(sql_query)
        except Exception as e:
            error_message = e.args
            return error_message

    @staticmethod
    def create_new_table_in_snowflake_db(sql_query, db):
        try:
            cursor = db.cursor()
            cursor.execute(sql_query)
        except Exception as e:
            error_message = e.raw_msg
            return error_message
