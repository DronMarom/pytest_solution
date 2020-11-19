import pandas as pd
import snowflake.connector


class SnowFlakeConnection:
    user = None
    password = None
    account = None
    warehouse = None

    def __init__(self, user, password, account, warehouse):
        self.user = user
        self.password = password
        self.account = account
        self.warehouse = warehouse


    def get_connection_to_snowflake(self):
        try:
            db = snowflake.connector.connect(
                user=self.user,
                password=self.password,
                account=self.account,
                warehouse=self.warehouse
            )

            return db

        except TypeError:

            print(TypeError)


    def run_query(self, sql_query, db):
        df = pd.read_sql_query(sql_query, db)
        return df
