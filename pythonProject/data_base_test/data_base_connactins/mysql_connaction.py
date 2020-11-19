
import pandas as pd
import pymysql


# Gets the version
class ConnectionToMySql:
    host = None
    user = None
    password = None
    db = None
    port = None

    def __init__(self, user, password, host, db, port):
        self.user = user
        self.password = password
        self.host = host
        self.db = db
        self.port = port

    def get_connection_to_mysql(self, sql_query):

        db = pymysql.connect(self.host, self.user, self.password, self.db, self.port)
        try:
            # Execute the SQL command

            df = pd.read_sql_query(sql_query, db)
            db.close()
            return df


        except pymysql.Error as e:
            print(e)

        # db.close()
