import pandas as pd


class Config:
    def __init__(self, env='test_env', data_base_type='snowflake'):
        self.snowflake_connaction = {"test_env": ["DORON_M", "Qwerty12", "lumos.eu-west-1", "LOAD_WH"]}[env]
        self.crm_connaction = \
            {"test_env": ["etl_dev@lumos-global.com", "Nova@1900", "9AwLIazlbdVJl3qni7LVRWNC", 'test'],
             "production_env": ["bi_etl@lumos-global.com", "arik123", "AiyAR5CQe6esB05rxccpePcnC", "None"]}[env]
        self.mysql_connactin = {"test_env": ["dwh-integ.lms.lumosglobal.com", "etl_user", "LH4a7t", 3306]}[env]
        self.dwh_nl_data_base = data_base_type
        self.log_directory = {"crm_table": "/home/doron/PycharmProjects/pythonProject/data_base_test/crm_tables"
                                           "/crm_result_directory"}

        self.empty_data_frame = pd.DataFrame()