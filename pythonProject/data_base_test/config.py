import pandas as pd


class Config:
    def __init__(self, env='test_env', data_base_type='snowflake'):
        self.snowflake_connaction = {"test_env": ["DORON_M", "Qwerty12", "lumos.eu-west-1", "LOAD_WH"]}[env]
        self.crm_connaction = \
            {"test_env": ["gadi@lumos.com.sbe2e", "PrOfit@2019", "3PhEcORaDW8OpZINQkC7Bj7O3", 'test'],
             # "test_env": ["isahar.profis@lumos-global.com.preprod","1234Qweas","UCis88PVOJDMGdN3IKh9tQkQ","test"],
             "production_env": ["bi_etl@lumos-global.com", "arik123", "AiyAR5CQe6esB05rxccpePcnC", "None"]}[env]
        self.mysql_connactin = {"test_env": ["dwh-integ.lms.lumosglobal.com", "etl_user", "LH4a7t", 3306]}[env]
        self.dwh_nl_data_base = data_base_type
        self.snowflake_connaction_for_data_lake = {"test_env": ["DORON_M", "Qwerty12", "lumos.eu-west-1", "LOAD_WH",
                                                                "QA", "CRM_DATA"]}[env]

        self.empty_data_frame = pd.DataFrame()
        self.number_of_threads = 4
