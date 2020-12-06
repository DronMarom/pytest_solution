from pytest import fixture
from log import log_file
from data_base_test.config import Config
from data_base_test.data_base_connactins import mysql_connaction, snowflake_connection, \
    sales_force_connection
from data_base_test.fixtures_get_data.fixtures_to_crm import *
from data_base_test.fixtures_get_data.fixtures_to_mysql_snowflake import *



# def pytest_addoption(parser):
#     parser.addoption("--env", action="store",
#                      help="Data env")
#     parser.addoption("--data_base_type", action="store",
#                      help="Witch data to use snowflake or mysql")


# @fixture(scope='session')
# def env(request):
#     print(request.config.getoption("--env"))
#     return request.config.getoption("--env")
#
#
# @fixture(scope='session')
# def data_base_type(request):
#     return request.config.getoption("--data_base_type")


@fixture(scope='session')
def data_config(env='test_env', data_base_type='mysql'):
    cfg = Config(env, data_base_type)
    return cfg


@fixture(scope='session')
def get_connaction_to_crm(data_config):
    sf_details = dict(username=data_config.crm_connaction[0], password=data_config.crm_connaction[1],
                      security_token=data_config.crm_connaction[2], domain=data_config.crm_connaction[3])

    # sf_details = dict(username="bi_etl@lumos-global.com", password="arik123",
    #                   security_token="AiyAR5CQe6esB05rxccpePcnC", domain=None)
    salefroce_connection_test_object = sales_force_connection.DataFromSF(**sf_details)
    return salefroce_connection_test_object


@fixture(scope='session')
def get_object_according_to_data_base_type(data_config):
    if data_config.dwh_nl_data_base == 'snowflake':
        snowflake_obj = snowflake_connection.SnowFlakeConnection(data_config.snowflake_connaction[0],
                                                                 data_config.snowflake_connaction[1],
                                                                 data_config.snowflake_connaction[2],
                                                                 data_config.snowflake_connaction[3])
        return snowflake_obj
    else:
        mysql_conn = mysql_connaction.ConnectionToMySql(data_config.mysql_connactin[1],
                                                        data_config.mysql_connactin[2],
                                                        data_config.mysql_connactin[0], '',
                                                        data_config.mysql_connactin[3])
        return mysql_conn


# log_file.delete_log_files()
