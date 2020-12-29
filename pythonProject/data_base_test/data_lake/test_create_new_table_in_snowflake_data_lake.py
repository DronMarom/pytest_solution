import os

from data_base_test.data_lake import helper_function_for_data_lake
from simple_salesforce import Salesforce
from pytest import mark

from data_base_test.helper_fanctions import comper_beween_data_frame


@mark.data_lake_crm_table
def test_create_new_table_in_snowflake_data_lake(get_connection_to_snowflake_for_data_lake, get_connaction_to_crm):
    os.environ['TEST_NAME'] = os.environ.get(
        'PYTEST_CURRENT_TEST').split(':')[-1].split(' ')[0]
    sf_test = dict(username='gadi@lumos.com.sbe2e', password="PrOfit@2019",
                   security_token='3PhEcORaDW8OpZINQkC7Bj7O3', domain='test')
    sf = Salesforce(**sf_test)
    object_name = 'Case'  # sys.argv[1]
    schema_env = 'QA.CRM_DATA.'
    print("Target Object: {}".format(object_name))
    mata_data = helper_function_for_data_lake.get_restful_mata_data(sf, object_name)
    field_list, columns_list = helper_function_for_data_lake.map_columns(mata_data, True)

    helper_function_for_data_lake.create_new_table_in_snwflake(field_list, object_name,
                                                               get_connection_to_snowflake_for_data_lake)
    query = '''select * from QA.CRM_DATA.{} limit 1 '''.format(object_name)
    object_from_snowflake_data_lake = helper_function_for_data_lake.get_columns_lis_from_snowflake_data_base(
        get_connection_to_snowflake_for_data_lake,
        query)
    crm_data_df = helper_function_for_data_lake.return_crm_table_with_all_columns(object_name, columns_list,
                                                                                  get_connaction_to_crm,
                                                                                  field_list,
                                                                                  list(
                                                                                      object_from_snowflake_data_lake.columns))

    file_path = helper_function_for_data_lake.convert_data_frame_to_csv_befor_upload_to_snowflake(object_name,
                                                                                                   crm_data_df)
    helper_function_for_data_lake.upload_the_csv_to_snowflake_stage_and_insert_to_relevant_table(
        get_connection_to_snowflake_for_data_lake,
        object_name, file_path)

    # query = '''select * from QA.CRM_DATA.{} '''.format(object_name)
    #
    # object_from_snowflake_data_lake_df = helper_function_for_data_lake.get_columns_lis_from_snowflake_data_base(
    #     get_connection_to_snowflake_for_data_lake,
    #     query)
    # comper_result = comper_beween_data_frame.comper_data_frame_old_and_new_data_frame(crm_data_df,
    #                                                                                   object_from_snowflake_data_lake_df,
    #                                                                                   'ID')
    # assert 1 == comper_beween_data_frame.is_test_failed(comper_result, crm_data_df,
    #                                                     object_from_snowflake_data_lake_df)
    print('sdfdfs')
