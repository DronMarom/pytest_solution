import os
import queue
import threading
import datetime
import time

from data_base_test.data_lake import helper_function_for_data_lake
from pytest import mark
from log import log_file


@mark.data_lake_crm_table
def test_insert_new_data_to_snowflake_crm_table(get_connection_to_snowflake_for_data_lake, get_connaction_to_crm, data_config):
    os.environ['TEST_NAME'] = os.environ.get(
        'PYTEST_CURRENT_TEST').split(':')[-1].split(' ')[0]
    write_result_to_log = log_file.setup_logger()

    # sf_test = dict(username='gadi@lumos.com.sbe2e', password="PrOfit@2019",
    #                security_token='3PhEcORaDW8OpZINQkC7Bj7O3', domain='test')
    # sf_test = dict(username='isahar.profis@lumos-global.com.preprod', password="1234Qweas",
    #                security_token='UCis88PVOJDMGdN3IKh9tQkQ', domain='test')
    #
    # sf = Salesforce(**sf_test)
    # sf = get_connaction_to_crm
    get_table_list_from_crm_management_table = '''select * from crm_management_table'''
    object_from_snowflake_crm_management_table_data_lake = helper_function_for_data_lake.get_columns_lis_from_snowflake_data_base(
        get_connection_to_snowflake_for_data_lake,
        get_table_list_from_crm_management_table)
    object_from_snowflake_crm_management_table_data_lake = object_from_snowflake_crm_management_table_data_lake[
        object_from_snowflake_crm_management_table_data_lake['run_load'] == True
    ]
    list_of_crm_table = object_from_snowflake_crm_management_table_data_lake['crm_table_name']

    def first_check(crm_table):
        object_name = crm_table
        schema_env = 'QA.CRM_DATA.'
        print("Target Object: {}".format(object_name))
        write_result_to_log.info(f'Start to run : {object_name} table')
        crm_table_decision = object_from_snowflake_crm_management_table_data_lake.loc[
            object_from_snowflake_crm_management_table_data_lake['crm_table_name'] == object_name].reset_index()


        last_modified_date = crm_table_decision['last_modified_date'][0]
        modified_date = helper_function_for_data_lake.cange_date_for_crm_data(last_modified_date)
        mata_data = helper_function_for_data_lake.get_restful_mata_data(get_connaction_to_crm, object_name)
        field_list, columns_list = helper_function_for_data_lake.map_columns(mata_data, crm_table_decision['is_new_table'][0])
        if crm_table_decision['is_new_table'][0]:
            helper_function_for_data_lake.create_new_table_in_snwflake(field_list, object_name,
                                                                       get_connection_to_snowflake_for_data_lake,
                                                                       write_result_to_log)
            del columns_list[-1]
        else:
            query = '''select * from QA.CRM_DATA.{} limit 1 '''.format(object_name)
            object_from_snowflake_data_lake = helper_function_for_data_lake.get_columns_lis_from_snowflake_data_base(
                get_connection_to_snowflake_for_data_lake,
                query)
            new_fields_in_crm_tables = helper_function_for_data_lake.return_new_columns(
                list(object_from_snowflake_data_lake.columns),
                columns_list, write_result_to_log, object_name)
            if len(new_fields_in_crm_tables) > 0:
                ddl = helper_function_for_data_lake.return_ddl_query_add_columns(field_list, object_name,
                                                                                 new_fields_in_crm_tables, schema_env)
                helper_function_for_data_lake.run_ddl_add_new_columns_to_snowflake_table(ddl,
                                                                                         get_connection_to_snowflake_for_data_lake)
        query = '''select * from QA.CRM_DATA.{} limit 1 '''.format(object_name)
        object_from_snowflake_data_lake = helper_function_for_data_lake.get_columns_lis_from_snowflake_data_base(
            get_connection_to_snowflake_for_data_lake,
            query)

        crm_data_df = helper_function_for_data_lake.return_crm_table_with_all_columns(object_name, columns_list,
                                                                                      get_connaction_to_crm,
                                                                                      field_list,
                                                                                      list(
                                                                                          object_from_snowflake_data_lake.columns),
                                                                                      crm_table_decision[
                                                                                          'incremental_load'][0],modified_date
                                                                                      , write_result_to_log)
        file_path = helper_function_for_data_lake.convert_data_frame_to_csv_befor_upload_to_snowflake(object_name,
                                                                                                      crm_data_df,
                                                                                                      write_result_to_log)
        helper_function_for_data_lake.upload_the_csv_to_snowflake_stage_and_insert_to_relevant_table(
            get_connection_to_snowflake_for_data_lake,
            object_name, file_path, write_result_to_log)

        # Update table crm_management_table after each run
        conn = get_connection_to_snowflake_for_data_lake.get_connection_to_snowflake()
        if crm_data_df.shape[0] != 0:
            max_last_modified_date = max(crm_data_df['LASTMODIFIEDDATE'])
            sql_query_update_last_modified_date = '''update crm_management_table set last_modified_date = '%s' where 
            crm_table_name='%s' ''' %(max_last_modified_date, object_name)
            get_connection_to_snowflake_for_data_lake.update_columns_in_snowflake_db(
                sql_query_update_last_modified_date, conn)

        sql_query_update_is_new_table = '''update crm_management_table set is_new_table=False where 
        crm_table_name='%s' ''' %(object_name)
        get_connection_to_snowflake_for_data_lake.update_columns_in_snowflake_db(sql_query_update_is_new_table, conn)
        conn.close()

        time_now = datetime.datetime.now()
        write_result_to_log.info(f' End run object {object_name}')

    time_now = datetime.datetime.now()
    write_result_to_log.info(f'start test : {time_now}')

    def worker():
        while True:
            item = q.get()
            if item is None:
                break
            first_check(item)
            q.task_done()
    q = queue.Queue()

    threads = []
    for item in list_of_crm_table:
        q.put(item)

    for i in range(data_config.number_of_threads):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)
    q.join()

    print('stopping workers!')

    # stop workers
    for i in range(data_config.number_of_threads):
        q.put(None)

    for t in threads:
        t.join()

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
