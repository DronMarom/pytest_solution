import os
import queue
import threading
import datetime
import time

from data_base_test.data_lake import helper_function_for_data_lake
from pytest import mark
from log import log_file


@mark.data_lake_crm_table
def test_insert_new_data_to_snowflake_crm_table(get_connection_to_snowflake_for_data_lake, get_connaction_to_crm,
                                                data_config):
    os.environ['TEST_NAME'] = os.environ.get(
        'PYTEST_CURRENT_TEST').split(':')[-1].split(' ')[0]
    write_result_to_log = log_file.setup_logger()
    get_table_list_from_crm_management_table = '''select * from crm_management_table'''
    object_from_snowflake_crm_management_table_data_lake = helper_function_for_data_lake.get_columns_lis_from_snowflake_data_base(
        get_connection_to_snowflake_for_data_lake,
        get_table_list_from_crm_management_table)
    object_from_snowflake_crm_management_table_data_lake = object_from_snowflake_crm_management_table_data_lake[
        object_from_snowflake_crm_management_table_data_lake['run_load'] == True
        ]
    list_of_crm_table = object_from_snowflake_crm_management_table_data_lake['crm_table_name']

    def louad_data_to_snowflake(crm_table):
        object_name = crm_table
        columns_only_in_snowflake = []
        schema_env = 'QA.CRM_DATA.'
        print("Target Object: {}".format(object_name))
        write_result_to_log.info(f'Start to run : {object_name} table')
        crm_table_decision = object_from_snowflake_crm_management_table_data_lake.loc[
            object_from_snowflake_crm_management_table_data_lake['crm_table_name'] == object_name].reset_index()
        mata_data = helper_function_for_data_lake.get_restful_mata_data(get_connaction_to_crm, object_name)
        field_list, columns_list = helper_function_for_data_lake.map_columns(mata_data,
                                                                             crm_table_decision['is_new_table'][0])
        if crm_table_decision['is_new_table'][0]:
            first_columns_only_in_snowflake=[]
            helper_function_for_data_lake.create_new_table_in_snwflake(field_list, object_name,
                                                                       get_connection_to_snowflake_for_data_lake,
                                                                       write_result_to_log)
            helper_function_for_data_lake.save_json_file_for_columns_that_exist_in_snownflake_and_not_in_crm(
                object_name,
                first_columns_only_in_snowflake)

            del columns_list[0]
        else:
            query = '''select * from QA.CRM_DATA.{} limit 1 '''.format(object_name)
            object_from_snowflake_data_lake = helper_function_for_data_lake.get_columns_lis_from_snowflake_data_base(
                get_connection_to_snowflake_for_data_lake,
                query)
            new_fields_in_crm_tables, columns_only_in_snowflake = helper_function_for_data_lake.return_new_columns(
                list(object_from_snowflake_data_lake.columns),
                columns_list, write_result_to_log, object_name)
            if len(new_fields_in_crm_tables) > 0:
                ddl = helper_function_for_data_lake.return_ddl_query_add_columns(field_list, object_name,
                                                                                 new_fields_in_crm_tables, schema_env)
                helper_function_for_data_lake.run_ddl_add_new_columns_to_snowflake_table(ddl,
                                                                                         get_connection_to_snowflake_for_data_lake,
                                                                                         write_result_to_log)
        query = '''select * from QA.CRM_DATA.{} limit 1 '''.format(object_name)
        object_from_snowflake_data_lake = helper_function_for_data_lake.get_columns_lis_from_snowflake_data_base(
            get_connection_to_snowflake_for_data_lake,
            query)

        crm_data_df = helper_function_for_data_lake.return_crm_table_with_all_columns(object_name, columns_list,
                                                                                      get_connaction_to_crm,
                                                                                      field_list,
                                                                                      list(
                                                                                          object_from_snowflake_data_lake.columns)
                                                                                      , write_result_to_log,
                                                                                      columns_only_in_snowflake)
        del crm_data_df['ROW_INSERT_DATE']
        columns_only_in_snowflake_old = helper_function_for_data_lake.read_json_file_for_columns_that_exist_in_snownflake_and_not_in_crm(
            object_name)
        is_new_delete_column = helper_function_for_data_lake.new_deleted_column_in_snowflake(columns_only_in_snowflake_old[object_name],columns_only_in_snowflake)

        if crm_table_decision['is_new_table'][0] == True or is_new_delete_column==False:
            helper_function_for_data_lake.save_json_file_for_columns_that_exist_in_snownflake_and_not_in_crm(
                object_name,
                columns_only_in_snowflake)
            file_name = f"{object_name}_last_full_data_frame.csv"
            header = True
            helper_function_for_data_lake.save_current_data_frame_to_csv(object_name, crm_data_df, file_name,
                                                                         header,
                                                                         write_result_to_log)

        if (crm_table_decision['is_new_table'][0] == False) and (
                crm_table_decision['incremental_load'][0] == True) and (is_new_delete_column==True):
            file_name = f"{object_name}_last_full_data_frame.csv"
            header = True
            temp_crm_data_df = crm_data_df
            crm_data_df = helper_function_for_data_lake.return_only_rows_that_change_or_new_rows(object_name,
                                                                                                 crm_data_df,
                                                                                                 write_result_to_log)

            helper_function_for_data_lake.save_current_data_frame_to_csv(object_name, temp_crm_data_df, file_name,
                                                                         header,
                                                                         write_result_to_log)

        if crm_data_df.shape[0] != 0:
            file_name = f"{object_name}.csv"
            header = False
            crm_data_df.insert(0, 'ROW_INSERT_DATE', datetime.datetime.now())
            file_path = helper_function_for_data_lake.save_current_data_frame_to_csv(object_name, crm_data_df,
                                                                                     file_name,
                                                                                     header,
                                                                                     write_result_to_log)

            helper_function_for_data_lake.upload_the_csv_to_snowflake_stage_and_insert_to_relevant_table(
                get_connection_to_snowflake_for_data_lake,
                object_name, file_path, write_result_to_log)

        # Update table crm_management_table after each run
        conn = get_connection_to_snowflake_for_data_lake.get_connection_to_snowflake()
        sql_query_update_is_new_table = '''update crm_management_table set is_new_table=False where 
        crm_table_name='%s' ''' % (object_name)
        get_connection_to_snowflake_for_data_lake.update_columns_in_snowflake_db(sql_query_update_is_new_table, conn)
        conn.close()

        time_now = datetime.datetime.now()
        write_result_to_log.info(f' End run object {object_name} End time : {time_now}')

    def worker():
        while True:
            item = q.get()
            if item is None:
                break
            louad_data_to_snowflake(item)
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
