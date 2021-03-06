import os
import threading
import pandas as pd
import datetime
import json
from data_base_test.helper_fanctions import comper_beween_data_frame


def set_parameter(column_type, record):
    # Takes column type and json record to determine column length/ percision

    if column_type == 'varchar' or column_type == 'Unknown':
        param = "(" + str(record['length']) + ")"
        if param == '(0)':
            param = '(1000)'
    elif column_type == 'decimal':
        param = "(" + str(record['precision']) + "," + str(record['scale']) + ")"
    else:
        param = ''
    return param


# Set Primary Key
def set_primary_key(column_name):
    # If id, then add primary key

    if column_name == 'id':
        param = 'Primary Key'
    else:
        param = ''
    return param


# Create csv file with column list
def create_csv_file_with_columns_list(md, object_name):
    flat_table = []
    for record in md['fields']:
        tmp = []
        tmp.append(record['name'])
        tmp.append(str(record['length']))
        tmp.append(record['label'])
        tmp.append((record['type']).lower())
        tmp.append(str(record['precision']))
        tmp.append(str(record['scale']))
        flat_table.append(tmp)

    headers = ['name', 'length', 'label', 'type', 'precision', 'scale']

    csv_file_name = "{}_metadata.csv".format(object_name)
    file = open(csv_file_name, 'w')
    file.write(','.join(headers) + '\n')

    for row in flat_table:
        file.write(','.join(tuple(row)) + '\n')
    file.close()
    print("Created Medatada CSV file")


def get_mapping():
    mapping = \
        {'id': 'varchar',
         'boolean': 'boolean',
         'reference': 'varchar',
         'string': 'varchar',
         'picklist': 'varchar',
         'textarea': 'varchar',
         'double': 'decimal',
         'phone': 'varchar',
         'url': 'varchar',
         'currency': 'double',
         'int': 'int',
         'datetime': 'timestamp',
         'date': 'timestamp',
         'email': 'varchar',
         'multipicklist': 'varchar',
         'percent': 'decimal',
         'decimal': 'decimal',
         'long': 'bigint',
         'address': 'varchar',
         'masterrecord': 'varchar',
         'location': 'varchar',
         'encryptedstring': 'varchar'}
    return mapping


# (6) Mapping function
def map_columns(json_data, new_table):
    ''' Takes json data from rest API and convert to Postgres Create Table Statement '''
    field_list = []
    counter = 1
    mapping = get_mapping()

    for record in json_data['fields']:
        tmp = []
        column_name = record['name'].lower()
        try:
            column_type = mapping[record['type'].lower()]
        except:
            column_type = 'Unknown'
        column_param = set_parameter(column_type, record)
        primary_key_param = set_primary_key(column_name)

        tmp.append(column_name)
        tmp.append(column_type)
        tmp.append(column_param)
        tmp.append(primary_key_param)
        counter += 1
        if counter <= len(json_data['fields']):
            if new_table:
                tmp.append(",")
        field_list.append(tmp)
    tmp = []
    if new_table:
        # field_list[len(field_list) - 1][3] = ','
        tmp.append('row_insert_date')
        tmp.append('timestamp')
        tmp.append('')
        tmp.append(',')
        field_list.insert(0, tmp)
    columns_list = []
    for i in field_list:
        columns_list.append(i[0])
    return field_list, columns_list


def return_ddl_query_create_table(field_list, object_name):
    ddl_query = f'Create Table {object_name} if not exists ( '
    create_table_ddl = ''
    for row in field_list:
        create_table_ddl += '  '.join(tuple(row))

    ddl_query += f' {create_table_ddl[:-2]})'
    return ddl_query


def return_ddl_query_add_columns(field_list, object_name, new_columns_list, schema_env):
    ddl_add_column = []
    ddl_query = f'alter table {schema_env}{object_name} add column '
    ddl_query = f'alter table {object_name} add column '
    for row in field_list:
        if row[0] in new_columns_list:
            temp_add_column = ' '.join(tuple(row))
            temp_ddl = ddl_query + temp_add_column
            ddl_add_column.append(temp_ddl)
    return ddl_add_column


def run_ddl_add_new_columns_to_snowflake_table(ddl_add_columns, get_connection_to_snowflake_for_data_lake,
                                               write_result_to_log):
    connection_to_snowflake = get_connection_to_snowflake_for_data_lake.get_connection_to_snowflake()
    for query in ddl_add_columns:
        get_connection_to_snowflake_for_data_lake.run_command_in_snowflake_db(query, connection_to_snowflake,
                                                                              write_result_to_log)
    connection_to_snowflake.close()


def get_restful_mata_data(sf, object_name):
    md = sf.sf.restful("sobjects/{}/describe/".format(object_name), params=None)
    return md


def get_columns_lis_from_snowflake_data_base(get_connection_to_snowflake_for_data_lake, query):
    connection_to_snowflake = get_connection_to_snowflake_for_data_lake.get_connection_to_snowflake()
    crm_table_data_lake_df = get_connection_to_snowflake_for_data_lake.run_query(query,
                                                                                 connection_to_snowflake)
    crm_table_data_lake_df.columns = crm_table_data_lake_df.columns.str.strip().str.lower()
    return crm_table_data_lake_df


def return_new_columns(snowflake_crm_df, columns_list_from_crm, write_result_to_log, object_name):
    s = set(snowflake_crm_df)
    temp3 = [x for x in columns_list_from_crm if x not in s]
    crm = set(columns_list_from_crm)
    columns_only_in_snowflake = [x for x in snowflake_crm_df if x not in crm]
    if 'row_insert_date' in columns_only_in_snowflake:
        columns_only_in_snowflake.remove('row_insert_date')
    write_result_to_log.info(f'Columns that exist in CRM and not exist in SNOWFLAKE for table {object_name} : {temp3}')
    write_result_to_log.info(
        f'Columns that exist in SNOWFLAKE and not exist in CRM for table {object_name} : {columns_only_in_snowflake}')
    return temp3, columns_only_in_snowflake


def get_list_of_all_columns_with_type_timestamp(field_list):
    list_of_timestamp_column = []
    for i in field_list:
        if i[1] == 'timestamp':
            list_of_timestamp_column.append(i[0].upper())
    return list_of_timestamp_column


def change_date_format_for_column_with_type_timestamp(df, list_of_timestamp_column):
    for i in list_of_timestamp_column:
        df[i] = pd.to_datetime(df[i])
        df[i] = df[i].dt.strftime('%Y-%m-%d %H:%M:%S')
    return df


def cange_date_for_crm_data(LastModifiedDate):
    modified_date = datetime.datetime.strftime(LastModifiedDate, '%Y-%m-%d %H:%M:%S')
    modified_date = modified_date.replace(' ', 'T')
    modified_date += '.000Z'
    return modified_date


def return_crm_table_with_all_columns(object_name, columns_list_from_crm, sf,
                                      field_list,
                                      object_list_from_snowflake_data_lake
                                      , write_result_to_log,
                                      columns_only_in_snowflake):
    all_columns = ' ,'.join(columns_list_from_crm)

    try:
        crm_sql_query = f'select {all_columns} from {object_name}'
        crm_data = sf.get_data_from_salesforce(crm_sql_query, write_result_to_log)
        write_result_to_log.info(f'Number of rows that loud from CRM for object {object_name} : {crm_data.shape[0]} ')
        if crm_data.shape[0] != 0:
            crm_data.columns = crm_data.columns.str.strip().str.upper()
            # Order the columns of the local dataframe to be in the same order like the snowflake table
            crm_data['ROW_INSERT_DATE'] = datetime.datetime.now()
            if len(columns_only_in_snowflake) > 0:
                for column in columns_only_in_snowflake:
                    crm_data[column.upper()] = None
            order_columns = [x.upper() for x in object_list_from_snowflake_data_lake]
            crm_data = crm_data[order_columns]
            # Remove for each time stamp the char (T and +) that came from the crm
            list_of_timestamp_column = get_list_of_all_columns_with_type_timestamp(field_list)
            crm_data = change_date_format_for_column_with_type_timestamp(crm_data, list_of_timestamp_column)
        return crm_data
    except Exception as e:
        write_result_to_log.error(f'Error from get CRM DATA for object {object_name} Error message : {e.args}')


def return_only_rows_that_change_or_new_rows(object_name, current_run_data_frame, write_result_to_log):
    try:

        file_name = f"{object_name}_last_full_data_frame.csv"
        file_path = os.path.abspath(file_name)
        last_run_data_frame = pd.read_csv(file_path)
        del last_run_data_frame['ROW_INSERT_DATE']
        columns_type = current_run_data_frame.dtypes
        columns_type = columns_type.to_dict()
        last_run_data_frame = last_run_data_frame.astype(columns_type)
        comper_result = comper_beween_data_frame.comper_data_frame_old_and_new_data_frame(current_run_data_frame,
                                                                                          last_run_data_frame, 'ID')
        write_result_to_log.info(comper_result)
        delta_data_frame = current_run_data_frame.merge(last_run_data_frame, on='id', how='outer', indicator=True).loc[
            lambda x: x['_merge'] == 'left_only']
        delta_data_frame.insert(0, 'ROW_INSERT_DATE', datetime.datetime.now())
        write_result_to_log.info(f'Daily delta for table : {object_name} = {delta_data_frame.shape[0]}')
        del delta_data_frame['_merge']
        return delta_data_frame
    except Exception as e:
        test = e


def save_current_data_frame_to_csv(object_name, crm_data_df, file_name, header, write_result_to_log):
    file_path = os.path.abspath(file_name)
    try:
        crm_data_df.to_csv(file_path, index=False, header=header)
        return file_path
    except Exception as e:
        write_result_to_log.error(f'Error for object {object_name} CSV file not created : {e.args}')


def create_new_table_in_snwflake(field_list, object_name, get_connection_to_snowflake_for_data_lake,
                                 write_result_to_log):
    query = return_ddl_query_create_table(field_list, object_name)
    try:
        connection_to_snowflake = get_connection_to_snowflake_for_data_lake.get_connection_to_snowflake()
        with connection_to_snowflake.cursor() as con:
            con.execute(f"{query}")

    except Exception as e:
        write_result_to_log.error(f'Error from create new table in snowflake :{e}')


def upload_the_csv_to_snowflake_stage_and_insert_to_relevant_table(get_connection_to_snowflake_for_data_lake,
                                                                   object_name, file_path, write_result_to_log):
    lock = threading.Lock()
    try:

        lock.acquire()
        connection_to_snowflake = get_connection_to_snowflake_for_data_lake.get_connection_to_snowflake()
        sql_query_remove_old_csv_from_stage = f"remove @%{object_name}"
        get_connection_to_snowflake_for_data_lake.run_command_in_snowflake_db(sql_query_remove_old_csv_from_stage,
                                                                              connection_to_snowflake,
                                                                              write_result_to_log)
        uplod_csv_to_snowflake_stage = f"put file://{file_path}* @%{object_name} PARALLEL=1 "
        get_connection_to_snowflake_for_data_lake.run_command_in_snowflake_db(uplod_csv_to_snowflake_stage,
                                                                              connection_to_snowflake,
                                                                              write_result_to_log)

        write_result_to_log.info(f'Finished to upload {object_name} scv to snowflake stage ')
        copy_csv_from_stage_to_snowflake_table = f'''copy into {object_name} file_format=(TYPE=csv field_delimiter=',' skip_header=0 FIELD_OPTIONALLY_ENCLOSED_BY = '"')'''
        get_connection_to_snowflake_for_data_lake.run_command_in_snowflake_db(copy_csv_from_stage_to_snowflake_table,
                                                                              connection_to_snowflake,
                                                                              write_result_to_log)
        write_result_to_log.info(f'Copy {object_name} data to snowflake ')

    except Exception as e:
        error_message = e.args
        write_result_to_log.error(f'Error from upload/copy new csv file :{error_message}')
    finally:
        lock.release()


def save_json_file_for_columns_that_exist_in_snownflake_and_not_in_crm(object_name, columns_list):
    dictionary_data = {object_name: columns_list}
    with open(f'{object_name}.json', "w") as file:
        json.dump(dictionary_data, file)


def read_json_file_for_columns_that_exist_in_snownflake_and_not_in_crm(object_name):
    with open(f'{object_name}.json', "r") as file:
        output = json.load(file)
        return output


def new_deleted_column_in_snowflake(old_list, new_list):
    result = all(elem in old_list for elem in new_list)
    return result
