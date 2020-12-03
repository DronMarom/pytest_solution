from datetime import datetime
import datacompy


def comper_data_frame_old_and_new_data_frame(integ, production, joinOnFields, file_name=''):
    compare = datacompy.Compare(
        integ,
        production,
        join_columns=joinOnFields,  # You can also specify a list of columns
        # abs_tol=0,  # Optional, defaults to 0
        # rel_tol=0,  # Optional, defaults to 0
        # df1_name='spsOld',  # Optional, defaults to 'df1'
        # df2_name='New'  # Optional, defaults to 'df2'
    )

    compare.matches(ignore_extra_columns=True)
    # This method prints out a human-readable report summarizing and sampling differences
    result = compare.report()
    return result


def write_test_result_to_file(file_name, result):
    now = datetime.now()
    date_time = now.strftime("%m-%d-%Y,%H:%M:%S")
    with open(file_name + '_' + date_time + 'result.txt', 'a') as resultSummary:
        resultSummary.write(result)


def is_test_failed(test_result, data_frame_local, data_frame_production):
    number_of_uneqale_rows = test_result.split("Number of rows with some compared columns unequal: ")[1]
    number_of_uneqale_rows = number_of_uneqale_rows[0]
    if data_frame_local.shape[0] == data_frame_production.shape[0] and int(number_of_uneqale_rows) == 0:
        return 1
    else:
        return 0
