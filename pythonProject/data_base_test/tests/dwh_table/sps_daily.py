from pytest import fixture
from pytest import mark

from data_base_test.data_base_querys import dwh_nl_querys
from data_base_test.helper_fanctions import comper_beween_data_frame
import pandas as pd


@fixture(scope='session')
def get_data_from_dwh_nl_sps(get_object_according_to_data_base_type, data_config):
    # if 'snowflake' in data_config.dwh_nl_data_base:
    #     connection_to_snowflake = get_object_according_to_data_base_type.get_connection_to_snowflake()
    #     dwh_nl_sps_df = get_object_according_to_data_base_type.run_query(snowflake_querys.DWH_NL_SPS,
    #                                                                      connection_to_snowflake)
    #     dwh_nl_sps_df.columns = dwh_nl_sps_df.columns.str.strip().str.lower()
    #
    #     return dwh_nl_sps_df

    dwh_nl_sps_df = get_object_according_to_data_base_type.get_connection_to_mysql(
        dwh_nl_querys.DWH_NL_SPS_FOR_SPS_DAILY)
    return dwh_nl_sps_df


@fixture(scope='session')
def get_data_from_inventory_idu(get_object_according_to_data_base_type, data_config):
    # if 'snowflake' in data_config.dwh_nl_data_base:
    #     connection_to_snowflake = get_object_according_to_data_base_type.get_connection_to_snowflake()
    #     dwh_nl_sps_df = get_object_according_to_data_base_type.run_query(snowflake_querys.DWH_NL_SPS,
    #                                                                      connection_to_snowflake)
    #     dwh_nl_sps_df.columns = dwh_nl_sps_df.columns.str.strip().str.lower()
    #
    #     return dwh_nl_sps_df

    dwh_nl_inventory_idu = get_object_according_to_data_base_type.get_connection_to_mysql(
        dwh_nl_querys.DWH_NL_INVENTORY_IDU)
    return dwh_nl_inventory_idu


@fixture(scope='session')
def get_data_from_stg_sps_daily_vw(get_object_according_to_data_base_type, data_config):
    # if 'snowflake' in data_config.dwh_nl_data_base:
    #     connection_to_snowflake = get_object_according_to_data_base_type.get_connection_to_snowflake()
    #     dwh_nl_sps_df = get_object_according_to_data_base_type.run_query(snowflake_querys.DWH_NL_SPS,
    #                                                                      connection_to_snowflake)
    #     dwh_nl_sps_df.columns = dwh_nl_sps_df.columns.str.strip().str.lower()
    #
    #     return dwh_nl_sps_df

    dwh_nl_inventory_idu = get_object_according_to_data_base_type.get_connection_to_mysql(
        dwh_nl_querys.DWH_STG_SPS_DAILY_VW)
    return dwh_nl_inventory_idu


@mark.dwh_table
@mark.sps_daily
@mark.full_run
class dwh_sps_daily_Test:
    # @staticmethod
    # def test_curr_panel1_wattage_curr_panel2_wattage(get_data_from_dwh_nl_sps_daily, get_data_from_dwh_nl_sps,
    #                                                  data_config):
    #     data_config.empty_data_frame = get_data_from_dwh_nl_sps
    #     comper_result = comper_beween_data_frame.comper_data_frame_old_and_new_data_frame(get_data_from_dwh_nl_sps,
    #                                                                                       get_data_from_dwh_nl_sps_daily,
    #                                                                                       'sps_short_num')
    #     assert 1 == comper_beween_data_frame.is_test_failed(comper_result, get_data_from_dwh_nl_sps,
    #                                                         get_data_from_dwh_nl_sps_daily)

    @staticmethod
    def test_curr_bin_warehouse(get_data_from_dwh_erp_vw_ref_sn_idu_territory,
                                get_data_from_dwh_erp_vw_ref_warehouse_territory, get_data_from_inventory_idu,
                                get_data_from_stg_sps_daily_vw):
        print (get_data_from_dwh_erp_vw_ref_warehouse_territory['erp_company'])
        get_data_from_dwh_erp_vw_ref_sn_idu_territory = get_data_from_dwh_erp_vw_ref_sn_idu_territory.rename(columns=
        {
            'serial_number':
                'sps_short_num'})
        local_curr_binwarehouse = pd.merge(get_data_from_inventory_idu, get_data_from_dwh_erp_vw_ref_sn_idu_territory,
                                           how='left', on='sps_short_num')
        del local_curr_binwarehouse['erp_company']
        columns_list = local_curr_binwarehouse.columns.values.tolist()
        local_curr_binwarehouse = pd.merge(local_curr_binwarehouse, get_data_from_dwh_erp_vw_ref_warehouse_territory,
                                           how='left', on='warehouse_key')
        # columns_list=local_curr_binwarehouse.columns.values.tolist()
        print (columns_list)
        local_curr_binwarehouse = local_curr_binwarehouse.loc[local_curr_binwarehouse['erp_company_name'] ==
                                                              local_curr_binwarehouse['erp_company']]
        local_curr_binwarehouse = local_curr_binwarehouse.rename(columns={'bin_name': 'curr_bin_warehouse'})
        local_curr_binwarehouse = local_curr_binwarehouse[['sps_short_num', 'curr_bin_warehouse']]
        comper_result = comper_beween_data_frame.comper_data_frame_old_and_new_data_frame(local_curr_binwarehouse,
                                                                                          get_data_from_stg_sps_daily_vw,
                                                                                          'sps_short_num')
        print(comper_result)
        assert 1 == comper_beween_data_frame.is_test_failed(comper_result, local_curr_binwarehouse,
                                                            get_data_from_stg_sps_daily_vw)
