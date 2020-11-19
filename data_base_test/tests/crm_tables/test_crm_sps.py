import pandas as pd
import numpy as np
from pytest import mark
from data_base_test.helper_fanctions import comper_beween_data_frame


@mark.crm_table
@mark.sps
@mark.full_run
class crm_sps_Test:
    def test_sps_customer_ownership_flag(self, get_data_from_hps, get_data_from_dwh_nl_sps, data_config):
        crm_customer_ownership_flag_df = get_data_from_hps[['sps_key', 'customer_ownership_flag']]
        conditions = [crm_customer_ownership_flag_df['customer_ownership_flag'] == True,
                      crm_customer_ownership_flag_df['customer_ownership_flag'] == False]
        choices = [1, 0]
        crm_customer_ownership_flag_df['customer_ownership_flag'] = np.select(conditions, choices, default=0)
        data_config.empty_data_frame = crm_customer_ownership_flag_df
        dwh_nl_customer_ownership_flag_df = get_data_from_dwh_nl_sps[['sps_key', 'customer_ownership_flag']]
        comper_result = comper_beween_data_frame.comper_data_frame_old_and_new_data_frame(
            crm_customer_ownership_flag_df,
            dwh_nl_customer_ownership_flag_df,
            'sps_key')
        assert 1 == comper_beween_data_frame.is_test_failed(comper_result, crm_customer_ownership_flag_df,
                                                            dwh_nl_customer_ownership_flag_df)

    def test_sps_curr_panel1_wattage(self, get_data_from_hps, get_data_from_dwh_nl_sps, get_data_from_component,
                                     data_config):
        crm_sps_curr_panel1_wattage_df = get_data_from_hps[['sps_key', 'curr_panel_key']]
        get_data_from_component = get_data_from_component.rename(columns={'Id': 'curr_panel_key',
                                                                          'Panel_Wattage__c': 'curr_panel1_wattage'})
        crm_sps_curr_panel1_wattage_df = pd.merge(crm_sps_curr_panel1_wattage_df, get_data_from_component, how='left',
                                                  on='curr_panel_key')
        crm_sps_curr_panel1_wattage_df = crm_sps_curr_panel1_wattage_df[['sps_key', 'curr_panel1_wattage']]
        data_config.empty_data_frame = pd.merge(data_config.empty_data_frame, crm_sps_curr_panel1_wattage_df,
                                                how='left',
                                                on='sps_key')
        dwh_nl_sps_curr_panel1_wattage_df = get_data_from_dwh_nl_sps[['sps_key', 'curr_panel1_wattage']]
        comper_result = comper_beween_data_frame.comper_data_frame_old_and_new_data_frame(
            crm_sps_curr_panel1_wattage_df,
            dwh_nl_sps_curr_panel1_wattage_df,
            'sps_key')
        assert 1 == comper_beween_data_frame.is_test_failed(comper_result, crm_sps_curr_panel1_wattage_df,
                                                            dwh_nl_sps_curr_panel1_wattage_df)

    def test_sps_curr_panel2_wattage(self, get_data_from_hps, get_data_from_dwh_nl_sps, get_data_from_component,
                                     data_config):
        crm_sps_curr_panel2_wattage_df = get_data_from_hps[['sps_key', 'curr_panel2_key']]
        get_data_from_component = get_data_from_component.rename(columns={'Id': 'curr_panel2_key',
                                                                          'Panel_Wattage__c': 'curr_panel2_wattage'})
        crm_sps_curr_panel2_wattage_df = pd.merge(crm_sps_curr_panel2_wattage_df, get_data_from_component, how='left',
                                                  on='curr_panel2_key')
        crm_sps_curr_panel2_wattage_df = crm_sps_curr_panel2_wattage_df[['sps_key', 'curr_panel2_wattage']]
        data_config.empty_data_frame = pd.merge(data_config.empty_data_frame, crm_sps_curr_panel2_wattage_df,
                                                how='left',
                                                on='sps_key')
        dwh_nl_sps_curr_panel2_wattage_df = get_data_from_dwh_nl_sps[['sps_key', 'curr_panel2_wattage']]
        comper_result = comper_beween_data_frame.comper_data_frame_old_and_new_data_frame(
            crm_sps_curr_panel2_wattage_df,
            dwh_nl_sps_curr_panel2_wattage_df,
            'sps_key')
        assert 1 == comper_beween_data_frame.is_test_failed(comper_result, crm_sps_curr_panel2_wattage_df,
                                                            dwh_nl_sps_curr_panel2_wattage_df)

    def test_sps_full_table(self, get_data_from_hps, get_data_from_dwh_nl_sps, data_config):
        data_config.empty_data_frame = pd.merge(data_config.empty_data_frame, get_data_from_hps,
                                                how='left',
                                                on='sps_key')
        del data_config.empty_data_frame['customer_ownership_flag_y']
        comper_result = comper_beween_data_frame.comper_data_frame_old_and_new_data_frame(
            data_config.empty_data_frame,
            get_data_from_dwh_nl_sps,
            'sps_key')
        assert 1 == comper_beween_data_frame.is_test_failed(comper_result, data_config.empty_data_frame,
                                                            get_data_from_dwh_nl_sps)
