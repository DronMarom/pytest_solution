import os

import pandas as pd
import numpy as np
from pytest import mark
from data_base_test.helper_fanctions import comper_beween_data_frame
from log import log_file


@mark.crm_table
@mark.lead
@mark.full_run
class crm_lead_Test:
    @staticmethod
    def test_contact_phone_number_verified_flag(get_data_from_crm_lead, get_data_from_dwh_nl_lead, data_config):
        os.environ['TEST_NAME'] = os.environ.get(
            'PYTEST_CURRENT_TEST').split(':')[-1].split(' ')[0]
        contact_phone_number_verified_flag_crm_df = get_data_from_crm_lead[
            ['lead_key', 'contact_phone_number_verified_flag']]
        conditions = [
            (contact_phone_number_verified_flag_crm_df['contact_phone_number_verified_flag'] == True),
            (contact_phone_number_verified_flag_crm_df['contact_phone_number_verified_flag'] != True)
        ]
        choices = ['1', '0']
        contact_phone_number_verified_flag_crm_df['contact_phone_number_verified_flag'] = np.select(conditions, choices,
                                                                                                    default='Unknown')
        contact_phone_number_verified_flag_dwh_nl_df = get_data_from_dwh_nl_lead[
            ['lead_key', 'contact_phone_number_verified_flag']]
        data_config.empty_data_frame = contact_phone_number_verified_flag_dwh_nl_df

        dwh_nl_lead_df = get_data_from_dwh_nl_lead[['lead_key', 'contact_phone_number_verified_flag']]
        comper_result = comper_beween_data_frame.comper_data_frame_old_and_new_data_frame(
            contact_phone_number_verified_flag_dwh_nl_df,
            dwh_nl_lead_df,
            'lead_key')
        write_result_to_log = log_file.setup_logger()
        write_result_to_log.error(comper_result)

        assert 1 == comper_beween_data_frame.is_test_failed(comper_result, contact_phone_number_verified_flag_dwh_nl_df,
                                                            dwh_nl_lead_df)

    @staticmethod
    def test_lead_agent_sub_channel(get_data_from_crm_lead, get_data_from_dwh_nl_lead, get_data_from_crm_contact,
                                    data_config):
        os.environ['TEST_NAME'] = os.environ.get(
            'PYTEST_CURRENT_TEST').split(':')[-1].split(' ')[0]

        get_data_from_crm_contact_local = get_data_from_crm_contact[['contact_key', 'lead_agent_sub_channel']]
        get_data_from_crm_contact_local = get_data_from_crm_contact_local.rename(columns={'contact_key':
                                                                                              'lead_agent_contact_key'
                                                                                          })
        get_data_from_crm_lead_local = get_data_from_crm_lead[['lead_key', 'lead_agent_contact_key']]
        get_data_from_crm_lead_local = pd.merge(get_data_from_crm_lead_local, get_data_from_crm_contact_local,
                                                how='left', on='lead_agent_contact_key')
        del get_data_from_crm_lead_local['lead_agent_contact_key']
        data_config.empty_data_frame = pd.merge(data_config.empty_data_frame, get_data_from_crm_lead_local, how='left',
                                                on='lead_key')
        get_data_from_dwh_nl_lead_local = get_data_from_dwh_nl_lead[['lead_key', 'lead_agent_sub_channel']]
        comper_result = comper_beween_data_frame.comper_data_frame_old_and_new_data_frame(
            get_data_from_crm_lead_local,
            get_data_from_dwh_nl_lead_local,
            'lead_key')
        write_result_to_log = log_file.setup_logger()
        write_result_to_log.error(comper_result)

        assert 1 == comper_beween_data_frame.is_test_failed(comper_result, get_data_from_crm_lead_local,
                                                            get_data_from_dwh_nl_lead_local)

    @staticmethod
    def test_crm_lead_full_table(get_data_from_crm_lead, get_data_from_dwh_nl_lead, data_config):
        os.environ['TEST_NAME'] = os.environ.get(
            'PYTEST_CURRENT_TEST').split(':')[-1].split(' ')[0]
        data_config.empty_data_frame = pd.merge(data_config.empty_data_frame, get_data_from_crm_lead, how='left',
                                                on='lead_key')
        comper_result = comper_beween_data_frame.comper_data_frame_old_and_new_data_frame(
            data_config.empty_data_frame,
            get_data_from_dwh_nl_lead,
            'lead_key')
        write_result_to_log = log_file.setup_logger()
        write_result_to_log.error(comper_result)

        assert 1 == comper_beween_data_frame.is_test_failed(comper_result, data_config.empty_data_frame,
                                                            get_data_from_dwh_nl_lead)

