import os

import pandas as pd
from pytest import mark
from data_base_test.helper_fanctions import comper_beween_data_frame
from log import log_file


@mark.dwh_nl_table
@mark.contract
@mark.full_run
class contract_Test:
    @staticmethod
    def test_sales_agent_sub_channel_and_contact_key(get_data_from_crm_lead, get_data_from_crm_contact,
                                                     get_data_from_installation, get_data_from_dwh_nl_contract,
                                                     data_config):
        os.environ['TEST_NAME'] = os.environ.get(
            'PYTEST_CURRENT_TEST').split(':')[-1].split(' ')[0]

        get_data_from_crm_contact_local = get_data_from_crm_contact[['contact_key', 'lead_agent_sub_channel']]
        get_data_from_crm_contact_local = get_data_from_crm_contact_local.rename(columns={'contact_key':
                                                                                              'lead_agent_contact_key'
                                                                                          })

        get_data_from_crm_lead_local = get_data_from_crm_lead[
            ['lead_key', 'lead_agent_contact_key', 'lead_converted_opportunity_key']]
        get_data_from_crm_lead_local = pd.merge(get_data_from_crm_lead_local, get_data_from_crm_contact_local,
                                                how='left', on='lead_agent_contact_key')
        get_data_from_crm_lead_local = get_data_from_crm_lead_local.rename(columns={'lead_agent_contact_key':
                                                                                        'sales_agent_contact_key',
                                                                                    'lead_agent_sub_channel':
                                                                                        'sales_agent_sub_channel'})
        get_data_from_installation_local = get_data_from_installation[['contract_key', 'contract_opportunity_key']]
        get_data_from_installation_local = get_data_from_installation_local.rename(columns={'contract_opportunity_key':
                                                                                                'lead_converted_opportunity_key'})
        get_data_from_crm_lead_local = get_data_from_crm_lead_local.fillna('-9')
        get_data_from_installation_local = pd.merge(get_data_from_installation_local, get_data_from_crm_lead_local,
                                                    how='left',
                                                    on='lead_converted_opportunity_key')
        data_config.empty_data_frame = get_data_from_installation_local
        get_data_from_dwh_nl_contract_local = get_data_from_dwh_nl_contract[['contract_key', 'sales_agent_contact_key',
                                                                             'sales_agent_sub_channel']]
        comper_result = comper_beween_data_frame.comper_data_frame_old_and_new_data_frame(
            get_data_from_installation_local,

            get_data_from_dwh_nl_contract_local,
            'contract_key')
        write_result_to_log = log_file.setup_logger()
        write_result_to_log.error(comper_result)

        assert 1 == comper_beween_data_frame.is_test_failed(comper_result, get_data_from_installation_local,
                                                            get_data_from_dwh_nl_contract_local)
