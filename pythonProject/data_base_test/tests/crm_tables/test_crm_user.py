import pytest
from pytest import mark
from data_base_test.helper_fanctions import comper_beween_data_frame
from data_base_test.data_base_querys import snowflake_querys, dwh_nl_querys, crm_querys, \
    convert_crm_column_name_to_dwh_nl_name
from log import log_file

@mark.crm_table
@mark.users
@mark.full_run
class  crm_user_Test():
    # def test_contact_contact_user_state(self,get_data_from_crm_contact,get_data_from_dwh_nl_contact,data_config):
    #     crm_contact_df=get_data_from_crm_contact[['contact_key','contact_user_state']]
    #     dwh_nl_contact_df=get_data_from_dwh_nl_contact[['contact_key','contact_user_state']]
    #     data_config.empty_data_frame = dwh_nl_contact_df
    #     comper_result=comper_beween_data_frame.comper_data_frame_old_and_new_data_frame(crm_contact_df, dwh_nl_contact_df, 'contact_key')
    #     assert 1==comper_beween_data_frame.is_test_failed(comper_result, crm_contact_df, dwh_nl_contact_df)

    def test_full_table_crm_users(self, get_data_from_dwh_nl_users, get_data_from_users, data_config):
        get_data_from_users = get_data_from_users.rename(columns={'ContactId':'contact_key'})
        data_config.empty_data_frame = pd.merge(data_config.empty_data_frame, get_data_from_users, how='left',on='contact_key')
        comper_result = comper_beween_data_frame.comper_data_frame_old_and_new_data_frame(data_config.empty_data_frame,
                                                                                          get_data_from_dwh_nl_users,
                                                                                          'user_key')
        test=log_file.setup_logger()
        test.log('comper_result')

        assert 1 == comper_beween_data_frame.is_test_failed(comper_result, data_config.empty_data_frame, get_data_from_dwh_nl_users)
