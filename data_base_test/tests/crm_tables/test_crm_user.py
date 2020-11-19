import pytest
from pytest import mark
from data_base_test.helper_fanctions import comper_beween_data_frame
from data_base_test.data_base_querys import snowflake_querys, dwh_nl_querys, crm_querys, \
    convert_crm_column_name_to_dwh_nl_name

@mark.crm_table
@mark.users
@mark.full_run
class  crm_user_Test():
    def test_contact_contact_user_state(self,get_data_from_crm_contact,get_data_from_dwh_nl_contact):
        crm_contact_df=get_data_from_crm_contact[['contact_key','contact_user_state']]
        dwh_nl_contact_df=get_data_from_dwh_nl_contact[['contact_key','contact_user_state']]
        comper_result=comper_beween_data_frame.comper_data_frame_old_and_new_data_frame(crm_contact_df, dwh_nl_contact_df, 'contact_key')
        assert 1==comper_beween_data_frame.is_test_failed(comper_result, crm_contact_df, dwh_nl_contact_df)