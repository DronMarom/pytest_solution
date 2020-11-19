import pytest

from data_base_test.data_base_querys import convert_crm_column_name_to_dwh_nl_name, crm_querys


@pytest.fixture(scope='session')
def get_data_from_crm_contact(get_connaction_to_crm):
    crm_contact_df = get_connaction_to_crm.get_data_from_salesforce(crm_querys.CRM_CONTACT)
    crm_contact_df = crm_contact_df.rename(columns=convert_crm_column_name_to_dwh_nl_name.DWH_CONTACT_NAME)
    return crm_contact_df


@pytest.fixture(scope='session')
def get_data_from_price_book(get_connaction_to_crm):
    crm_price_book_df = get_connaction_to_crm.get_data_from_salesforce(crm_querys.CRM_PRICBOOK2)
    crm_price_book_df = crm_price_book_df.rename(columns=convert_crm_column_name_to_dwh_nl_name.DWH_PRICBOOK_NAME)
    return crm_price_book_df


@pytest.fixture(scope='session')
def get_data_from_users(get_connaction_to_crm):
    crm_users_df = get_connaction_to_crm.get_data_from_salesforce(crm_querys.CRM_USER)
    crm_users_df = crm_users_df.rename(columns=convert_crm_column_name_to_dwh_nl_name.DWH_USER_NAME)
    return crm_users_df


@pytest.fixture(scope='session')
def get_data_from_hps(get_connaction_to_crm):
    crm_hps_df = get_connaction_to_crm.get_data_from_salesforce(crm_querys.CRM_HPS)
    crm_hps_df = crm_hps_df.rename(columns=convert_crm_column_name_to_dwh_nl_name.DWH_SPS_NAME)
    return crm_hps_df


@pytest.fixture(scope='session')
def get_data_from_component(get_connaction_to_crm):
    crm_component_df = get_connaction_to_crm.get_data_from_salesforce(crm_querys.CRM_COMPONENT)
    return crm_component_df
