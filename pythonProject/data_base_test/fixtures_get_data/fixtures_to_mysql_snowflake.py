import pytest

from data_base_test.data_base_querys import snowflake_querys, dwh_nl_querys


@pytest.fixture(scope='session')
def get_data_from_dwh_nl_contact(get_object_according_to_data_base_type, data_config):
    if 'snowflake' in data_config.dwh_nl_data_base:
        connection_to_snowflake = get_object_according_to_data_base_type.get_connection_to_snowflake()
        contact_df = get_object_according_to_data_base_type.run_query(snowflake_querys.CONTACT_TABLE_FOR_CRM_USER_TEST,
                                                                      connection_to_snowflake)
        contact_df.columns = contact_df.columns.str.strip().str.lower()

        return contact_df
    else:
        dwh_nl_contact_df = get_object_according_to_data_base_type.get_connection_to_mysql(dwh_nl_querys.DWH_NL_CONTACT)
        return dwh_nl_contact_df


@pytest.fixture(scope='session')
def get_data_from_dwh_nl_lead(get_object_according_to_data_base_type, data_config):
    if 'snowflake' in data_config.dwh_nl_data_base:
        connection_to_snowflake = get_object_according_to_data_base_type.get_connection_to_snowflake()
        lead_df = get_object_according_to_data_base_type.run_query(snowflake_querys.DWH_NL_LEAD,
                                                                      connection_to_snowflake)
        lead_df.columns = lead_df.columns.str.strip().str.lower()

        return lead_df
    else:
        dwh_nl_lead_df = get_object_according_to_data_base_type.get_connection_to_mysql(dwh_nl_querys.DWH_NL_CRM_LEAD)
        return dwh_nl_lead_df


@pytest.fixture(scope='session')
def get_data_from_dwh_nl_users(get_object_according_to_data_base_type, data_config):
    if 'snowflake' in data_config.dwh_nl_data_base:
        connection_to_snowflake = get_object_according_to_data_base_type.get_connection_to_snowflake()
        crm_users_df = get_object_according_to_data_base_type.run_query(snowflake_querys.DWH_NL_CRM_USERS,
                                                                        connection_to_snowflake)
        crm_users_df.columns = crm_users_df.columns.str.strip().str.lower()

        return crm_users_df
    else:
        dwh_nl_crm_users_df = get_object_according_to_data_base_type.get_connection_to_mysql(
            dwh_nl_querys.DWH_NL_CRM_USERS)
        return dwh_nl_crm_users_df


@pytest.fixture(scope='session')
def get_data_from_dwh_nl_pricebook(get_object_according_to_data_base_type, data_config):
    if 'snowflake' in data_config.dwh_nl_data_base:
        connection_to_snowflake = get_object_according_to_data_base_type.get_connection_to_snowflake()
        dwh_nl_price_book_df = get_object_according_to_data_base_type.run_query(snowflake_querys.DWH_NL_CRM_PRICEBOOK,
                                                                                connection_to_snowflake)
        dwh_nl_price_book_df.columns = dwh_nl_price_book_df.columns.str.strip().str.lower()

        return dwh_nl_price_book_df
    else:
        dwh_nl_price_book_df = get_object_according_to_data_base_type.get_connection_to_mysql(
            dwh_nl_querys.DWH_NL_CRM_PRICEBOOK)
        return dwh_nl_price_book_df


@pytest.fixture(scope='session')
def get_data_from_dwh_nl_sps(get_object_according_to_data_base_type, data_config):
    if 'snowflake' in data_config.dwh_nl_data_base:
        connection_to_snowflake = get_object_according_to_data_base_type.get_connection_to_snowflake()
        dwh_nl_sps_df = get_object_according_to_data_base_type.run_query(snowflake_querys.DWH_NL_SPS,
                                                                         connection_to_snowflake)
        dwh_nl_sps_df.columns = dwh_nl_sps_df.columns.str.strip().str.lower()

        return dwh_nl_sps_df
    else:
        dwh_nl_sps_df = get_object_according_to_data_base_type.get_connection_to_mysql(
            dwh_nl_querys.DWH_NL_SPS)
        return dwh_nl_sps_df


@pytest.fixture(scope='session')
def get_data_from_dwh_nl_tenant(get_object_according_to_data_base_type, data_config):
    # if 'snowflake' in data_config.dwh_nl_data_base:
    #     connection_to_snowflake = get_object_according_to_data_base_type.get_connection_to_snowflake()
    #     dwh_nl_sps_df = get_object_according_to_data_base_type.run_query(snowflake_querys.DWH_NL_SPS,
    #                                                                      connection_to_snowflake)
    #     dwh_nl_sps_df.columns = dwh_nl_sps_df.columns.str.strip().str.lower()
    #
    #     return dwh_nl_sps_df

    dwh_nl_tenant_df = get_object_according_to_data_base_type.get_connection_to_mysql(
        dwh_nl_querys.DWH_NL_TENANT)
    return dwh_nl_tenant_df


@pytest.fixture(scope='session')
def get_data_from_dwh_erp_vw_ref_sn_idu_territory(get_object_according_to_data_base_type, data_config):
    # if 'snowflake' in data_config.dwh_nl_data_base:
    #     connection_to_snowflake = get_object_according_to_data_base_type.get_connection_to_snowflake()
    #     dwh_nl_sps_df = get_object_according_to_data_base_type.run_query(snowflake_querys.DWH_NL_SPS,
    #                                                                      connection_to_snowflake)
    #     dwh_nl_sps_df.columns = dwh_nl_sps_df.columns.str.strip().str.lower()
    #
    #     return dwh_nl_sps_df

    dwh_erp_vw_ref_sn_idu_df = get_object_according_to_data_base_type.get_connection_to_mysql(
        dwh_nl_querys.VW_REF_SN_IDU_TERRITORY)
    return dwh_erp_vw_ref_sn_idu_df


@pytest.fixture(scope='session')
def get_data_from_dwh_erp_vw_ref_warehouse_territory(get_object_according_to_data_base_type, data_config):
    # if 'snowflake' in data_config.dwh_nl_data_base:
    #     connection_to_snowflake = get_object_according_to_data_base_type.get_connection_to_snowflake()
    #     dwh_nl_sps_df = get_object_according_to_data_base_type.run_query(snowflake_querys.DWH_NL_SPS,
    #                                                                      connection_to_snowflake)
    #     dwh_nl_sps_df.columns = dwh_nl_sps_df.columns.str.strip().str.lower()
    #
    #     return dwh_nl_sps_df

    dwh_erp_vw_ref_warehouse_territory_df = get_object_according_to_data_base_type.get_connection_to_mysql(
        dwh_nl_querys.VW_REF_WAREHOUSE_TERRITORY)
    return dwh_erp_vw_ref_warehouse_territory_df


def get_data_from_dwh_nl_sps_daily(get_object_according_to_data_base_type, data_config):
    # if 'snowflake' in data_config.dwh_nl_data_base:
    #     connection_to_snowflake = get_object_according_to_data_base_type.get_connection_to_snowflake()
    #     dwh_nl_sps_df = get_object_according_to_data_base_type.run_query(snowflake_querys.DWH_NL_SPS,
    #                                                                      connection_to_snowflake)
    #     dwh_nl_sps_df.columns = dwh_nl_sps_df.columns.str.strip().str.lower()
    #
    #     return dwh_nl_sps_df

    dwh_nl_sps_daily_df = get_object_according_to_data_base_type.get_connection_to_mysql(
        dwh_nl_querys.DWH_NL_SPS_DAILY)
    return dwh_nl_sps_daily_df
