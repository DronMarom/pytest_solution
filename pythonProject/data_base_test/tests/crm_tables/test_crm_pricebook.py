import os

import numpy as np
import pandas as pd
from pytest import mark

from data_base_test.helper_fanctions import comper_beween_data_frame
from log import log_file


@mark.crm_table
@mark.price_book
@mark.full_run
class price_book_Test:


    @staticmethod
    def test_crm_price_book_ecommerce_flag(get_data_from_dwh_nl_pricebook, get_data_from_price_book, data_config):
        os.environ['TEST_NAME'] = os.environ.get(
            'PYTEST_CURRENT_TEST').split(':')[-1].split(' ')[0]
        crm_ecommerce_flag_df = get_data_from_price_book[['price_book_key', 'ecommerce_flag']]
        dwh_nl_ecommerce_flag_df = get_data_from_dwh_nl_pricebook[['price_book_key', 'ecommerce_flag']]
        conditions = [crm_ecommerce_flag_df['ecommerce_flag'] ==True,
                      crm_ecommerce_flag_df['ecommerce_flag'] == False]
        choices = [1, 0]
        crm_ecommerce_flag_df['ecommerce_flag'] = np.select(conditions, choices, default=0)
        data_config.empty_data_frame = crm_ecommerce_flag_df
        comper_result = comper_beween_data_frame.comper_data_frame_old_and_new_data_frame(crm_ecommerce_flag_df,
                                                                                          dwh_nl_ecommerce_flag_df,
                                                                                          'price_book_key')
        write_result_to_log = log_file.setup_logger()
        write_result_to_log.error(comper_result)

        assert 1 == comper_beween_data_frame.is_test_failed(comper_result, crm_ecommerce_flag_df,
                                                            dwh_nl_ecommerce_flag_df)

    @staticmethod
    def test_crm_price_book_created_user_name(get_data_from_dwh_nl_pricebook, get_data_from_price_book,
                                              get_data_from_users, data_config):
        os.environ['TEST_NAME'] = os.environ.get(
            'PYTEST_CURRENT_TEST').split(':')[-1].split(' ')[0]
        crm_created_user_name_df = get_data_from_price_book[['price_book_key', 'price_book_created_user_id']]
        crm_users_df = get_data_from_users[['user_key', 'username']]
        crm_users_df = crm_users_df.rename(
            columns={'user_key': 'price_book_created_user_id', 'username': 'price_book_created_user_name'})
        crm_created_user_name_df = pd.merge(crm_created_user_name_df, crm_users_df, how='left',
                                            on='price_book_created_user_id')
        crm_created_user_name_df = crm_created_user_name_df[['price_book_key', 'price_book_created_user_name']]
        dwh_nl_created_user_name_df = get_data_from_dwh_nl_pricebook[
            ['price_book_key', 'price_book_created_user_name']]
        data_config.empty_data_frame = pd.merge(data_config.empty_data_frame, crm_created_user_name_df, how='left',
                                                on='price_book_key')
        comper_result = comper_beween_data_frame.comper_data_frame_old_and_new_data_frame(crm_created_user_name_df,
                                                                                          dwh_nl_created_user_name_df,
                                                                                          'price_book_key')
        write_result_to_log = log_file.setup_logger()
        write_result_to_log.error(comper_result)
        assert 1 == comper_beween_data_frame.is_test_failed(comper_result, crm_created_user_name_df,
                                                            dwh_nl_created_user_name_df)

    @staticmethod
    def test_crm_price_book_updated_user_name(get_data_from_dwh_nl_pricebook, get_data_from_price_book,
                                              get_data_from_users, data_config):
        os.environ['TEST_NAME'] = os.environ.get(
            'PYTEST_CURRENT_TEST').split(':')[-1].split(' ')[0]
        crm_created_user_name_df = get_data_from_price_book[['price_book_key', 'price_book_updated_user_id']]
        crm_users_df = get_data_from_users[['user_key', 'username']]
        crm_users_df = crm_users_df.rename(
            columns={'user_key': 'price_book_updated_user_id', 'username': 'price_book_updated_user_name'})
        crm_created_user_name_df = pd.merge(crm_created_user_name_df, crm_users_df, how='left',
                                            on='price_book_updated_user_id')
        crm_created_user_name_df = crm_created_user_name_df[['price_book_key', 'price_book_updated_user_name']]
        data_config.empty_data_frame = pd.merge(data_config.empty_data_frame, crm_created_user_name_df, how='left',
                                                on='price_book_key')
        dwh_nl_created_user_name_df = get_data_from_dwh_nl_pricebook[
            ['price_book_key', 'price_book_updated_user_name']]
        comper_result = comper_beween_data_frame.comper_data_frame_old_and_new_data_frame(crm_created_user_name_df,
                                                                                          dwh_nl_created_user_name_df,
                                                                                          'price_book_key')
        write_result_to_log = log_file.setup_logger()
        write_result_to_log.error(comper_result)
        assert 1 == comper_beween_data_frame.is_test_failed(comper_result, crm_created_user_name_df,
                                                            dwh_nl_created_user_name_df)

    @staticmethod
    def test_crm_price_book_full_table(get_data_from_dwh_nl_pricebook, get_data_from_price_book, data_config):
        os.environ['TEST_NAME'] = os.environ.get(
            'PYTEST_CURRENT_TEST').split(':')[-1].split(' ')[0]
        data_config.empty_data_frame = pd.merge(data_config.empty_data_frame, get_data_from_price_book, how='left',
                                                on='price_book_key')
        del data_config.empty_data_frame['ecommerce_flag_y']
        comper_result = comper_beween_data_frame.comper_data_frame_old_and_new_data_frame(
            get_data_from_dwh_nl_pricebook,
            data_config.empty_data_frame,
            'price_book_key')

        write_result_to_log = log_file.setup_logger()
        write_result_to_log.error(comper_result)
        assert 1 == comper_beween_data_frame.is_test_failed(comper_result, data_config.empty_data_frame,
                                                            get_data_from_dwh_nl_pricebook)
