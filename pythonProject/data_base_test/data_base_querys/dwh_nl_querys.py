
import os
RELEVANCE_DATE = os.environ.get('RELEVANCE_DATE')
DWH_NL_CONTACT = '''SELECT * from dwh_nl.contact'''
SRC_DATA = '''select id as payment_id,total_amount,Fee as charged_amount,tenant_name,crm_owner as payment_crm_owner,
RecordCreationTimestamp,payment_method as provider_name from dwh_src.src_SyncOrderRelationInput_payments_LMS '''
STG_DATA = '''select payment_id,payment_total_amount,charged_amount,tenant_name,payment_crm_created_by as 
payment_crm_owner,commission_paid_by_customer,commission_paid_by_lumos,commission_liability,
commission_paid_by_lumos_including_vat,net_amount  from dwh_stg.stg_payments_commission '''
LUMOS_COMMISSION_PAYMENT_MNG = '''select * from dwh_mng.lumos_commission_payment_mng'''
TENANT_TABLE = '''select tenant_name,partner_key,erp_company_name as erp_company from dwh_nl.tenant '''
ERP_TAX_RATE = '''select erp_company,from_date as tax_rate_from_date,to_date as tax_rate_to_date,tax_rate_1 from 
dwh_erp.tax_rate where tax_key_erp=-2 and tax_group_key = -2 '''
DWH_NL_CRM_USERS = '''select * from dwh_nl.crm_users'''
DWH_NL_CRM_PRICEBOOK = '''select * from dwh_nl.price_book'''
DWH_NL_SPS = '''select sps_key, customer_ownership_flag,curr_panel1_wattage,curr_panel2_wattage from dwh_nl.sps '''
DWH_NL_SPS_FOR_SPS_DAILY = '''select sps_short_num,curr_panel1_wattage,curr_panel2_wattage from dwh_nl.sps '''
DWH_NL_SPS_DAILY = '''select sps_short_num,curr_panel1_wattage,curr_panel2_wattage,curr_warehouse_bin from dwh_nl.sps_daily 
where relevance_date = {RELEVANCE_DATE} '''
DWH_STG_SPS_DAILY_VW = '''select system_id as sps_short_num,bin_name from dwh_stg.stg_sps_daily_vw'''
DWH_NL_CRM_LEAD = '''select * from dwh_nl.lead'''
DWH_NL_CONTRACT = '''select * from dwh_nl.contract'''
DWH_NL_CONTRACT_DAILY = '''select contract_key,sales_channel_type,contract_activity_status_cd,
contract_activity_status from dwh_stg.stg_cntrct_daily '''
DWH_NL_INVENTORY_IDU = '''select system_id as sps_short_num,brand,erp_company_name from 
dwh_stg.stg_sps_daily_inventory_idu '''
DWH_NL_TENANT = '''select 
partner_desc,
partner_key,
partner_short_name,
sps_daily_logistics_usd
FROM tenant '''

VW_REF_WAREHOUSE_TERRITORY = '''
select * from dwh_erp.ref_warehouse_txtligh
UNION
select * from dwh_erp.ref_warehouse_ivory
UNION
select * from dwh_erp.ref_warehouse_nova
UNION
select * from dwh_erp.ref_warehouse_spshk'''

VW_REF_SN_IDU_TERRITORY = '''select `dwh_erp`.`ref_sn_txtligh`.`erp_company` AS `erp_company`,`dwh_erp`.`ref_sn_txtligh`.`serial_number` 
AS `serial_number`,`dwh_erp`.`ref_sn_txtligh`.`update_date` AS `update_date`,
`dwh_erp`.`ref_sn_txtligh`.`warehouse_name` AS `warehouse_name`,`dwh_erp`.`ref_sn_txtligh`.`warehouse_desc` AS 
`warehouse_desc`,`dwh_erp`.`ref_sn_txtligh`.`always_on_change_date` AS `always_on_change_date`,
`dwh_erp`.`ref_sn_txtligh`.`refurbished_flag` AS `refurbished_flag`,`dwh_erp`.`ref_sn_txtligh`.`calculated_part_name` 
AS `calculated_part_name`,`dwh_erp`.`ref_sn_txtligh`.`part_sub_family_name` AS `part_sub_family_name`,
`dwh_erp`.`ref_sn_txtligh`.`warehouse_key` AS warehouse_key 
from 
`dwh_erp`.`ref_sn_txtligh` where (`dwh_erp`.`ref_sn_txtligh`.`part_family_name` = 'IDU') 
union all select 
`dwh_erp`.`ref_sn_ivory`.`erp_company` AS `erp_company`,`dwh_erp`.`ref_sn_ivory`.`serial_number` AS `serial_number`,
`dwh_erp`.`ref_sn_ivory`.`update_date` AS `update_date`,`dwh_erp`.`ref_sn_ivory`.`warehouse_name` AS 
`warehouse_name`,`dwh_erp`.`ref_sn_ivory`.`warehouse_desc` AS `warehouse_desc`,
`dwh_erp`.`ref_sn_ivory`.`always_on_change_date` AS `always_on_change_date`,
`dwh_erp`.`ref_sn_ivory`.`refurbished_flag` AS `refurbished_flag`,`dwh_erp`.`ref_sn_ivory`.`calculated_part_name` AS 
`calculated_part_name`,`dwh_erp`.`ref_sn_ivory`.`part_sub_family_name` AS `part_sub_family_name`, 
`dwh_erp`.`ref_sn_ivory`.`warehouse_key` AS warehouse_key 
from 
`dwh_erp`.`ref_sn_ivory` where (`dwh_erp`.`ref_sn_ivory`.`part_family_name` = 'IDU'); '''
