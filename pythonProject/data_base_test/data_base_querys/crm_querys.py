CRM_CONTACT = '''SELECT AccountId,Id,State__c,Sales_Agent_sub_Channel__c
 FROM Contact'''
CRM_USER = '''select Id,Customer_Subscription_Method__c,
Profile_Name__c,
Name from User'''
CRM_PRICBOOK2 = '''select Id,Tenant_SIM__c,Ecommerce__c,Generation__c,Sales_Channel__c,Unit_To_Bundle_Combo__c,Type__c,
Name,
CreatedDate,
LastModifiedDate,
IsActive,
IsDeleted,
Partner__c,
CreatedById,
LastModifiedById
from Pricebook2 '''
CRM_PRICBOOK2_ENTRY = '''select
id,
Name,
Price_Description__c,
Pricebook2Id,
Product2Id,
UnitPrice,
Price_Description2__c,
IsActive,
IsDeleted,
Joining_Fee__c,
Last_Month__c,
Security_Deposit__c,
Discounted_App__c,
CreatedById,
LastModifiedById,
Paid_To_Store__c,
Paid_In_Advance_To_Lumos__c,
MTN_Id__c,
CurrencyIsoCode,
Basic_Warranty_Period__c
FROM PricebookEntry
'''
CRM_INSTALLATION_C = '''SELECT Offer__c,
Owned_By__c,
RecordTypeId,
Purchase_Plan__c,
Price_Book__c,
Partner__c,
Id,
Rate_Plan__c,
Opportunity__c,
Generation__c
FROM Installation__c'''

CRM_CONTRACT_ASSET = '''SELECT
Active__c,
Contract__c,
Name,
CreatedById,
CreatedDate,
CurrencyIsoCode,
Customer_Details__c,
DCAC_Type__c,
Product_Number__c,
DCAC_Serial_Number__c,
IsDeleted,
From_Offer__c,
Is_There_a_Warranty__c,
LastModifiedById,
LastModifiedDate,
Panel__c,
Partner_ID__c,
Product__c,
Product_Brand__c,
Product_Catalog_ID__c,
Product_Description__c,
Product_Version__c,
Id,
SystemModstamp,
Type__c,
Verified_by_Lumos__c,
Warranty_End_Date__c,
Warranty_Month__c,
What_in_the_Box__c
FROM Contract_Asset__c'''

CRM_RECORDTYPE = '''select Id,Name from RecordType'''

CRM_PRODUCTS2 = '''SELECT Unit_Name__c,
Unit_Type__c,
Name,
IsActive,
Adaptors__c,
Admin_Switch__c,
Billable__c,
Connectors__c,
CreatedById,
CreatedDate,
Currency__c,
CurrencyIsoCode,
IsDeleted,
ERP_Part_Number__c,
Full_SFID__c,
Image__c,
LastModifiedById,
LastModifiedDate,
Lumos_P_N__c,
MTN_Id__c,
Months__c,
Number_of_Panels__c,
Number_of_days__c,
Parent_Product__c,
Parent_Product_Description__c,
Parent_Product_Name__c,
Period_To_Own__c,
Period_To_Own_Type__c,
Product_Brand__c,
Product_Catalog_ID__c,
ProductCode,
Description,
Family,
Id,
Product_Version__c,
Purchase_Plan__c,
SIM__c,
Sales_Channel__c,
SystemModstamp,
Tenant_SIM__c,
Usage_Period__c,
Usage_Period_Type__c,
What_in_the_Box__c
FROM Product2'''
CRM_LEAD = '''select Id,
Type__c,
LeadSource,
Partner__c,
Status,
CreatedDate,
Name,
Salutation,
FirstName,
LastName,
Category__c,
Phone,
Country__c,
State__c,
City_formula__c,
Street__c,
Email,
Prefered_language__c,
CurrencyIsoCode,
IsConverted,
ConvertedDate,
ConvertedAccountId,
ConvertedContactId,
ConvertedOpportunityId,
RecordTypeId,
OwnerId,
CreatedById,
LastModifiedById,
LastModifiedDate,
IsDeleted,
Agent_Name__c,
Contact_phone_number_verified__c,
I_confirm_that_Guarantor_is_approved__c,
Welcome_Call_Preformed__c,Sub_Channel__c,
Sales_Agent__c
 from Lead'''

CRM_COMPONENT = '''select Id,Panel_Wattage__c from Component__c where Type__c='Panel' '''

CRM_HPS = '''SELECT Id,
Partner__c,
Product_Name__c,
Product__c,
installation__c,
Tenant_SIM__c,
Panel__c,
Panel2__c,
Panel_2_SN__c ,
Customer_Ownership__c,
Payment_Method_Allowed__c,
Subtype__c
FROM HPS__c'''
