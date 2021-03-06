import time

import pandas as pd
from simple_salesforce import Salesforce


class DataFromSF:
    sf = None

    def __init__(self, **SFENV):

            try:
                self.sf = Salesforce(**SFENV)
            except:
                print ('get connection fialed')

    def get_data_from_salesforce(self, sql_query, write_result_to_log):
        for i in range(0, 5):
            try:
                get_records_from_salesforce = self.sf.query_all(sql_query)
            except Exception as e:
                time.sleep(20)
                if i == 4:
                    write_result_to_log.error(f'Error when get data from CRM Error message : {e.args}')
                continue
            break

        records = get_records_from_salesforce['records']
        df1 = pd.DataFrame(records)
        if df1.shape[0] != 0:
            del df1['attributes']
        return df1

    def update_salesforce_rows(self, salesforce_id, tableName, **kwargs):

        if tableName == 'PricebookEntry':
            self.sf.PricebookEntry.update(salesforce_id, kwargs)
        elif tableName == 'opportunity':
            self.sf.Opportunity.update(salesforce_id, kwargs)
        elif tableName == 'OpportunityLineItem':
            self.sf.OpportunityLineItem.update(salesforce_id, kwargs)
        elif tableName == 'Installation__c':
            self.sf.Installation__c.update(salesforce_id, kwargs)
