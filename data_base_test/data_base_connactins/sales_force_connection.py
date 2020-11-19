import pandas as pd
from simple_salesforce import Salesforce



class DataFromSF:
    sf = None

    def __init__(self, **SFENV):
        self.sf = Salesforce(**SFENV)

    def get_data_from_salesforce(self, sql_query):

        get_records_from_salesforce = self.sf.query_all(sql_query)
        records = get_records_from_salesforce['records']
        df1 = pd.DataFrame(records)
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
