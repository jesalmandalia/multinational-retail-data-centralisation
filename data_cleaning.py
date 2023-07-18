import pandas as pd
import numpy as np
#from data_extraction import DataExtracter
#from database_utils import DatabaseConnector
from validate_email import validate_email

class DataCleaning:
    #        Create a method called clean_data in the DataCleaning class which will perform the cleaning of the user data.
    def __init__(self, data):
        self.data = data

    def clean_user_data(self):
        print("Cleaning user data")
        self.data = self.data.dropna() 
        self.data = self.clean_names('first_name')
        self.data = self.clean_names('last_name')
        self.data = self.clean_dates('date_of_birth')
        self.data = self.clean_emails('email_address')
        self.data = self.clean_phone_number('phone_number')
        self.data = self.clean_dates('join_date')
        self.data = self.clean_user_uuid('user_uuid')
         
    def clean_dates(self, col): 
        self.data[col] = pd.to_datetime(self.data[col], errors='coerce')
       # print(self.data.isna().any())
        self.data = self.data.dropna(subset=[col])
        #print(self.data.isna().any())
        return self.data

    def clean_names(self, col):
        self.data[col] = self.data[col].str.title()
        return self.data

    def clean_emails(self, col):
        self.data['valid_email'] = self.data[col].apply(lambda x: validate_email(x))
        #print(self.data[col].value_counts())
        self.data = self.data[self.data['valid_email'] == True]
        return self.data  
               
    def clean_phone_number(self, col):
        #check the length of the phone number is greater than 3, shortest phonenumber is 4 digits
        self.data = self.data[self.data[col].str.len() > 3]
        return self.data 

    def clean_user_uuid(self, col):
        self.data[col] = self.data[col][self.data[col].str.len() == 36]
        return self.data

    #Create a method called clean_data in your DataCleaning class to clean the data to remove any erroneous values, NULL values or errors with formatting.
    def clean_card_data(self):
        print("Cleaning card data")
       # print(self.data.isna().any())
        self.data = self.data.dropna()
        self.data = self.clean_dates('date_payment_confirmed')

if __name__ == '__main__':

    print("Running data cleaning")
    #data_extractor = DataExtracter()
    #db_connector = DatabaseConnector()
    #tables_list = db_connector.list_db_tables()
    #print("Tables in database:", tables_list)
    #clean_data = DataCleaning(
    #    data_extractor.read_rds_table(db_connector, tables_list[1]))
    #clean_data.clean_data()
