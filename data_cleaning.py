import pandas as pd
import numpy as np
#from data_extraction import DataExtracter
#from database_utils import DatabaseConnector
from validate_email import validate_email

class DataCleaning:
    #        Create a method called clean_user_data in the DataCleaning class which will perform the cleaning of the user data.

    # You will need clean the user data, look out for NULL values, errors with dates, incorrectly typed values and rows filled with the wrong information.

    def __init__(self, user_data):
        self.user_data = user_data

    def clean_user_data(self):
        print("Cleaning user data")
        self.user_data = self.user_data.dropna()
        
        self.user_data = self.clean_names('first_name')
        self.user_data = self.clean_names('last_name')
        self.user_data = self.clean_dates('date_of_birth')
        self.user_data = self.clean_emails('email_address')
        self.user_data = self.clean_phone_number('phone_number')
        self.user_data = self.clean_dates('join_date')
        self.user_data = self.clean_user_uuid('user_uuid')
         
    def clean_dates(self, col): 
        self.user_data[col] = pd.to_datetime(self.user_data[col], errors='coerce')
        #print(self.user_data.isna().any())
        self.user_data = self.user_data.dropna(subset=[col])
        return self.user_data

    def clean_names(self, col):
        self.user_data[col] = self.user_data[col].str.title()
        return self.user_data

    def clean_emails(self, col):
        self.user_data['valid_email'] = self.user_data[col].apply(lambda x: validate_email(x))
        #print(self.user_data[col].value_counts())
        self.user_data = self.user_data[self.user_data['valid_email'] == True]
        return self.user_data  
               
    def clean_phone_number(self, col):
        #check the length of the phone number is greater than 3, shortest phonenumber is 4 digits
        self.user_data = self.user_data[self.user_data[col].str.len() > 3]
        return self.user_data 

    def clean_user_uuid(self, col):
        self.user_data[col] = self.user_data[col][self.user_data[col].str.len() == 36]
        return self.user_data

if __name__ == '__main__':

    print("Running data cleaning")
    #data_extractor = DataExtracter()
    #db_connector = DatabaseConnector()
    #tables_list = db_connector.list_db_tables()
    #print("Tables in database:", tables_list)
    #clean_data = DataCleaning(
    #    data_extractor.read_rds_table(db_connector, tables_list[1]))
    #clean_data.clean_user_data()
