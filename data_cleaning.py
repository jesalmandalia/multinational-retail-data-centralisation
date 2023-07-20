import pandas as pd
import numpy as np
#from data_extraction import DataExtracter
#from database_utils import DatabaseConnector
from validate_email import validate_email
pd.options.mode.chained_assignment = None  # default='warn'
import uuid

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
        #self.data = self.clean_emails('email_address')
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

    #def clean_emails(self, col):
    #    self.data['valid_email'] = self.data[col].apply(lambda x: validate_email(x))
    #    #print(self.data[col].value_counts())
    #    self.data = self.data[self.data['valid_email'] == True]
    #    return self.data  
               
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

    def clean_store_data(self):
        print("Cleaning store data")
        self.data = self.data.drop(columns=['lat']) #drop lat column, it is the same as latitude but this one is empty
        self.data = self.data.dropna()
        self.data = self.clean_dates('opening_date')
        #set the column for staff_numbers to be an integer
        #if any cant be converted to an integer, drop the row
        self.data['staff_numbers'] = pd.to_numeric(self.data['staff_numbers'], errors='coerce')
        self.data = self.data.dropna(subset=['staff_numbers'])
        self.data['staff_numbers'] = self.data['staff_numbers'].astype(int)
        print(self.data.isna().any()) 
        print(self.data.head())
        print(self.data.shape)
        print(self.data.dtypes)

    def convert_product_weights(self):
        print("Converting product weights")
        #print(self.data['weight'].head())
        #print(self.data['weight'].shape)
        self.data = self.data.dropna()
        self.data['weight'] = self.data['weight'].str.replace(" .", "", regex=False)
        
        for weight in self.data['weight']:    
            if isinstance(weight, str) and "x" in weight:
                number_of_items, weight_of_each_item = weight.split("x")
                if weight_of_each_item[-2:] == "kg":
                    total_weight = float(number_of_items) * float(weight_of_each_item[:-2])
                    total_weight = str(total_weight) + "kg"
                elif weight_of_each_item[-1:] == "g":
                    total_weight = float(number_of_items) * float(weight_of_each_item[:-1])
                    total_weight = str(total_weight) + "g"
                elif weight_of_each_item[-2:] == "ml":
                    total_weight = float(number_of_items) * float(weight_of_each_item[:-2])
                    total_weight = str(total_weight) + "ml"
                else:
                    total_weight = None
                #now replace the weight with the total weight
                self.data['weight'] = self.data['weight'].replace(weight, total_weight)           
                
        
        def convert(weight):
            if weight[-2:] == "kg":
                return float(weight[:-2])
            elif weight[-1:] == "g":
                return float(weight[:-1])/1000
            elif weight[-2:] == "ml":
                return float(weight[:-2])/1000
            elif weight[-1:] == "l":
                return float(weight[:-1])
            elif weight[-2:] == "oz":
                return float(weight[:-2])/35.274
            else:
                #remove the weight if it is not in the correct format
                return None
              
        #apply the function to the weight column
        self.data['weight'] = self.data['weight'].apply(convert)
        
        self.data = self.data.dropna()
        #print(self.data['weight'].head())
        #print(self.data['weight'].shape)
        return self.data
    
    def clean_product_data(self):
        print("Cleaning product data")
        #clean of any erroneous values, NULL values or errors with formatting.
        self.data = self.data.dropna()
        self.data = self.clean_dates('date_added')
        
        return self.data


    def clean_orders_data(self):
        print("Cleaning orders data")
        
        self.data = self.data.drop(columns=['first_name', 'last_name', '1', 'level_0'])
        self.data = self.data.dropna()
        
        self.data['card_number'] = self.data['card_number'].astype(int)
        self.data['product_quantity'] = self.data['product_quantity'].astype(int)
             
        return self.data 
    
    def clean_date_events_data(self):
        print("Cleaning date events data")
        self.data = self.data.dropna()
        new_col_datetime = self.data['year'].astype(str) + "-" + self.data['month'].astype(str) + "-" + self.data['day'].astype(str) + " " + self.data['timestamp'].astype(str)
        self.data['datetime'] = new_col_datetime
        self.data = self.clean_dates('datetime')
        #self.data = self.data.drop(columns=['year', 'month', 'day', 'timestamp'])
       
        #def convert_time_period(time):
        #    if time.hour >= 6 and time.hour < 12:
        #        return "Morning"
        #    elif time.hour >= 12 and time.hour < 18:
        #        return "Afternoon"
        #    elif time.hour >= 18 and time.hour < 24:
        #        return "Evening"
        #    elif time.hour >= 0 and time.hour < 6:
        #        return "Night"
        #    else:
        #        return None
        
        #self.data['time_period'] = self.data['datetime'].apply(convert_time_period)
        
        return self.data            
if __name__ == '__main__':

    print("Running data cleaning")
    #data_extractor = DataExtracter()
    #db_connector = DatabaseConnector()
    #tables_list = db_connector.list_db_tables()
    #print("Tables in database:", tables_list)
    #clean_data = DataCleaning(
    #    data_extractor.read_rds_table(db_connector, tables_list[1]))
    #clean_data.clean_data()
