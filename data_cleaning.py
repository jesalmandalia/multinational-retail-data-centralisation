from data_transformations import DataTransformations
import numpy as np
import pandas as pd
import uuid
pd.options.mode.chained_assignment = None  # default='warn'


class DataCleaning:
    """
    A class for cleaning various types of data.

    Methods:
    1. __init__: Initialises the DataCleaning class with the given data.
    2. clean_user_data: Cleans user-related data, including names, 
       dates of birth, phone numbers, and UUIDs.
    3. clean_card_data: Cleans card-related data, including the date of 
       payment confirmation.
    4. clean_store_data: Cleans store-related data, including dropping 
       unnecessary columns and correcting the continent names.
    5. clean_product_data: Cleans product-related data, including 
       converting weights, dropping rows with missing values and 
       converting date columns.
    6. clean_orders_data: Cleans order-related data, including dropping 
        unnecessary columns and converting numerical columns to integers.
    7. clean_date_events_data: Cleans date events data, including combining
        separate date and time columns into a single datetime column.

    """

    def __init__(self, data):
        self.data = data
       
    def clean_user_data(self):
        """
        Cleans user-related data, including names, dates of birth, 
        phone numbers, and UUIDs.
        """

        print("Cleaning user data")
        self.data = self.data.dropna()
        data_transformations = DataTransformations(self.data)        
        self.data = data_transformations.clean_names('first_name')        
        self.data = data_transformations.clean_names('last_name')
        self.data = data_transformations.clean_dates('date_of_birth')
        self.data = data_transformations.clean_phone_number('phone_number')
        self.data = data_transformations.clean_dates('join_date')
        self.data = data_transformations.clean_user_uuid('user_uuid')

        return self.data
    
    def clean_card_data(self):
        """
        Cleans card-related data, including the date of payment confirmation.
        """

        print("Cleaning card data")
        self.data = self.data.dropna()
        data_transformations = DataTransformations(self.data)        
        self.data = data_transformations.clean_dates('date_payment_confirmed')
        
        return self.data

    def clean_store_data(self):
        """ 
        Cleans store-related data, including dropping unnecessary columns 
        and correcting the continent names.
        """

        print("Cleaning store data")
        self.data = self.data.drop(columns=['lat'])
        data_transformations = DataTransformations(self.data)        
        self.data = data_transformations.clean_dates('opening_date')
        self.data["continent"] = self.data["continent"].str.replace(
            "eeEurope", "Europe")
        self.data["continent"] = self.data["continent"].str.replace(
            "eeAmerica", "America")
        print(self.data.head())
        print(self.data.shape)
        print(self.data.dtypes)

        return self.data


    def clean_product_data(self):
        """
        Cleans product-related data, including converting weights, 
        dropping rows with missing values and converting date columns.
        """

        print("Cleaning product data")
        self.data = self.data.dropna()
        data_transformations = DataTransformations(self.data)        
        self.data = data_transformations.convert_product_weights()
        self.data = data_transformations.clean_dates('date_added')

        return self.data

    def clean_orders_data(self):
        """
        Cleans orders-related data, including dropping unnecessary 
        columns and converting numerical columns to integers.
        """

        print("Cleaning orders data")
        self.data = self.data.drop(
            columns=['first_name', 'last_name', '1', 'level_0'])
        self.data = self.data.dropna()
        self.data['card_number'] = self.data['card_number'].astype(int)
        self.data['product_quantity'] = self.data['product_quantity'].astype(
            int)

        return self.data

    def clean_date_events_data(self):
        """
        Cleans date events data, including combining separate date 
        and time columns into a single datetime column
        """

        print("Cleaning date events data")
        self.data = self.data.dropna()
        new_col_datetime = self.data['year'].astype(
            str) + "-" + self.data['month'].astype(
            str) + "-" + self.data['day'].astype(
                str) + " " + self.data['timestamp'].astype(str)
        self.data['datetime'] = new_col_datetime
        data_transformations = DataTransformations(self.data)        
        self.data = data_transformations.clean_dates('datetime')

        return self.data


if __name__ == '__main__':

    print("Running data cleaning")

