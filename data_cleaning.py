import numpy as np
import pandas as pd
import uuid
pd.options.mode.chained_assignment = None  # default='warn'


class DataCleaning:
    """
    A class for cleaning various types of data.

    Methods:
    1. __init__: Initialises the DataCleaning class with the given data.
    2. clean_user_data: Cleans user-related data, including names, dates of birth, phone numbers, and UUIDs.
    3. clean_dates: Converts date columns to pandas datetime format.
    4. clean_names: Capitalises the first letter of names in a given column.
    5. clean_phone_number: Removes rows with phone numbers of length less than or equal to 3.
    6. clean_user_uuid: Removes rows with user UUIDs that are not 36 characters long.
    7. clean_card_data: Cleans card-related data, including the date of payment confirmation.
    8. clean_store_data: Cleans store-related data, including dropping unnecessary columns and correcting the continent names.
    9. convert_product_weights: Converts product weights to a consistent format (kg).
    10. clean_product_data: Cleans product-related data, including dropping rows with missing values and converting date columns.
    11. clean_orders_data: Cleans order-related data, including dropping unnecessary columns and converting numerical columns to integers.
    12. clean_date_events_data: Cleans date events data, including combining separate date and time columns into a single datetime column.

    """

    def __init__(self, data):
        self.data = data

    def clean_user_data(self):
        """
        Cleans user-related data, including names, dates of birth, phone numbers, and UUIDs.
        """

        print("Cleaning user data")
        self.data = self.data.dropna()
        self.data = self.clean_names('first_name')
        self.data = self.clean_names('last_name')
        self.data = self.clean_dates('date_of_birth')
        self.data = self.clean_phone_number('phone_number')
        self.data = self.clean_dates('join_date')
        self.data = self.clean_user_uuid('user_uuid')

    def clean_dates(self, col):
        """
        Converts date columns to pandas datetime format.
        """
        
        self.data[col] = pd.to_datetime(self.data[col], errors='coerce')
        self.data = self.data.dropna(subset=[col])
        return self.data

    def clean_names(self, col):
        """
        Capitalises the first letter of names in a given column.
        """

        self.data[col] = self.data[col].str.title()
        return self.data

    def clean_phone_number(self, col):
        """
        Removes rows with phone numbers of length less than or equal to 3.
        """
        
        self.data = self.data[self.data[col].str.len() > 3]
        return self.data

    def clean_user_uuid(self, col):
        """
        Removes rows with user UUIDs that are not 36 characters long.
        """
        
        self.data[col] = self.data[col][self.data[col].str.len() == 36]
        return self.data

    def clean_card_data(self):
        """
        Cleans card-related data, including the date of payment confirmation.
        """
        
        print("Cleaning card data")
        self.data = self.data.dropna()
        self.data = self.clean_dates('date_payment_confirmed')

    def clean_store_data(self):
        """ 
        Cleans store-related data, including dropping unnecessary columns and correcting the continent names.
        """
        
        print("Cleaning store data")

        self.data = self.data.drop(columns=['lat'])
        self.data = self.clean_dates('opening_date')
        self.data["continent"] = self.data["continent"].str.replace(
            "eeEurope", "Europe")
        self.data["continent"] = self.data["continent"].str.replace(
            "eeAmerica", "America")

        print(self.data.head())
        print(self.data.shape)
        print(self.data.dtypes)

        return self.data

    def convert_product_weights(self):
        """
        Converts product weights to a consistent format (kg).
        """
        
        print("Converting product weights")
        # print(self.data['weight'].head())
        # print(self.data['weight'].shape)
        self.data = self.data.dropna()
        self.data['weight'] = self.data['weight'].str.replace(
            " .", "", regex=False)

        for weight in self.data['weight']:
            if isinstance(weight, str) and "x" in weight:
                number_of_items, weight_of_each_item = weight.split("x")
                if weight_of_each_item[-2:] == "kg":
                    total_weight = float(number_of_items) * \
                        float(weight_of_each_item[:-2])
                    total_weight = str(total_weight) + "kg"
                elif weight_of_each_item[-1:] == "g":
                    total_weight = float(number_of_items) * \
                        float(weight_of_each_item[:-1])
                    total_weight = str(total_weight) + "g"
                elif weight_of_each_item[-2:] == "ml":
                    total_weight = float(number_of_items) * \
                        float(weight_of_each_item[:-2])
                    total_weight = str(total_weight) + "ml"
                else:
                    total_weight = None
                # now replace the weight with the total weight
                self.data['weight'] = self.data['weight'].replace(
                    weight, total_weight)

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
                # remove the weight if it is not in the correct format
                return None

        self.data['weight'] = self.data['weight'].apply(convert)

        self.data = self.data.dropna()
        return self.data

    def clean_product_data(self):
        """
        Cleans product-related data, including dropping rows with missing values and converting date columns.
        """
        
        print("Cleaning product data")
        self.data = self.data.dropna()
        self.data = self.clean_dates('date_added')

        return self.data

    def clean_orders_data(self):
        """
        Cleans orders-related data, including dropping unnecessary columns and converting numerical columns to integers.
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
        Cleans date events data, including combining separate date and time columns into a single datetime column
        """
        
        print("Cleaning date events data")
        
        self.data = self.data.dropna()
        new_col_datetime = self.data['year'].astype(str) + "-" + self.data['month'].astype(
            str) + "-" + self.data['day'].astype(str) + " " + self.data['timestamp'].astype(str)
        self.data['datetime'] = new_col_datetime
        self.data = self.clean_dates('datetime')

        return self.data


if __name__ == '__main__':

    print("Running data cleaning")
