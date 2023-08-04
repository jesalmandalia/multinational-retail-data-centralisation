import numpy as np
import pandas as pd
import uuid
pd.options.mode.chained_assignment = None  # default='warn'


class DataTransformations:
    """
    A class for data transformations which clean the dataframe.

    Methods:
    1. clean_dates: Converts date columns to pandas datetime format.
    2. clean_names: Capitalises the first letter of names in a given column.
    3. clean_phone_number: Removes rows with phone numbers of length 
       less than or equal to 3.
    4. clean_user_uuid: Removes rows with user UUIDs that are not 36 
       characters.
    5. convert_product_weights: Converts product weights to a 
       consistent format (kg).
    6. _multiple_items_to_single_weight: Calculate the total weight when
       the input is a multiple of a number and a weight unit

    """

    def __init__(self, data):
        self.data = data

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


    def convert_product_weights(self):
        """
        Converts product weights to a consistent format (kg).
        """

        print("Converting product weights")
        self.data = self.data.dropna()
        self.data['weight'] = self.data['weight'].str.replace(
            " .", "", regex=False)
        self.data['weight'] = self._multiple_items_to_single_weight(
                self.data['weight'])
        # Convert all weights to kg. Assume 1l = 1kg and 1oz = 28.35g
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

    def _multiple_items_to_single_weight(self, weight_column):
        """        
        Calculate the total weight when the input is a multiple of a number and a weight unit e.g. 5 x 0.3 kg is converted to a single weight 1.5 kg
        """

        for weight in weight_column:
            # Look for a string with an x (multiply) in it and split into two
            # e.g. 5 x 0.3 kg becomes 5 and 0.3 kg. Then calculate the total.
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
                weight_column = weight_column.replace(
                    weight, total_weight)
        
        return weight_column