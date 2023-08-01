import yaml
from sqlalchemy import create_engine, inspect
from data_cleaning import DataCleaning
from data_extraction import DataExtracter
import requests
import json
import pandas as pd


class DatabaseConnector:
    """
    Class to handle database operations.

    Methods:
        read_db_creds(file_path):
            Read the database credentials from a YAML file.

        init_db_engine(file_path):
            Initialise the database engine using the credentials obtained from read_db_creds.

        list_db_tables():
            List all the tables in the database.

        upload_to_db(df, table_name):
            Upload the given DataFrame to the specified database table.

        list_number_of_stores(number_of_stores_api_url, store_dict):
            Retrieve the number of stores from the provided API URL.

        retrieve_stores_data(store_api_url, store_dict):
            Retrieve data for all stores from the provided API URL.
    """

    def read_db_creds(self, file_path):
        """
        Read the database credentials from a YAML file.

        Args:
            file_path (str): Path to the YAML file containing database credentials.

        Returns:
            dict: A dictionary containing the database credentials.
        """
        with open(file_path) as f:
            creds = yaml.load(f, Loader=yaml.FullLoader)
        return creds

    def init_db_engine(self, file_path):
        """
        Initialize the database engine using the credentials obtained from read_db_creds.

        Args:
            file_path (str): Path to the YAML file containing database credentials.

        Returns:
            sqlalchemy.engine.Engine: Database engine object.
        """
        creds = self.read_db_creds(file_path)
        engine = create_engine("postgresql+psycopg2://"+creds['RDS_USER']+":"+creds['RDS_PASSWORD'] +
                               "@"+creds['RDS_HOST']+":"+str(creds['RDS_PORT'])+"/"+creds['RDS_DATABASE'])
        return engine

    def list_db_tables(self):
        """
        List all the tables in the database.

        Returns:
            list: A list of table names present in the database.
        """
        engine = self.init_db_engine("db_creds.yaml")
        inspection = inspect(engine)
        tables_list = inspection.get_table_names()
        return tables_list

    def upload_to_db(self, df, table_name):
        """
        Upload the given DataFrame to the specified database table.

        Args:
            df (pandas.DataFrame): DataFrame to be uploaded to the database.
            table_name (str): Name of the target database table.
        """
        print("Uploading to database")
        engine = self.init_db_engine("my_db_creds.yaml")
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print("Upload complete")

    def list_number_of_stores(self, number_of_stores_api_url, store_dict):
        """
        Retrieve the number of stores from the provided API URL.

        Args:
            number_of_stores_api_url (str): API URL to get the number of stores.
            store_dict (dict): Dictionary containing API headers.

        Returns:
            dict: A dictionary containing the response from the API in JSON format.
        """
        print("Listing number of stores")
        response = requests.get(number_of_stores_api_url, headers=store_dict)
        return response.json()

    def retrieve_stores_data(self, store_api_url, store_dict):
        """
        Retrieve data for all stores from the provided API URL.

        Args:
            store_api_url (str): API URL to get the store data.
            store_dict (dict): Dictionary containing API headers.

        Returns:
            pandas.DataFrame: DataFrame containing the retrieved store data.
        """
        print("Retrieving stores data")
        total_number_of_stores = self.list_number_of_stores(
            number_of_stores_api_url, store_dict)
        print(total_number_of_stores['number_stores'])
        stores_df_list = []
        for i in range(0, total_number_of_stores['number_stores']):
            url = format(store_api_url + str(i))
            response = requests.get(url, headers=store_dict)
            stores_df_list.append(pd.DataFrame(response.json(), index=[i]))
        stores_df = pd.concat(stores_df_list)
        print(stores_df.head())
        print(stores_df.tail())
        print(stores_df.shape)
        return stores_df


if __name__ == '__main__':

    # Create an instance of the DatabaseConnector class
    db_connector = DatabaseConnector()
    
    # Retrieve a list of all tables in the database
    tables_list = db_connector.list_db_tables()
    print("Tables in database:", tables_list)
    
    # Create an instance of the DataExtracter class
    data_extractor = DataExtracter()
    
    # Read data from the legacy_users table (index 1) and perform data cleaning and upload to database
    clean_data = DataCleaning(data_extractor.read_rds_table(db_connector, tables_list[1]))
    clean_data.clean_user_data()
    #db_connector.upload_to_db(clean_data.data, "dim_users")
    
    # Read data from the orders_table table (index 2) and perform data cleaning for orders and upload to database
    orders_df = data_extractor.read_rds_table(db_connector, tables_list[2])
    clean_orders_data = DataCleaning(orders_df).clean_orders_data()
    #db_connector.upload_to_db(clean_orders_data, "orders_table")
    
    # Retrieve data from a PDF file and perform data cleaning for card details and upload to database
    link_to_pdf = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    df_pdf_data = data_extractor.retrieve_pdf_data(link_to_pdf)
    clean_pdf_data = DataCleaning(df_pdf_data)
    clean_pdf_data.clean_card_data()
    #db_connector.upload_to_db(clean_pdf_data.data, "dim_card_details")
    
    # Set up API credentials and URLs to retrieve store data
    store_api_key = "x-api-key"
    store_api_key_value = "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
    store_dict = {store_api_key: store_api_key_value}
    store_api_url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"
    number_of_stores_api_url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    
    # Retrieve store data from the API and perform data cleaning and upload to database
    stores_data = db_connector.retrieve_stores_data(store_api_url, store_dict)
    clean_stores_data = DataCleaning(stores_data).clean_store_data()
    #db_connector.upload_to_db(clean_stores_data, "dim_store_details")
    
    # Extract data from a CSV file. Convert the product weights to kg and perform data cleaning for product details and upload to database
    s3_address = "s3://data-handling-public/products.csv"
    products_df = data_extractor.extract_from_s3(s3_address)
    products_df = DataCleaning(products_df).convert_product_weights()
    clean_products_data = DataCleaning(products_df).clean_product_data()
    #db_connector.upload_to_db(clean_products_data, "dim_products")
    
    # Retrieve data from a JSON file and perform data cleaning for date events and upload to database
    link_to_json = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
    date_events_df = data_extractor.retrieve_json_data(link_to_json)
    clean_date_events_data = DataCleaning(date_events_df).clean_date_events_data()
    #db_connector.upload_to_db(clean_date_events_data, "dim_date_times")
    