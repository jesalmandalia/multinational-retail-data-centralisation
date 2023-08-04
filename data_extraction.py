from sqlalchemy import text
import pandas as pd
import boto3
import requests
import tabula

class DataExtracter:
    """
    A class for extracting data from various sources.

    Methods:
    1. read_rds_table: Reads data from a table in the database.
    2. retrieve_pdf_data: Retrieves data from a PDF file.
    3. extract_from_s3: Extracts data from a CSV file stored in Amazon S3.
    4. retrieve_json_data: Retrieves data from a JSON file.
    5. retrieve_stores_data(store_api_url, store_dict):
            Retrieve data for all stores from the provided API URL.

    """

    def read_rds_table(self, db_connector, table_name):
        """
        Reads data from a specific table in the database.

        Parameters:
        db_connector (DatabaseConnector): An instance of the DatabaseConnector 
            class to establish a database connection.
        table_name (str): The name of the table to read data from.

        Returns:
        pd.DataFrame: A DataFrame containing the data from the specified table.

        """
        engine = db_connector.init_db_engine("db_creds.yaml") 
        query = 'SELECT * FROM ' + table_name
        df = pd.DataFrame(engine.connect().execute(text(query))) 
        print(df.head())
        print(df.shape)
        
        return df 

    def retrieve_pdf_data(self, link):
        """
        Retrieves data from a PDF file.

        Parameters:
        link (str): The URL or file path of the PDF file.

        Returns:
        pd.DataFrame: A DataFrame containing the data extracted from the PDF.

        """
        print("Retrieving pdf data")
        dfs_list_pdf_data = tabula.read_pdf(link, pages='all') 
        df_pdf_data = pd.concat(dfs_list_pdf_data)      
        print(df_pdf_data.head())
        print(df_pdf_data.shape)
        
        return df_pdf_data

    def extract_from_s3(self, s3_address):
        """
        Extracts data from a CSV file stored in Amazon S3.

        Parameters:
        s3_address (str): The S3 address (s3://bucket/key) of the CSV file.

        Returns:
        pd.DataFrame: A DataFrame containing the data from the CSV file.

        """
        print("Extracting data from S3")
        s3 = boto3.client('s3')   
        split_address = s3_address.split('/')
        bucket = split_address[2]
        key = split_address[3]
        obj = s3.get_object(Bucket=bucket, Key=key)
        df_s3_data = pd.read_csv(obj['Body']) 
        print(df_s3_data.head())
        print(df_s3_data.shape)
        
        return df_s3_data

    def retrieve_json_data(self, link):
        """
        Retrieves data from a JSON file.

        Parameters:
        link (str): The URL or file path of the JSON file.

        Returns:
        pd.DataFrame: A DataFrame containing the data from the JSON file.

        """
        print("Retrieving json data")
        df_json_data = pd.read_json(link)
        print(df_json_data.head())
        print(df_json_data.shape)
        
        return df_json_data       
    
    
    def retrieve_stores_data(self, db_connector, store_api_url, number_of_stores_api_url, 
                             store_dict):
        """
        Retrieve data for all stores from the provided API URL.

        Parameters:
            db_connector (DatabaseConnector): An instance of the DatabaseConnector 
                class to establish a database connection.
            store_api_url (str): API URL to get the store data.
            number_of_stores_api_url (str): API URL to get the number
                of stores.
            store_dict (dict): Dictionary containing API headers.

        Returns:
            pandas.DataFrame: DataFrame containing the retrieved store 
            data.
        """
        print("Retrieving stores data")
        total_number_of_stores = db_connector.list_number_of_stores(
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
    print("Running data extraction")
