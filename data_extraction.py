from sqlalchemy import text
import pandas as pd
import boto3
import tabula

class DataExtracter:
    """
    A class for extracting data from various sources.

    Methods:
    1. read_rds_table: Reads data from a table in the database.
    2. retrieve_pdf_data: Retrieves data from a PDF file.
    3. extract_from_s3: Extracts data from a CSV file stored in Amazon S3.
    4. retrieve_json_data: Retrieves data from a JSON file.

    """

    def read_rds_table(self, db_connector, table_name):
        """
        Reads data from a specific table in the database.

        Args:
        db_connector (DatabaseConnector): An instance of the DatabaseConnector class to establish a database connection.
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

        Args:
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

        Args:
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

        Args:
        link (str): The URL or file path of the JSON file.

        Returns:
        pd.DataFrame: A DataFrame containing the data from the JSON file.

        """
        print("Retrieving json data")
        df_json_data = pd.read_json(link)
        
        print(df_json_data.head())
        print(df_json_data.shape)
        
        return df_json_data        

if __name__ == '__main__':
    print("Running data extraction")
