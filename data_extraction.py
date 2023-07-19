#from database_utils import DatabaseConnector  
import pandas as pd
from sqlalchemy import text
import tabula
import boto3

class DataExtracter:
   
    print("Running data extraction") 
    def read_rds_table(self, db_connector, table_name):
        engine = db_connector.init_db_engine("db_creds.yaml") 
        query = 'SELECT * FROM ' + table_name
        df = pd.DataFrame(engine.connect().execute(text(query)))
        print(df.head())
        print(df.shape)
        return df 

    def retrieve_pdf_data(self, link):
        print("Retrieving pdf data")
        # Read remote pdf into list of DataFrame
        dfs_list_pdf_data = tabula.read_pdf(link, pages='all') #returns list of dataframes, each df is one of the pdf pages
        df_pdf_data = pd.concat(dfs_list_pdf_data) #concatenate all dfs into one df
        print(df_pdf_data.head())
        print(df_pdf_data.shape)
        return df_pdf_data

    def extract_from_s3(self, s3_address):
        print("Extracting data from S3")
        s3 = boto3.client('s3')
        #'s3' is a key word. create connection to S3 using default config and all buckets within S3
        
        #split the adress into bucket and key
        split_address = s3_address.split('/')
        bucket = split_address[2]
        key = split_address[3]
        obj = s3.get_object(Bucket=bucket, Key=key)
        #what data type is obj? obj is a dictionary with metadata and body etc.  
        df_s3_data = pd.read_csv(obj['Body']) 
        print(df_s3_data.head())
        print(df_s3_data.shape)
        return df_s3_data

    def retrieve_json_data(self, link):
        print("Retrieving json data")
        df_json_data = pd.read_json(link)
        print(df_json_data.head())
        print(df_json_data.shape)
        return df_json_data        

if __name__ == '__main__':
        
    test = DataExtracter()
    #db_connector = DatabaseConnector()
    #tables_list = db_connector.list_db_tables() 
    #test.read_rds_table(db_connector, tables_list[1])
    
