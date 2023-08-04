from sqlalchemy import create_engine, inspect
import pandas as pd
import json
import requests
import yaml


class DatabaseConnector:
    """
    Class to handle database operations.

    Methods:
        read_db_creds(file_path):
            Read the database credentials from a YAML file.

        init_db_engine(file_path):
            Initialise the database engine using the credentials 
            obtained from read_db_creds.

        list_db_tables():
            List all the tables in the database.

        upload_to_db(df, table_name):
            Upload the given DataFrame to the specified database table.

        list_number_of_stores(number_of_stores_api_url, store_dict):
            Retrieve the number of stores from the provided API URL.
    """

    def read_db_creds(self, file_path):
        """
        Read the database credentials from a YAML file.

        Parameters:
            file_path (str): Path to the YAML file containing 
            database credentials.

        Returns:
            dict: A dictionary containing the database credentials.
        """
        with open(file_path) as f:
            creds = yaml.load(f, Loader=yaml.FullLoader)
            
        return creds

    def init_db_engine(self, file_path):
        """
        Initialise the database engine using the credentials obtained 
        from read_db_creds.

        Parameters:
            file_path (str): Path to the YAML file containing database 
            credentials.

        Returns:
            sqlalchemy.engine.Engine: Database engine object.
        """
        creds = self.read_db_creds(file_path)
        engine = create_engine("postgresql+psycopg2://"+creds['RDS_USER']+
                               ":"+creds['RDS_PASSWORD']+"@"+creds['RDS_HOST']+
                               ":"+str(creds['RDS_PORT'])+
                               "/"+creds['RDS_DATABASE'])
        
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

        Parameters:
            df (pandas.DataFrame): DataFrame to be uploaded to the 
            database.
            table_name (str): Name of the target database table.
        """
        print("Uploading to database")
        engine = self.init_db_engine("my_db_creds.yaml")
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print("Upload complete")

    def list_number_of_stores(self, number_of_stores_api_url, store_dict):
        """
        Retrieve the number of stores from the provided API URL.

        Parameters:
            number_of_stores_api_url (str): API URL to get the number
            of stores.
            store_dict (dict): Dictionary containing API headers.

        Returns:
            dict: A dictionary containing the response from the API 
            in JSON format.
        """
        print("Listing number of stores")
        response = requests.get(number_of_stores_api_url, headers=store_dict)
        
        return response.json()


if __name__ == '__main__':

    print("Running data utilities")