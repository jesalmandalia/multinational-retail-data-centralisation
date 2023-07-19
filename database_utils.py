import yaml
from sqlalchemy import create_engine, inspect
from data_cleaning import DataCleaning
from data_extraction import DataExtracter
import requests
import json
import pandas as pd


class DatabaseConnector:

    def read_db_creds(self, file_path):
        # read the credentials from a yaml file
        with open(file_path) as f:
            creds = yaml.load(f, Loader=yaml.FullLoader)
        return creds

    def init_db_engine(self, file_path):
        # read credentials from the return of read_db_creds
        creds = self.read_db_creds(file_path)
        engine = create_engine("postgresql+psycopg2://"+creds['RDS_USER']+":"+creds['RDS_PASSWORD'] +
                               "@"+creds['RDS_HOST']+":"+str(creds['RDS_PORT'])+"/"+creds['RDS_DATABASE'])
        return engine

    def list_db_tables(self):
        engine = self.init_db_engine("db_creds.yaml")
        inspection = inspect(engine)

        tables_list = inspection.get_table_names()
        return tables_list

    def upload_to_db(self, df, table_name):
        print("Uploading to database")
        engine = self.init_db_engine("my_db_creds.yaml")
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        return None

    def list_number_of_stores(self, number_of_stores_api_url, store_dict):
        print("Listing number of stores")
        response = requests.get(number_of_stores_api_url, headers=store_dict)
        return response.json()

    def retrieve_stores_data(self, store_api_url, store_dict):
        print("Retrieving stores data")
        total_number_of_stores = self.list_number_of_stores(
            number_of_stores_api_url, store_dict)
        print(total_number_of_stores['number_stores'])

        stores_df_list = []
        for i in range(1, total_number_of_stores['number_stores']):
            # for i in range(1, 5): #total_number_of_stores['number_stores']+1):
            url = format(store_api_url + str(i))
            response = requests.get(url, headers=store_dict)
            stores_df_list.append(pd.DataFrame(response.json(), index=[i]))
        stores_df = pd.concat(stores_df_list)
        print(stores_df.head())
        print(stores_df.shape)
        return stores_df


if __name__ == '__main__':

    db_connector = DatabaseConnector()
    tables_list = db_connector.list_db_tables()
    print("Tables in database:", tables_list)
    data_extractor = DataExtracter()
    #clean_data = DataCleaning(
    #    data_extractor.read_rds_table(db_connector, tables_list[1]))
    #clean_data.clean_user_data()
    #db_connector.upload_to_db(clean_data.data, "dim_users")

    orders_df = data_extractor.read_rds_table(db_connector, tables_list[2])
    clean_orders_data = DataCleaning(orders_df).clean_orders_data()   
    db_connector.upload_to_db(clean_orders_data, "orders_table")

    #link_to_pdf = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    #df_pdf_data = data_extractor.retrieve_pdf_data(link_to_pdf)
    #clean_pdf_data = DataCleaning(df_pdf_data)
    #clean_pdf_data.clean_card_data()
    #db_connector.upload_to_db(clean_pdf_data.data, "dim_card_details")

    #store_api_key = "x-api-key"
    #store_api_key_value = "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
    #store_dict = {store_api_key: store_api_key_value}
    #store_api_url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"  # + store_number
    #number_of_stores_api_url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"

    #stores_data = db_connector.retrieve_stores_data(store_api_url, store_dict)
    #clean_stores_data = DataCleaning(stores_data)
    #clean_stores_data.clean_store_data()
    #db_connector.upload_to_db(clean_stores_data.data, "dim_store_details")

    #s3_address = "s3://data-handling-public/products.csv"
    #products_df = data_extractor.extract_from_s3(s3_address)
    #products_df = DataCleaning(products_df).convert_product_weights()
    #clean_products_data = DataCleaning(products_df).clean_product_data()
    #db_connector.upload_to_db(clean_products_data, "dim_products")
    
   
 