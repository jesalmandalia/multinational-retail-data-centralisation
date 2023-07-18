#from database_utils import DatabaseConnector  
import pandas as pd
from sqlalchemy import text
import tabula

class DataExtracter:
   
    print("Running data extraction") 
    def read_rds_table(self, db_connector, table_name):
        engine = db_connector.init_db_engine("db_creds.yaml") 
        query = 'SELECT * FROM ' + table_name
        df = pd.DataFrame(engine.connect().execute(text(query)))
        return df 

    def retrieve_pdf_data(self, link):
        print("Retrieving pdf data")
        # Read remote pdf into list of DataFrame
        dfs_list_pdf_data = tabula.read_pdf(link, pages='all') #returns list of dataframes, each df is one of the pdf pages
        df_pdf_data = pd.concat(dfs_list_pdf_data) #concatenate all dfs into one df
        print(df_pdf_data.head())
        print(df_pdf_data.shape)
        return df_pdf_data

if __name__ == '__main__':
        
    test = DataExtracter()
    #db_connector = DatabaseConnector()
    #tables_list = db_connector.list_db_tables() 
    #test.read_rds_table(db_connector, tables_list[1])
    
