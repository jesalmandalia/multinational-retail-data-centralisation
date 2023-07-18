#from database_utils import DatabaseConnector  
import pandas as pd
from sqlalchemy import text

class DataExtracter:
   
    print("Running data extraction") 
    def read_rds_table(self, db_connector, table_name):
        engine = db_connector.init_db_engine("db_creds.yaml") 
        query = 'SELECT * FROM ' + table_name
        df = pd.DataFrame(engine.connect().execute(text(query)))
        return df 


if __name__ == '__main__':
        
    test = DataExtracter()
    #db_connector = DatabaseConnector()
    #tables_list = db_connector.list_db_tables() 
    #test.read_rds_table(db_connector, tables_list[1])
    
