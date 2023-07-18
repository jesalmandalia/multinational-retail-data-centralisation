import yaml
from sqlalchemy import create_engine, inspect
from data_cleaning import DataCleaning
from data_extraction import DataExtracter

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
        # list all tables in the database
        engine = self.init_db_engine("db_creds.yaml")
        inspection = inspect(engine)

        tables_list = inspection.get_table_names()
        return tables_list

    def upload_to_db(self, df, table_name):
        print("Uploading to database")
        engine = self.init_db_engine("my_db_creds.yaml")
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        return None


if __name__ == '__main__':

    db_connector = DatabaseConnector()
    tables_list = db_connector.list_db_tables()
    print("Tables in database:", tables_list)
    data_extractor = DataExtracter()
    clean_data = DataCleaning(
        data_extractor.read_rds_table(db_connector, tables_list[1]))
    clean_data.clean_user_data()
    #print(clean_data.user_data.head())
    #print(clean_data.user_data.shape)
    db_connector.upload_to_db(clean_data.user_data, "dim_card_details")
    
    
