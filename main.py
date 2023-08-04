from data_cleaning import DataCleaning
from data_extraction import DataExtracter
from database_utils import DatabaseConnector

# Create an instance of the DatabaseConnector class
db_connector = DatabaseConnector()

# Retrieve a list of all tables in the database
tables_list = db_connector.list_db_tables()
print("Tables in database:", tables_list)

# Create an instance of the DataExtracter class
data_extractor = DataExtracter()

# Read data from the legacy_users table (index 1) and 
# perform data cleaning and upload to database
users_table = data_extractor.read_rds_table(db_connector, tables_list[1])
users_data_cleaner = DataCleaning(users_table)
clean_users_data = users_data_cleaner.clean_user_data()
db_connector.upload_to_db(clean_users_data, "dim_users")

# Read data from the orders_table table (index 2) and 
# perform data cleaning for orders and upload to database
orders_df = data_extractor.read_rds_table(db_connector, tables_list[2])
orders_data_cleaner = DataCleaning(orders_df)
clean_orders_data = orders_data_cleaner.clean_orders_data()
db_connector.upload_to_db(clean_orders_data, "orders_table")

# Retrieve data from a PDF file and 
# perform data cleaning for card details and upload to database
link_to_pdf = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
df_pdf_data = data_extractor.retrieve_pdf_data(link_to_pdf)
pdf_data_cleaner = DataCleaning(df_pdf_data)
clean_pdf_data = pdf_data_cleaner.clean_card_data()
db_connector.upload_to_db(clean_pdf_data, "dim_card_details")

# Set up API credentials and URLs to retrieve store data
store_api_key = "x-api-key"
store_api_key_value = "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
store_dict = {store_api_key: store_api_key_value}
store_api_url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"
number_of_stores_api_url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
# Retrieve store data from the API 
# and perform data cleaning and upload to database
stores_data = data_extractor.retrieve_stores_data(db_connector,
        store_api_url, number_of_stores_api_url, store_dict)
stores_data_cleaner = DataCleaning(stores_data)
clean_stores_data = stores_data_cleaner.clean_store_data()
db_connector.upload_to_db(clean_stores_data, "dim_store_details")

# Extract data from a CSV file. 
# Convert the product weights to kg and 
# perform data cleaning for product details and upload to database
s3_address = "s3://data-handling-public/products.csv"
products_df = data_extractor.extract_from_s3(s3_address)
products_data_cleaner = DataCleaning(products_df)
clean_products_data = products_data_cleaner.clean_product_data()
db_connector.upload_to_db(clean_products_data, "dim_products")

# Retrieve data from a JSON file and 
# perform data cleaning for date events and upload to database
link_to_json = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
date_events_df = data_extractor.retrieve_json_data(link_to_json)
date_events_data_cleaner = DataCleaning(date_events_df)
clean_date_events_data = date_events_data_cleaner.clean_date_events_data()
db_connector.upload_to_db(clean_date_events_data, "dim_date_times")
