# Multinational Retail Data Centralisation


- [Multinational Retail Data Centralisation](#multinational-retail-data-centralisation)
	- [Project Background](#project-background)
	- [Project Overview](#project-overview)
	- [Files](#files)
		- [`database_utils.py`](#database_utilspy)
		- [`data_cleaning.py`](#data_cleaningpy)
		- [`data_extracter.py`](#data_extracterpy)
		- [`create_the_database_schema.sql`](#create_the_database_schemasql)
		- [`querying_the_data.sql`](#querying_the_datasql)
	- [How To Run](#how-to-run)
	- [Required Dependencies](#required-dependencies)
		- [Example Usage](#example-usage)
			- [sqlalchemy](#sqlalchemy)
			- [tabula](#tabula)
			- [boto3](#boto3)
			- [requests](#requests)



## Project Background
A multinational company sells various goods across the globe. Currently, their sales data is spread across different data sources making it not easily accessible or analysable by current members of the team. In an effort to become more data-driven, the organisation would like to make its sales data accessible from one centralised location. 


This project creates a system to access the current company data in a database from one centralised location and acts as a single source of truth for sales data Included also are queries for the database to get up-to-date metrics for the business. 


## Project Overview

This project consists of three Python files that facilitate data extraction, cleaning, and database connectivity. These files are designed to be used together in a data processing pipeline to handle data from various sources, clean it, and store it in a PostgreSQL database.

There are 2 SQL files which perform various tasks, such as data manipulation, and data type casting, to maintain and analyse the star-based schema in the PostgreSQL database. The queries to retrieve up-to-date metrics have also been included.

## Files

### `database_utils.py`

This file contains the `DatabaseConnector` class, which provides methods for reading database credentials from a YAML file, initialising a database engine for connecting to PostgreSQL, listing database tables, uploading data to the database, and obtaining the number of stores in each country from the `dim_store_details` table. The methods in this class include:

- `read_db_creds`: Reads the database credentials from a YAML file.
- `init_db_engine`: Initialises the database engine using the credentials obtained.
- `list_db_engine`: List all the tables in the database.
- `upload_to_db`: Uploads the given DataFrame to the specified database table.
- `list_number_of_stores`: Retrieves the number of stores from the provided API URL.
- `retrieve_stores_data`: Retrieves data for all stores from the provided API URL.


### `data_cleaning.py`

This file contains the `DataCleaning` class, which is responsible for cleaning different types of data. The methods in this class include:

- `clean_user_data`: Cleans user-related data, including names, dates of birth, phone numbers, and UUIDs.
- `clean_dates`: Converts date columns to pandas datetime format.
- `clean_names`: Capitalises the first letter of names in a given column.
- `clean_phone_number`: Removes rows with phone numbers of length less than or equal to 3.
- `clean_user_uuid`: Removes rows with user UUIDs that are not 36 characters long.
- `clean_card_data`: Cleans card-related data, including the date of payment confirmation.
- `clean_store_data`: Cleans store-related data, including dropping unnecessary columns and updating the continent names.
- `convert_product_weights`: Converts product weights to a consistent format (e.g., kilograms, grams, millilitres).
- `clean_product_data`: Cleans product-related data, including dropping rows with missing values and converting date columns.
- `clean_orders_data`: Cleans order-related data, including dropping unnecessary columns and converting numerical columns to integers.
- `clean_date_events_data`: Cleans date events data, including combining separate date and time columns into a single datetime column.

### `data_extracter.py`

This file contains the `DataExtracter` class, which handles data extraction from various sources. The methods in this class include:

- `read_rds_table`: Reads a table from a PostgreSQL database using SQLAlchemy.
- `retrieve_pdf_data`: Retrieves data from a PDF file using the tabula library.
- `extract_from_s3`: Extracts data from a CSV file stored in an S3 bucket using boto3.
- `retrieve_json_data`: Retrieves data from a JSON file.


### `create_the_database_schema.sql`

The `create_the_database_schema.sql` contains a series of SQL queries that perform data type casting, data manipulation, and schema modifications for the star-based schema in the PostgreSQL database.

- Task 1: Cast the columns of `orders_table` to the correct data types. 
- Task 2: Cast the columns of the `dim_users` table to the correct data types. 
- Task 3: Update the `dim_store_details` table, handle null values, and cast columns to correct data types.
- Task 4: Update the `dim_products` table to remove the '£' symbol from `product_price`, and add a new `weight_class` column based on the product weight range.
- Task 5: Update the `dim_products` table. Convert `removed` to `still_available`, and cast columns to the correct data types 
- Task 6: Cast the columns in the `dim_date_times` table to the correct data types. 
- Task 7: Cast the columns of the `dim_card_details` table to the correct data types.


- Task 8: Create primary keys in the dimension tables (`dim_card_details`, `dim_date_times`, `dim_products`, `dim_store_details`, and `dim_users`).
- Task 9: Finalise the star-based schema by adding foreign keys to the `orders_table` referencing the dimension tables.

Please ensure you run these SQL queries in the correct order and make necessary backups before applying any modifications to your database.


### `querying_the_data.sql`

This section provides an overview of the SQL queries present in the SQL file. These queries perform various tasks to answer business-related questions.

- Number of Stores per Country: This query counts the number of stores in each country from the 'dim_store_details' table and presents the results grouped by 'country_code'. 

- Top Store Locations: This query determines the locations with the most stores. It counts the number of stores in each locality from the 'dim_store_details' table and presents the results grouped by 'locality'. 

- Monthly Sales Analysis: This query calculates total sales for each month by multiplying 'product_quantity' and 'product_price' from the 'orders_table' and 'dim_products' tables. The results are grouped by 'month', showing which months typically produce the highest cost of sales.

- Sales from Online vs. Offline: This query examines how many sales are made online (Web) and offline for each location. The 'WEB-1388012W' store is labelled as 'Web', while other stores are labelled as 'Offline'.

- Store Sales Percentage: This query calculates the percentage of sales that each type of store contributes to the overall total sales. It first calculates the total sales for each store type in 'store_sales'. Then, it calculates the percentage of each store type's sales relative to the overall total.

- Monthly Sales by Year: This query analyses the month in each year that produced the highest cost of sales. It sums up 'product_quantity * product_price' for each month and year and groups the results by 'month' and 'year'.

- Staff Headcount per Country: This query calculates the total staff headcount for each country from the 'dim_store_details' table and groups the results by 'country_code'. 

- Top-Selling German Store Types: This query identifies the German store type that is selling the most. It calculates total sales for each store type in Germany (country_code = 'DE') and presents the results grouped by 'store_type' and 'country_code'.

- Time Between Sales: This query calculates how quickly the company is making sales by computing the average time difference between consecutive datetime values in the 'dim_date_times' table. The results are grouped by 'year', showing the average time taken to make a sale in each year.

Please ensure you run these SQL queries in the correct order and take necessary backups before applying any modifications to your database.

## How To Run

To run the project, follow these steps:

1. Clone the repository:

```bash
git clone https://github.com/jesalmandalia/multinational-retail-data-centralisation
cd multinational-retail-data-centralisation
```

2. Install the required dependencies:

Please ensure that you have installed the necessary libraries (e.g., pandas, sqlalchemy, tabula, boto3) before running the scripts. See the [required dependencies](#required-dependencies) section for more details.


```bash
pip install -r requirements.txt
```

3. Update the database credentials:

Ensure you have created/filled in the db_creds.yaml and my_db_creds.yaml with the credentials of the database you are accessing and want to upload to respectively.

4. Run the main script:

```bash
python main.py
```

## Required Dependencies 

Before running the project, make sure you have the following dependencies installed:

- **sqlalchemy**: Used to connect to the PostgreSQL database.
- **tabula**: Required for extracting data from PDF files.
- **boto3**: Used for interacting with AWS S3.
- **requests**: A Python library for making HTTP requests

You can install the dependencies individually using pip with the following command:

```bash
pip install boto3
```

Be sure to replace boto3 with whichever package you need. The `requirements.txt` file can also be used to set up the environment. 

### Example Usage 

#### sqlalchemy
```python
from sqlalchemy import create_engine

# Connecting to a PostgreSQL database
engine = create_engine('postgresql://username:password@localhost/db_name')

# Executing SQL queries
result = engine.execute('SELECT * FROM table_name')

```

#### tabula
```python
import tabula

# Read remote PDF into list of DataFrame
dfs_list_pdf_data = tabula.read_pdf('https://example.com/sample.pdf', pages='all')

# Concatenate all DataFrames into one DataFrame
df_pdf_data = pd.concat(dfs_list_pdf_data)

```

#### boto3
```python
import boto3

# Creating an S3 client
s3 = boto3.client('s3')

# Uploading a file to an S3 bucket
s3.upload_file('file.txt', 'my-bucket', 'file.txt')

```

#### requests
```python
import requests

# Make a GET request to an API
response = requests.get('https://api.example.com/data')

# Convert the JSON response to a Python dictionary
data = response.json()

```

