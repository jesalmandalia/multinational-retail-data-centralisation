/* Task 1 /
/ Cast the columns of orders_table to the correct data types. */
ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

ALTER TABLE orders_table
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;

SELECT MAX(LENGTH(CAST(card_number AS text))) AS max_length
FROM orders_table; --19

ALTER TABLE orders_table
ALTER COLUMN card_number TYPE VARCHAR(19);

SELECT MAX(LENGTH(store_code)) AS max_length
FROM orders_table; --12

ALTER TABLE orders_table
ALTER COLUMN store_code TYPE VARCHAR(12);

SELECT MAX(LENGTH(product_code)) AS max_length
FROM orders_table; --11

ALTER TABLE orders_table
ALTER COLUMN product_code TYPE VARCHAR(11);

ALTER TABLE orders_table
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;

ALTER TABLE orders_table
ALTER COLUMN product_quantity TYPE SMALLINT;

SELECT * FROM public.orders_table
LIMIT 5;



/* Task 2 /
/ Cast the columns of the dim_users table to the correct data types. */
ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255);

ALTER TABLE dim_users
ALTER COLUMN last_name TYPE VARCHAR(255);

ALTER TABLE dim_users
ALTER COLUMN date_of_birth TYPE DATE;

SELECT MAX(LENGTH(country_code)) AS max_length
FROM dim_users; --3

ALTER TABLE dim_users
ALTER COLUMN country_code TYPE VARCHAR(3);

ALTER TABLE dim_users
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;

ALTER TABLE dim_users
ALTER COLUMN join_date TYPE DATE;

SELECT * FROM public.dim_users
LIMIT 10



/* Task 3 /
/ Update the dim_store_details table. Represent the null values of the web store correctly. Cast the columns to the correct data types. */
UPDATE dim_store_details
SET address = 'N/A'
WHERE address IS NULL;

UPDATE dim_store_details
SET latitude = 'N/A'
WHERE latitude IS NULL;

UPDATE dim_store_details
SET longitude = 'N/A'
WHERE longitude IS NULL;

UPDATE dim_store_details
SET locality = 'N/A'
WHERE locality IS NULL;

UPDATE dim_store_details
SET longitude = '0.0'
WHERE longitude = 'N/A';

UPDATE dim_store_details
SET latitude = '0.0'
WHERE latitude = 'N/A';

ALTER TABLE dim_store_details
ALTER longitude TYPE FLOAT USING longitude::FLOAT;

ALTER TABLE dim_store_details
ALTER COLUMN locality TYPE VARCHAR(255);

ALTER TABLE dim_store_details
ALTER staff_numbers TYPE SMALLINT USING longitude::SMALLINT;

ALTER TABLE dim_store_details
ALTER COLUMN opening_date TYPE DATE;

ALTER TABLE dim_store_details
ALTER latitude TYPE FLOAT USING longitude::FLOAT;

ALTER TABLE dim_store_details
ALTER COLUMN continent TYPE VARCHAR(255);

SELECT MAX(LENGTH(store_code)) AS max_length
FROM dim_store_details; --12

ALTER TABLE dim_store_details
ALTER COLUMN store_code TYPE VARCHAR(12);

SELECT MAX(LENGTH(country_code)) AS max_length
FROM dim_store_details; --2

ALTER TABLE dim_store_details
ALTER COLUMN country_code TYPE VARCHAR(2);

ALTER TABLE dim_store_details
ALTER COLUMN store_type TYPE VARCHAR(255);



/* Task 4 /
/ Update the dim_products table. Remove the £ from product_price. Add a new column weight_class which will contain human-readable values based on the weight range of the product. */
UPDATE dim_products
SET product_price = REPLACE(product_price, '£', '');

ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR;

UPDATE dim_products
SET weight_class =
    CASE
        WHEN weight < 2 THEN 'Light'
        WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
        WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
        WHEN weight >= 140 THEN 'Truck_Required'
        ELSE 'Unknown'
    END;

SELECT MAX(LENGTH(weight_class)) AS max_length
FROM dim_products; --14

ALTER TABLE dim_products
ALTER COLUMN weight_class TYPE VARCHAR(14);

SELECT * FROM public.dim_products
LIMIT 10;



/* Task 5/
/ Update the dim_products table. Cast the columns to the correct data types. */	
ALTER TABLE dim_products
ADD COLUMN still_available BOOLEAN;

UPDATE dim_products
SET still_available = CASE
                      WHEN removed = 'Still_avaliable' THEN true
                      ELSE false
        	END;

ALTER TABLE dim_products
DROP COLUMN removed;

ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT;

ALTER TABLE dim_products
ALTER COLUMN weight TYPE FLOAT USING weight::FLOAT;

SELECT MAX(LENGTH(EAN)) AS max_length
FROM dim_products; --17

ALTER TABLE dim_products
ALTER COLUMN EAN TYPE VARCHAR(17);

SELECT MAX(LENGTH(product_code)) AS max_length
FROM dim_products; --11

ALTER TABLE dim_products
ALTER COLUMN product_code TYPE VARCHAR(11);

ALTER TABLE dim_products
ALTER COLUMN date_added TYPE DATE;

ALTER TABLE dim_products
ALTER COLUMN uuid TYPE UUID USING uuid::UUID;



/* Task 6 /
/ Update the dim_date_times table. Cast the columns to the correct data types. */
SELECT MAX(LENGTH(month)) AS max_length
FROM dim_date_times; --2

ALTER TABLE dim_date_times
ALTER COLUMN month TYPE VARCHAR(2);

SELECT MAX(LENGTH(year)) AS max_length
FROM dim_date_times; --4

ALTER TABLE dim_date_times
ALTER COLUMN year TYPE VARCHAR(4);

SELECT MAX(LENGTH(day)) AS max_length
FROM dim_date_times; --2

ALTER TABLE dim_date_times
ALTER COLUMN day TYPE VARCHAR(2);

SELECT MAX(LENGTH(time_period)) AS max_length
FROM dim_date_times; --10

ALTER TABLE dim_date_times
ALTER COLUMN time_period TYPE VARCHAR(10);

ALTER TABLE dim_date_times
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;



/* Task 7 /
/ Cast the columns of dim_card_details to the correct data types. */
UPDATE dim_card_details
SET card_number = REPLACE(card_number, '?', '')
WHERE card_number LIKE '?%';

SELECT MAX(LENGTH(card_number)) AS max_length
FROM dim_card_details; --19

ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(19);

SELECT MAX(LENGTH(expiry_date)) AS max_length
FROM dim_card_details; --5

ALTER TABLE dim_card_details
ALTER COLUMN expiry_date TYPE VARCHAR(5);

ALTER TABLE dim_card_details
ALTER COLUMN date_payment_confirmed TYPE DATE;



/* Task 8 /
/ Create the primary keys in the dimension tables. */
ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number);

ALTER TABLE dim_date_times
ADD PRIMARY KEY (date_uuid);

ALTER TABLE dim_products
ADD PRIMARY KEY (product_code);

ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code);

ALTER TABLE dim_users
ADD PRIMARY KEY (user_uuid);



/* Task 9 /
/ Finalise the star-based schema and add the foreign keys to the orders table. */
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_dim_card_details
FOREIGN KEY (card_number)
REFERENCES dim_card_details (card_number);

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_dim_date_times
FOREIGN KEY (date_uuid)
REFERENCES dim_date_times (date_uuid);

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_dim_products
FOREIGN KEY (product_code)
REFERENCES dim_products (product_code);

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_dim_store_details
FOREIGN KEY (store_code)
REFERENCES dim_store_details (store_code);

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_dim_users
FOREIGN KEY (user_uuid)
REFERENCES dim_users (user_uuid);

