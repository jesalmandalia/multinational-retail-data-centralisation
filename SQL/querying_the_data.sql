/* Task 1 /
/ This query returns how many stores the business has and in which country. It counts the number of stores in each country from the 'dim_store_details' table. The results are grouped by 'country_code' and ordered based on the number of stores (num_stores). */
SELECT country_code, COUNT(*) AS num_stores
FROM dim_store_details
GROUP BY country_code
ORDER BY num_stores DESC;



/* Task 2 /
/ This query returns the locations which currently have the most stores. It counts the number of stores in each locality from the 'dim_store_details' table. The results are grouped by locality. */
SELECT locality, COUNT(*) AS num_stores
FROM dim_store_details
GROUP BY locality
ORDER BY num_stores DESC
LIMIT 6;



/* Task 3 /
/ This query looks at which months produce the average highest cost of sales typically. It calculates total sales for each month by multiplying product_quantity and product_price from 'orders_table' and 'dim_products' tables, respectively. The results are grouped by 'month'.*/
SELECT
    SUM(o.product_quantity * p.product_price) AS total_sales,
    d.month
--    p.product_code,
--    p.product_price,
--    o.product_quantity,
FROM
    orders_table o
JOIN
    dim_products p ON o.product_code = p.product_code
JOIN
    dim_date_times d ON o.date_uuid = d.date_uuid
GROUP BY
    d.month
ORDER BY
    total_sales DESC;



/* Task 4 /
/ This query looks at how many sales are coming from online. It counts sales and sums product_quantity for each location (Web or Offline), labeling 'WEB-1388012W' as 'Web' and others as 'Offline', ordered by location in descending order. */
SELECT
    COUNT(*) AS numbers_of_sales,
    SUM(o.product_quantity) AS product_quantity_count,
    CASE
        WHEN s.store_code = 'WEB-1388012W' THEN 'Web'
        ELSE 'Offline'
    END AS location
FROM
    orders_table o
JOIN 
    dim_store_details s ON o.store_code = s.store_code
GROUP BY
    location
ORDER BY
    location DESC;



/* Task 5 /
/ This query looks at the percentage of sales which come through each type of store. The total sales are first calculated. This information is then used to calculate the percentage of each store type's sales relative to the overall total.*/
WITH store_sales AS (
    SELECT
        s.store_type,
        SUM(o.product_quantity * p.product_price) AS total_sales
    FROM
        orders_table o
    JOIN
        dim_products p ON o.product_code = p.product_code
    JOIN 
        dim_store_details s ON o.store_code = s.store_code
    GROUP BY
        s.store_type
)
SELECT
    store_type,
    (total_sales),
    ROUND((total_sales::numeric / SUM(total_sales::numeric) OVER()) * 100, 2) AS percentage_total
FROM
    store_sales
ORDER BY
    total_sales DESC;



/* Task 6 /
/ This query looks at which month in each year produced the highest cost of sales. It sums the staff_numbers and then returns the result grouped by country.*/
SELECT
	SUM(o.product_quantity * p.product_price) AS total_sales,
    d.year,
	d.month
FROM
    orders_table o
JOIN
    dim_products p ON o.product_code = p.product_code
JOIN
    dim_date_times d ON o.date_uuid = d.date_uuid
GROUP BY
    d.month,
	d.year
ORDER BY
    total_sales DESC;



/* Task 7 /
/ This query calculates the staff headcount. It sums up the staff numbers for each country and groups the results by 'country_code'.*/
SELECT
    SUM(staff_numbers) AS total_staff_numbers,
    country_code
FROM
    dim_store_details
GROUP BY
    country_code
ORDER BY
    total_staff_numbers DESC;



/*Task 8/
/ This query returns which German store type is selling the most. It calculates total sales for each store type in Germany (DE). Then groups by store_type and country_code, ordering in ascending order. */
SELECT
    ROUND(SUM(o.product_quantity::numeric * p.product_price::numeric), 2) AS total_sales,
    s.store_type,
    s.country_code
FROM
    orders_table o
JOIN
    dim_products p ON o.product_code = p.product_code
JOIN
    dim_store_details s ON o.store_code = s.store_code
WHERE
    s.country_code = 'DE'
GROUP BY
    s.store_type,
    s.country_code
ORDER BY
    total_sales;



/*Task 9/
/ This query returns how quickly the company is making sales. It calculates the average time difference for each year based on the datetime column. */ 
SELECT
    year,
    AVG(time_diff) AS actual_time_taken
FROM (
    SELECT
        year,
        datetime,
        LEAD(datetime) OVER (PARTITION BY year ORDER BY datetime) AS next_datetime,
        LEAD(datetime) OVER (PARTITION BY year ORDER BY datetime) - datetime AS time_diff
    FROM dim_date_times
) AS time_diffs
WHERE next_datetime IS NOT NULL
GROUP BY year
ORDER BY actual_time_taken DESC;


