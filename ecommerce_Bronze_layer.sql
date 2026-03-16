SELECT COUNT(*) AS total_records
FROM ecommerce_catalog.gold.dim_orders;

#Bronze layer

CREATE TABLE ecommerce_catalog.bronze.customers_table
USING PARQUET
LOCATION 's3://ecommerce-analytics-datalake1/processed/ecommerce_sales_parquet/customers_dataset';

CREATE TABLE ecommerce_catalog.bronze.order_items_table
USING PARQUET
LOCATION 's3://ecommerce-analytics-datalake1/processed/ecommerce_sales_parquet/order_items_dataset';

CREATE TABLE ecommerce_catalog.bronze.order_payment_table
USING PARQUET
LOCATION 's3://ecommerce-analytics-datalake1/processed/ecommerce_sales_parquet/order_payment_dataset';

CREATE TABLE ecommerce_catalog.bronze.orders_table
USING PARQUET
LOCATION 's3://ecommerce-analytics-datalake1/processed/ecommerce_sales_parquet/orders_dataset';

CREATE TABLE ecommerce_catalog.bronze.product_category_name_translation_table
USING PARQUET
LOCATION 's3://ecommerce-analytics-datalake1/processed/ecommerce_sales_parquet/product_category_name_translation';

CREATE TABLE ecommerce_catalog.bronze.products_table
USING PARQUET
LOCATION 's3://ecommerce-analytics-datalake1/processed/ecommerce_sales_parquet/products_dataset';

CREATE TABLE ecommerce_catalog.bronze.sellers_table
USING PARQUET
LOCATION 's3://ecommerce-analytics-datalake1/processed/ecommerce_sales_parquet/sellers_dataset';



-- #Silver layer
-- #Create Customers Table
CREATE TABLE ecommerce_catalog.silver.customers_clean
USING DELTA AS
SELECT
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state
FROM ecommerce_catalog.bronze.customers_table
WHERE customer_id IS NOT NULL;

CREATE  or REPLACE TABLE ecommerce_catalog.silver.customers_clean
USING DELTA AS
SELECT
    CAST(customer_id AS STRING) AS customer_id,
    CAST(customer_unique_id AS STRING) AS customer_unique_id,
    CAST(customer_zip_code_prefix AS INT) AS customer_zip_code_prefix,
    customer_city,
    customer_state
FROM ecommerce_catalog.bronze.customers_table;


--Create Orders Table and change data types
CREATE OR REPLACE TABLE ecommerce_catalog.silver.orders_clean
USING DELTA AS
SELECT
  CAST(order_id AS STRING) AS order_id,
  CAST(customer_id AS STRING) AS customer_id,
  COALESCE(order_status, 'unknown') AS order_status,
  TRY_CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchase_timestamp,
  TRY_CAST(order_approved_at AS TIMESTAMP) AS order_approved_at,
  TRY_CAST(order_delivered_carrier_date AS TIMESTAMP) AS order_delivered_carrier_date,
  TRY_CAST(order_delivered_customer_date AS TIMESTAMP) AS order_delivered_customer_date,
  TRY_CAST(order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_date
FROM ecommerce_catalog.bronze.orders_table;



SELECT COUNT(*) AS total_string_rows
FROM ecommerce_catalog.bronze.orders_table
WHERE order_delivered_carrier_date REGEXP '[a-zA-Z]';

SELECT
TRY_TO_TIMESTAMP(order_delivered_carrier_date,'yyyy-MM-dd HH:mm:ss') 
AS order_delivered_carrier_date
FROM ecommerce_catalog.bronze.orders_table;

SELECT COUNT(*) AS null_count
FROM ecommerce_catalog.bronze.orders_table
WHERE order_delivered_carrier_date IS NULL;



--In this query, you created a clean products table in the Silver layer by converting data types, replacing NULL values with 0 or 'unknown', and removing special characters from product_category_name using REGEXP_REPLACE.
CREATE TABLE ecommerce_catalog.silver.products_clean
USING DELTA AS
SELECT
CAST(product_id AS STRING) AS product_id,

regexp_replace(
    upper(COALESCE(NULLIF(TRIM(product_category_name), ''), 'NONE')),
    '[^a-zA-Z0-9 ]', ''
) AS product_category_name,

COALESCE(CAST(try_cast(product_weight_g AS DOUBLE) AS INT),0) AS product_weight_g,
COALESCE(CAST(try_cast(product_length_cm AS DOUBLE) AS INT),0) AS product_length_cm,
COALESCE(CAST(try_cast(product_height_cm AS DOUBLE) AS INT),0) AS product_height_cm,
COALESCE(CAST(try_cast(product_width_cm AS DOUBLE) AS INT),0) AS product_width_cm
FROM ecommerce_catalog.bronze.products_table;

 
--In this query, you created a clean customers table in the Silver layer by converting data types, replacing NULL values with default values using COALESCE, and removing special characters from customer_city and customer_state using REGEXP_REPLACE.
CREATE TABLE ecommerce_catalog.silver.customers_clean
USING DELTA AS
SELECT
CAST(customer_id AS STRING) AS customer_id,
CAST(customer_unique_id AS STRING) AS customer_unique_id,
COALESCE(CAST(try_cast(customer_zip_code_prefix AS INT) AS INT),0) 
AS customer_zip_code_prefix,
regexp_replace(
    upper(COALESCE(NULLIF(TRIM(customer_city), ''), 'NONE')),
    '[^a-zA-Z0-9 ]', ''
) AS customer_city,
regexp_replace(
    upper(COALESCE(NULLIF(TRIM(customer_state), ''), 'NONE')),
    '[^a-zA-Z0-9 ]', ''
) AS customer_state
FROM ecommerce_catalog.bronze.customers_table;

--In this query, you cleaned the payment data by converting column data types and replacing NULL or invalid values with default values using COALESCE and TRY_CAST.
CREATE OR REPLACE TABLE ecommerce_catalog.silver.order_payments_clean
USING DELTA AS
SELECT
  CAST(order_id AS STRING) AS order_id,
  COALESCE(TRY_CAST(payment_sequential AS INT), 1) AS payment_sequential,
  COALESCE(LOWER(payment_type), 'unknown') AS payment_type,
  COALESCE(TRY_CAST(payment_installments AS INT), 1) AS payment_installments,
  COALESCE(TRY_CAST(payment_value AS DOUBLE), 0.0) AS payment_value
FROM ecommerce_catalog.bronze.order_payment_table;


--checking for null values in seller table
CREATE TABLE  ecommerce_catalog.silver.sellers_clean
USING DELTA AS
SELECT 
COUNT(*) AS total_rows,
COUNT(seller_id) AS seller_id_not_null,
COUNT(seller_zip_code_prefix) AS zip_not_null,
COUNT(seller_city) AS city_not_null,
COUNT(seller_state) AS state_not_null
FROM ecommerce_catalog.bronze.sellers_table;


-- Create Silver table with proper datatypes
CREATE OR REPLACE TABLE ecommerce_catalog.silver.sellers_clean
USING DELTA
AS
SELECT
    CAST(seller_id AS STRING) AS seller_id,
    COALESCE(TRY_CAST(TRIM(seller_zip_code_prefix) AS INT), 0) AS seller_zip_code_prefix,
    regexp_replace(
        upper(COALESCE(NULLIF(TRIM(seller_city), ''), 'NONE')),
        '[^a-zA-Z0-9 ]', ''
    ) AS seller_city,
    regexp_replace(
        upper(COALESCE(NULLIF(TRIM(seller_state), ''), 'NONE')),
        '[^a-zA-Z0-9 ]', ''
    ) AS seller_state
FROM ecommerce_catalog.bronze.sellers_table;

-- Create Silver table with proper datatypes

SELECT *
FROM ecommerce_catalog.bronze.order_items_table
WHERE order_id IS NULL
   OR order_item_id IS NULL
   OR product_id IS NULL
   OR seller_id IS NULL
   OR shipping_limit_date IS NULL
   OR price IS NULL
   OR freight_value IS NULL;

SELECT *
FROM ecommerce_catalog.silver.customers_clean where customer_id IS NULL;



--checkibng for null values
SELECT *
FROM ecommerce_catalog.bronze.product_category_name_translation_table
WHERE product_category_name IS NULL
   OR product_category_name_english IS NULL;

--checking for special characters
SELECT COUNT(*) AS total_special_characters
FROM ecommerce_catalog.bronze.customers_table
WHERE customer_city RLIKE '[^a-zA-Z0-9 ]';



--checking for special characters
SELECT COUNT(*) AS total_special_characters
FROM ecommerce_catalog.bronze.sellers_table
WHERE  seller_city RLIKE '[^a-zA-Z0-9 ]';

SELECT COUNT(*) AS total_special_characters
FROM ecommerce_catalog.bronze.products_table
WHERE  product_category_name RLIKE '[^a-zA-Z0-9 ]';


--handling null values in products_clean
SELECT
regexp_replace(
    UPPER(COALESCE(NULLIF(TRIM(product_category_name), ''), 'NONE')),
    '[^A-Z]',
    ''
) AS product_category_name,
COALESCE(product_weight_g,0) AS product_weight_g,
COALESCE(product_length_cm,0) AS product_length_cm,
COALESCE(product_height_cm,0) AS product_height_cm,
COALESCE(product_width_cm,0) AS product_width_cm
FROM ecommerce_catalog.silver.products_clean;

--handling null values in orders_clean
SELECT
COALESCE(order_delivered_carrier_date, order_approved_at) AS order_delivered_carrier_date,
COALESCE(order_approved_at, order_purchase_timestamp) AS order_approved_at,
COALESCE(order_delivered_customer_date,
         order_delivered_carrier_date,
         order_approved_at) AS order_delivered_customer_date
FROM ecommerce_catalog.silver.orders_clean;




select * from ecommerce_catalog.silver.order_payments_clean;
--handling null values and change city and state to upper case, remove special characters
----handling null values for seller_state and seller_city in sellers_clean table
SELECT
regexp_replace(
    upper(COALESCE(NULLIF(TRIM(seller_city), ''), 'NONE')),
    '[^a-zA-Z0-9 ]',''
) AS seller_city,
regexp_replace(
    upper(COALESCE(NULLIF(TRIM(seller_state), ''), 'NONE')),
    '[^a-zA-Z]',''
) AS seller_state
FROM ecommerce_catalog.silver.sellers_clean;

--handling null values for customer_state and customer_city in customer_clean table
SELECT
regexp_replace(
    upper(COALESCE(NULLIF(TRIM(customer_city), ''), 'NONE')),
    '[^a-zA-Z0-9 ]',''
) AS customer_city,
regexp_replace(
    upper(COALESCE(NULLIF(TRIM(customer_state), ''), 'NONE')),
    '[^a-zA-Z]',''
) AS customer_state
FROM ecommerce_catalog.silver.customers_clean;




