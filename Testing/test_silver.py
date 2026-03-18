import pytest
from pyspark.sql import SparkSession

SILVER_SCHEMA = "ecommerce_catalog.silver"

tables = [
    "products_clean",
    "customers_clean",
    "sellers_clean",
    "orders_clean",
    "order_items_clean",
    "order_payments_clean",
    "product_category_name_translation_clean"
]

# -----------------------------------------
# Spark Session Fixture
# -----------------------------------------

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("silver-layer-test") \
        .getOrCreate()


# -----------------------------------------
# Test 1 : Silver tables exist
# -----------------------------------------

def test_silver_tables_exist(spark):

    tables_df = spark.sql(f"SHOW TABLES IN {SILVER_SCHEMA}")

    for table in tables:
        assert tables_df.filter(tables_df.tableName == table).count() == 1


# -----------------------------------------
# Test 2 : Silver tables contain data
# -----------------------------------------

@pytest.mark.parametrize("table", tables)
def test_silver_row_count(spark, table):

    df = spark.table(f"{SILVER_SCHEMA}.{table}")

    assert df.count() > 0


# -----------------------------------------
# Test 3 : Column names are lowercase
# -----------------------------------------

@pytest.mark.parametrize("table", tables)
def test_column_lowercase(spark, table):

    df = spark.table(f"{SILVER_SCHEMA}.{table}")

    for col_name in df.columns:
        assert col_name == col_name.lower()


# -----------------------------------------
# Test 4 : Products table schema validation
# -----------------------------------------

def test_products_schema(spark):

    df = spark.table(f"{SILVER_SCHEMA}.products_clean")

    expected_columns = [
        "product_id",
        "product_category_name",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm"
    ]

    for col in expected_columns:
        assert col in df.columns


# -----------------------------------------
# Test 5 : Customers table schema validation
# -----------------------------------------

def test_customers_schema(spark):

    df = spark.table(f"{SILVER_SCHEMA}.customers_clean")

    expected_columns = [
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state"
    ]

    for col in expected_columns:
        assert col in df.columns


# -----------------------------------------
# Test 6 : Orders timestamp columns exist
# -----------------------------------------

def test_orders_timestamp_columns(spark):

    df = spark.table(f"{SILVER_SCHEMA}.orders_clean")

    expected_timestamps = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]

    for col in expected_timestamps:
        assert col in df.columns


# -----------------------------------------
# Test 7 : No duplicate keys
# -----------------------------------------

def test_orders_no_duplicates(spark):

    df = spark.table(f"{SILVER_SCHEMA}.orders_clean")

    duplicates = (
        df.groupBy("order_id")
        .count()
        .filter("count > 1")
        .count()
    )

    assert duplicates == 0


# -----------------------------------------
# Test 8 : Price values should not be negative
# -----------------------------------------

def test_price_positive(spark):

    df = spark.table(f"{SILVER_SCHEMA}.order_items_clean")

    invalid = df.filter(df.price < 0).count()

    assert invalid == 0


# -----------------------------------------
# Test 9 : Payment values should not be negative
# -----------------------------------------

def test_payment_positive(spark):

    df = spark.table(f"{SILVER_SCHEMA}.order_payments_clean")

    invalid = df.filter(df.payment_value < 0).count()

    assert invalid == 0


# -----------------------------------------
# Test 10 : No empty strings in cleaned columns
# -----------------------------------------

def test_no_empty_strings_customers(spark):

    df = spark.table(f"{SILVER_SCHEMA}.customers_clean")

    empty_city = df.filter(df.customer_city == "").count()

    assert empty_city == 0