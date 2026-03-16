import pytest
from pyspark.sql import SparkSession

BRONZE_SCHEMA = "ecommerce_catalog.bronze"

tables = [
    "customers_table",
    "order_items_table",
    "order_payments_table",
    "orders_table",
    "products_table",
    "product_category_translation_table",
    "sellers_table"
]

# -----------------------------------------
# Spark Session Fixture
# -----------------------------------------

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("bronze-layer-test") \
        .getOrCreate()


# -----------------------------------------
# Test 1 : Bronze tables exist
# -----------------------------------------

def test_bronze_tables_exist(spark):

    tables_df = spark.sql(f"SHOW TABLES IN {BRONZE_SCHEMA}")

    for table in tables:
        assert tables_df.filter(tables_df.tableName == table).count() == 1


# -----------------------------------------
# Test 2 : Bronze tables have data
# -----------------------------------------

@pytest.mark.parametrize("table", tables)
def test_bronze_row_count(spark, table):

    df = spark.table(f"{BRONZE_SCHEMA}.{table}")

    assert df.count() > 0


# -----------------------------------------
# Test 3 : Column names are lowercase
# -----------------------------------------

@pytest.mark.parametrize("table", tables)
def test_column_lowercase(spark, table):

    df = spark.table(f"{BRONZE_SCHEMA}.{table}")

    for col_name in df.columns:
        assert col_name == col_name.lower()


# -----------------------------------------
# Test 4 : Customers table required columns
# -----------------------------------------

def test_customers_columns(spark):

    df = spark.table(f"{BRONZE_SCHEMA}.customers_table")

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
# Test 5 : Orders table required columns
# -----------------------------------------

def test_orders_columns(spark):

    df = spark.table(f"{BRONZE_SCHEMA}.orders_table")

    expected_columns = [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp"
    ]

    for col in expected_columns:
        assert col in df.columns


# -----------------------------------------
# Test 6 : Order items table required columns
# -----------------------------------------

def test_order_items_columns(spark):

    df = spark.table(f"{BRONZE_SCHEMA}.order_items_table")

    expected_columns = [
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "price"
    ]

    for col in expected_columns:
        assert col in df.columns