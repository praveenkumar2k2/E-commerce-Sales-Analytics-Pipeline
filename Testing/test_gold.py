import pytest
from pyspark.sql import SparkSession

GOLD_SCHEMA = "ecommerce_catalog.gold"

tables = [
    "dim_customers",
    "dim_products",
    "dim_sellers",
    "dim_orders",
    "dim_date",
    "fact_sales"
]

# -----------------------------------------
# Spark Session Fixture
# -----------------------------------------

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("gold-layer-test") \
        .getOrCreate()


# -----------------------------------------
# Test 1 : Gold tables exist
# -----------------------------------------

def test_gold_tables_exist(spark):

    tables_df = spark.sql(f"SHOW TABLES IN {GOLD_SCHEMA}")

    for table in tables:
        assert tables_df.filter(tables_df.tableName == table).count() == 1


# -----------------------------------------
# Test 2 : Gold tables contain data
# -----------------------------------------

@pytest.mark.parametrize("table", tables)
def test_gold_row_count(spark, table):

    df = spark.table(f"{GOLD_SCHEMA}.{table}")

    assert df.count() > 0


# -----------------------------------------
# Test 3 : No duplicate primary keys in dimensions
# -----------------------------------------

def test_dim_customers_no_duplicates(spark):

    df = spark.table(f"{GOLD_SCHEMA}.dim_customers")

    duplicates = df.groupBy("customer_id").count().filter("count > 1").count()

    assert duplicates == 0


def test_dim_products_no_duplicates(spark):

    df = spark.table(f"{GOLD_SCHEMA}.dim_products")

    duplicates = df.groupBy("product_id").count().filter("count > 1").count()

    assert duplicates == 0


def test_dim_sellers_no_duplicates(spark):

    df = spark.table(f"{GOLD_SCHEMA}.dim_sellers")

    duplicates = df.groupBy("seller_id").count().filter("count > 1").count()

    assert duplicates == 0


def test_dim_orders_no_duplicates(spark):

    df = spark.table(f"{GOLD_SCHEMA}.dim_orders")

    duplicates = df.groupBy("order_id").count().filter("count > 1").count()

    assert duplicates == 0


# -----------------------------------------
# Test 4 : Fact table columns exist
# -----------------------------------------

def test_fact_sales_columns(spark):

    df = spark.table(f"{GOLD_SCHEMA}.fact_sales")

    expected_columns = [
        "order_id",
        "customer_id",
        "order_date",
        "product_id",
        "seller_id",
        "payment_seq",
        "payment_type",
        "payment_installments",
        "payment_value",
        "product_price",
        "freight_value"
    ]

    for col in expected_columns:
        assert col in df.columns


# -----------------------------------------
# Test 5 : Fact table values should not be negative
# -----------------------------------------

def test_fact_sales_positive_values(spark):

    df = spark.table(f"{GOLD_SCHEMA}.fact_sales")

    invalid_price = df.filter(df.product_price < 0).count()
    invalid_payment = df.filter(df.payment_value < 0).count()

    assert invalid_price == 0
    assert invalid_payment == 0


# -----------------------------------------
# Test 6 : Fact table customer_id exists in dimension
# -----------------------------------------

def test_fact_customer_fk(spark):

    fact_df = spark.table(f"{GOLD_SCHEMA}.fact_sales")
    dim_df = spark.table(f"{GOLD_SCHEMA}.dim_customers")

    missing = fact_df.join(
        dim_df,
        fact_df.customer_id == dim_df.customer_id,
        "left_anti"
    ).count()

    assert missing == 0


# -----------------------------------------
# Test 7 : Fact table product_id exists in dimension
# -----------------------------------------

def test_fact_product_fk(spark):

    fact_df = spark.table(f"{GOLD_SCHEMA}.fact_sales")
    dim_df = spark.table(f"{GOLD_SCHEMA}.dim_products")

    missing = fact_df.join(
        dim_df,
        fact_df.product_id == dim_df.product_id,
        "left_anti"
    ).count()

    assert missing == 0


# -----------------------------------------
# Test 8 : Fact table seller_id exists in dimension
# -----------------------------------------

def test_fact_seller_fk(spark):

    fact_df = spark.table(f"{GOLD_SCHEMA}.fact_sales")
    dim_df = spark.table(f"{GOLD_SCHEMA}.dim_sellers")

    missing = fact_df.join(
        dim_df,
        fact_df.seller_id == dim_df.seller_id,
        "left_anti"
    ).count()

    assert missing == 0


# -----------------------------------------
# Test 9 : Fact table order_id exists in order dimension
# -----------------------------------------

def test_fact_order_fk(spark):

    fact_df = spark.table(f"{GOLD_SCHEMA}.fact_sales")
    dim_df = spark.table(f"{GOLD_SCHEMA}.dim_orders")

    missing = fact_df.join(
        dim_df,
        fact_df.order_id == dim_df.order_id,
        "left_anti"
    ).count()

    assert missing == 0