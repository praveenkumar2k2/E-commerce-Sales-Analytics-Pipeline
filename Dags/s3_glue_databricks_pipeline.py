from airflow import DAG
from airflow.utils import timezone
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import timedelta

# -----------------------------
# DAG default arguments
# -----------------------------
default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# -----------------------------
# DAG definition
# -----------------------------
with DAG(
    dag_id="s3_glue_databricks_pipeline",
    default_args=default_args,
    description="Pipeline for 7 CSVs from S3 to Glue to Databricks",
    schedule="@daily",
    start_date=timezone.datetime(2026, 3, 14),
    catchup=False,
) as dag:

    # List of CSV files in S3
    csv_files = [
        "customer_dataset/customer_dataset.csv",
        "orders_dataset/orders_datasets.csv",
        "product_datasets/product_datasets.csv",
        "product_category_name_translation/product_category_name_translation.csv",
        "sellers_datasets/sellers_datasets.csv",
        "order_items_dataset/order_items_dataset.csv",
        "order_payment_dataset/order_payment_dataset.csv"
    ]

    # -----------------------------
    # Task 1: Wait for all S3 files
    # -----------------------------
    wait_for_s3_files = []
    for file in csv_files:
        sensor = S3KeySensor(
            task_id=f"wait_for_{file.split('/')[0]}",
            bucket_key=f"raw/ecommerce_sales/{file}",  # remove 's3://'
            bucket_name="ecommerce-analytics-datalake1",
            aws_conn_id="aws_default",
            poke_interval=60,
            timeout=600,
        )
        wait_for_s3_files.append(sensor)

    # -----------------------------
    # Task 2: Run AWS Glue job
    # -----------------------------
    glue_job = GlueJobOperator(
    task_id="run_glue_job",
    job_name="ecommerce-job",
    script_location="s3://my-glue-airflow/scripts/glue_etl_script.py",
    iam_role_name="AWSGlueServiceRole",
    region_name="ap-south-1",
    wait_for_completion=True
)

    # -----------------------------
    # Task 3: Trigger Databricks Job (Bronze → Silver → Gold)
    # -----------------------------
    databricks_job = DatabricksRunNowOperator(
        task_id="run_databricks_job",
        databricks_conn_id="databricks_default",
        job_id=1103378867252484,  # your Databricks Job ID
        notebook_params={"env": "prod"}
    )

    # -----------------------------
    # DAG task dependencies
    # -----------------------------
    wait_for_s3_files >> glue_job >> databricks_job