# E-commerce Sales Analytics Pipeline

## Project Overview

The E-commerce Sales Analytics Pipeline is an end-to-end Data Engineering project designed to process raw transactional data from an e-commerce marketplace and transform it into analytics-ready datasets for reporting and business insights.

The pipeline follows the Medallion Architecture (Bronze → Silver → Gold) using the Databricks Lakehouse platform. Raw data is ingested from AWS S3, processed using PySpark, stored in Delta Lake tables, and structured into a Star Schema model for analytics.

This system enables organizations to analyze sales performance, customer behavior, and product trends efficiently.

---

## Project Objectives

The objective of this project is to build a scalable batch ETL pipeline for an e-commerce sales analytics system that can efficiently handle large volumes of data. The pipeline processes raw transactional data stored in Amazon S3 and uses PySpark on Databricks to perform data cleaning, transformation, and validation to ensure high data quality. The project also focuses on designing an analytics-ready star schema model that makes it easier to analyze data and run queries. Using SQL, the pipeline generates useful business insights and dashboards such as sales trends, customer behavior, and product performance. Finally, the entire pipeline is automated using Databricks Workflows, enabling smooth, scheduled, and reliable execution without manual intervention.

---

## System Architecture

The pipeline follows a modern Lakehouse Data Engineering Architecture.

![e-commerce system architecture](https://github.com/user-attachments/assets/5c83014e-8ffb-4c2a-82ed-f702183537dc)

---

## Technology Stack

| Technology | Purpose |
|------------|--------|
| AWS S3 | Data Lake Storage |
| Databricks | Data Engineering Platform |
| PySpark | Distributed Data Processing |
| Delta Lake | Optimized Storage Format |
| Unity Catalog | Metadata Management |
| SQL | Analytical Queries |
| Databricks Workflows | Pipeline Scheduling |
| Git | Version Control |
| dbt | Advanced Data Transformations |

---

## Dataset

Dataset Used:

Brazilian E-Commerce Public Dataset (Olist) from Kaggle.

The dataset contains transactional information related to:

- Customers
- Orders
- Order Items
- Payments
- Products
- Sellers
- Product Categories

Example Raw Storage Path:
s3://ecommerce-data-lake/raw/ecommerce_sales/YYYY/MM/DD/


---

## Data Pipeline Layers

### Bronze Layer – Raw Data Ingestion

Purpose:
Store raw data in Delta format.

Tasks performed:

- Read CSV datasets from AWS S3
- Convert raw data into Delta tables
- Preserve original data with minimal transformation
- Add ingestion metadata

Example Bronze Tables:
| Bronze Tables |
|---------------|
| bronze_customers |
| bronze_orders |
| bronze_order_items |
| bronze_payments |
| bronze_products |
| bronze_sellers |



---

### Silver Layer – Data Cleaning and Transformation

Purpose:
Create clean and standardized datasets.

Transformations include:

- Remove duplicate records
- Handle missing values
- Convert timestamp formats
- Standardize column formats
- Perform data validation checks
- Join related datasets

Example Silver Tables:
| Silver Tables |
|---------------|
| customers_clean |
| orders_clean |
| order_items_clean |
| payments_clean |
| products_clean |
| sellers_clean |



---

### Gold Layer – Analytics Data Model

Purpose:
Create an analytics-ready star schema for business intelligence.

| Table Type | Table Name |
|-----------|------------|
| Fact Table | fact_sales |
| Dimension | dim_customers |
| Dimension | dim_products |
| Dimension | dim_sellers |
| Dimension | dim_orders |
| Dimension | dim_date |



The Gold layer supports analytical queries such as revenue analysis, customer behavior analysis, and seller performance evaluation.

---

## Business Insights Generated

The pipeline enables approximately 15 business insights categorized into three analytics levels.

### Descriptive Analytics

- Total revenue trend
- Total orders per day
- Top selling products
- Sales by product category
- Customer distribution by region

### Diagnostic Analytics

- Customer purchase frequency
- Average order value
- Seller performance analysis
- Delivery performance
- Shipping cost impact

### Strategic / Predictive Analytics

- Customer lifetime value (CLV)
- Customer segmentation
- Product affinity analysis
- Demand forecasting
- Customer churn detection

---

## Analytics Dashboards

The Gold layer supports multiple business dashboards.

### Sales Performance Dashboard

Key metrics:

- Total revenue
- Total orders
- Average order value
- Top selling products
- Revenue by category

### Customer Analytics Dashboard

Key metrics:

- Customer lifetime value
- Repeat customers
- Purchase frequency
- Customer segmentation

### Strategic Insights Dashboard

Key metrics:

- Demand forecasting
- Customer churn analysis
- Product affinity insights

---

## Pipeline Automation

The pipeline execution is automated using Databricks Workflows.

Schedule:
Daily Batch Job
Time: 3 AM UTC


Workflow tasks include:

1. Bronze ingestion
2. Silver transformation
3. Gold modeling
4. Data quality checks
5. Analytics table refresh

---

## Data Quality and Monitoring

The pipeline includes several data quality validation checks.

- Record count validation
- Schema validation
- Null value checks
- Duplicate detection
- Data anomaly detection

Errors and pipeline issues are logged in:
ecommerce_catalog.logs.etl_errors



Alerts are triggered for job failures or abnormal data patterns.

---

## My Role in the Project

Role:
Project Lead and Data Quality / Pipeline Orchestration Engineer

Responsibilities:

- Led the development of the end-to-end e-commerce analytics pipeline
- Implemented data quality monitoring and validation checks
- Managed Bronze, Silver, and Gold layer validation
- Configured pipeline scheduling using Databricks Workflows
- Performed advanced transformations using dbt and PySpark
- Validated business insights generated from the Gold analytics layer

---

## Key Project Outcomes

- Built a scalable lakehouse data pipeline
- Implemented Medallion Architecture
- Designed star schema data warehouse model
- Enabled business-ready analytics datasets
- Automated daily ETL pipeline execution

---

## Future Improvements

- Integrate real-time streaming data ingestion
- Implement machine learning-based demand forecasting
- Deploy dashboards using Power BI or Streamlit
- Implement CI/CD for data pipelines
---
## Conclusion

This project demonstrates how a modern data engineering pipeline can transform raw e-commerce data into reliable analytics datasets and business insights using the Lakehouse architecture.

The system integrates AWS S3, Databricks, PySpark, Delta Lake, and SQL analytics to deliver scalable and efficient data processing for business intelligence.
