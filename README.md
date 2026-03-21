## Project Overview

The E-commerce Sales Analytics Pipeline is an end-to-end Data Engineering project designed to process raw transactional data from an e-commerce marketplace and transform it into analytics-ready datasets for reporting and business insights.

The pipeline follows the Medallion Architecture (Bronze → Silver → Gold) using the Databricks Lakehouse platform. Raw data is ingested from AWS S3, processed using PySpark, stored in Delta Lake tables, and structured into a Star Schema model for analytics.

This system enables organizations to analyze sales performance, customer behavior, and product trends efficiently.

---

## Project Objectives

- The goal of this project is to build a **scalable, reliable, and production-ready batch ETL pipeline** for e-commerce sales analytics capable of handling large volumes of data efficiently.

- The pipeline is designed to ensure **high data quality, fault tolerance, and consistency**, enabling trusted and accurate data for business decision-making.

- It aims to deliver **analytics-ready datasets using a structured data model**, supporting efficient querying and performance optimization.

- The project focuses on generating **actionable business insights** such as sales trends, customer behavior, and product performance.

- It also emphasizes **end-to-end automation and monitoring**, ensuring smooth, scheduled, and reliable pipeline execution.

---

## Lakehouse Architecture

The pipeline follows a modern Lakehouse Data Engineering Architecture.



![object_remover-1773815864000](https://github.com/user-attachments/assets/1eff00c1-4bec-42e3-8162-ce8b6e37844d)

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

**Purpose:**  
Create clean and standardized datasets.

**Transformations include:**

- Remove duplicate records  
- Handle missing values  
- Convert timestamp formats  
- Standardize column formats  
- Perform data validation checks  
- Join related datasets  
- Apply data type casting  
- Trim whitespace and handle invalid characters  
- Add metadata columns (`processed_timestamp`, `data_quality_flag`)  

**Quarantine Handling:**

- Invalid or malformed records are not deleted  
- Stored in separate **quarantine tables** for analysis  
- Helps in:
  - Debugging data issues  
  - Maintaining data integrity  
  - Avoiding data loss  

---

**Example Silver Tables:**

| Silver Tables |
|---------------|
| customers |
| orders |
| order_items |
| payments |
| products |
| product_category_translation |
| sellers |

---

**Quarantine Tables:**

| Quarantine Tables |
|------------------|
| customer_quarantine_data |
| orders_quarantine_data |
| order_items_quarantine_data |
| payments_quarantine_data |
| products_quarantine_data |
| sellers_quarantine_data |

---

### Gold Layer – Analytics Data Model

**Purpose:**  
Create an analytics-ready star schema for business intelligence.

| Table Type   | Table Name     |
|-------------|----------------|
| Fact Table  | fact_orders    |
| Fact Table  | fact_payments  |
| Fact Table  | fact_sales     |
| Dimension   | dim_customers  |
| Dimension   | dim_products   |
| Dimension   | dim_sellers    |
| Dimension   | dim_date       |

---

**SCD Type 2 Implementation (dim_customers):**

- Implemented **Slowly Changing Dimension (SCD Type 2)** to track historical changes in customer data.

- Instead of updating records, new records are inserted when changes occur.

- Key columns used:
  - `customer_key` (surrogate key)
  - `effective_start_date`
  - `effective_end_date`
  - `is_current`

- Logic:
  - Existing record → marked as inactive (`is_current = false`)
  - New record → inserted with updated values (`is_current = true`)

- Benefits:
  - Maintains full history of customer changes  
  - Enables time-based and trend analysis  
  - Supports accurate business reporting  

---

The Gold layer supports analytical queries such as revenue analysis, customer behavior analysis, and seller performance evaluation.
---

##  Business Insights Generated

The pipeline generates business-ready insights from the Gold layer, structured into three key dashboards.

---

###  Sales Performance Insights

- Revenue Trend Analysis (Daily / Monthly)
- Total Orders and Sales Volume
- Top Selling Products by Revenue
- Revenue by Product Category
- Regional Sales Distribution


---

###  Customer Analytics Insights

- Customer Lifetime Value (CLV)
- Repeat vs New Customer Analysis
- Customer Purchase Frequency
- Average Order Value (AOV)
- Customer Segmentation (Behavior & Region)


---

###  Strategic Insights

- Demand Forecasting Trends
- Customer Churn Analysis
- Product Affinity (Market Basket Analysis)
- Seller Performance Evaluation
- Delivery and Fulfillment Performance


---

##  Analytics Dashboards

The Gold layer supports multiple business dashboards designed for different stakeholders.

---

###  Sales Performance Dashboard

**Key Metrics:**

- Total Revenue  
- Total Orders  
- Average Order Value (AOV)  
- Top Selling Products  
- Revenue by Category  
- Regional Sales  
<img width="4000" height="4422" alt="d855994d-1" src="https://github.com/user-attachments/assets/39eb0c73-8efc-4260-9ae5-8412c0771359" />
---

###  Customer Analytics Dashboard

**Key Metrics:**

- Customer Lifetime Value (CLV)  
- Repeat Customers  
- Purchase Frequency  
- Customer Segmentation  
<img width="4000" height="3339" alt="e58d4cf5-1" src="https://github.com/user-attachments/assets/284e49a2-a623-49f0-9ecf-1b3062e9a426" />

---

###  Strategic Insights Dashboard

**Key Metrics:**

- Demand Forecasting  
- Customer Churn Analysis  
- Product Affinity Insights  
- Seller Performance  
- Delivery Performance  
![Strategic_Insights_Dashboard 2026-03-19 09_37_page-0001](https://github.com/user-attachments/assets/4e2bd730-b8ce-4187-95e7-15c1cc2b23a1)
---

##  Pipeline Automation

The pipeline execution is automated using **Databricks Workflows and AWS Airflow** to ensure reliable and scheduled processing.

**Schedule:**  
Daily Batch Job  
Time: 3 AM UTC  

**Workflow Design:**

- Created a **Databricks Job** where each notebook is configured as an individual task:
  - Bronze ingestion notebook  
  - Silver transformation notebook  
  - Gold modeling notebook  
  - Data validation and quality checks  

- Defined task dependencies to ensure proper execution order across layers.

- Integrated **AWS Airflow** to orchestrate the Databricks job:
  - Airflow triggers the Databricks workflow  
  - Manages scheduling and pipeline execution  
  - Ensures end-to-end orchestration across all stages  

- Configured **Slack alerts** for real-time notifications:
  - Job success/failure alerts  
  - Data quality issue alerts  
  - Pipeline failure monitoring  

---

**Workflow tasks include:**

1. Data ingestion from AWS S3 (Raw Zone)  
2. Bronze layer creation (Delta tables)  
3. Silver layer transformation (data cleaning, validation, quarantine handling)  
4. Gold layer modeling (Star Schema + SCD Type 2)  
5. Data quality checks and validation  
6. Analytics table preparation for dashboards  

---

##  Data Quality and Monitoring

The pipeline implements strong **data quality and monitoring mechanisms** to ensure reliability and accuracy.

**Data Quality Checks:**

- Record count validation  
- Schema validation  
- Null value handling and checks  
- Duplicate detection  
- Data validation rules (invalid/malformed data detection)  

**Quarantine Handling:**

- Invalid records are stored in **quarantine tables** instead of being dropped  
- Enables debugging, traceability, and business review  

**Monitoring & Logging:**

- Errors and pipeline issues are logged in:
  `logs.etl_errors`  

- Alerts are triggered for:
  - Job failures  
  - Data quality issues  
  - Pipeline anomalies  

---

##  Key Project Outcomes

- Built a **scalable lakehouse data pipeline** using AWS S3 and Databricks  
- Implemented **Medallion Architecture (Bronze, Silver, Gold)** for structured data processing  
- Developed **robust data quality framework** with validation and quarantine handling  
- Designed an **analytics-ready Star Schema model** with SCD Type 2 implementation  
- Enabled **business-ready insights and dashboards** for analytics use cases  
- Automated the pipeline using **Databricks Workflows and Airflow** for reliable execution  

---

##  Future Improvements

- Integrate **real-time streaming ingestion** using Spark Structured Streaming  
- Enhance analytics with **advanced forecasting and ML models**  
- Deploy dashboards using **Power BI / Databricks SQL dashboards**  
- Implement **CI/CD pipelines** for automated testing and deployment  
- Add **data lineage and governance** using Unity Catalog enhancements  

---

## Conclusion

This project demonstrates how a modern data engineering pipeline can transform raw e-commerce data into reliable analytics datasets and business insights using the Lakehouse architecture.

The system integrates AWS S3, Databricks, PySpark, Delta Lake, and SQL analytics to deliver scalable and efficient data processing for business intelligence.
