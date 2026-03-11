# E-commerce-Sales-Analytics-Pipeline
## Project Overview

This project implements an end-to-end **Data Engineering pipeline** for analyzing e-commerce sales data using **AWS, Databricks, PySpark, Delta Lake, and SQL**.

The pipeline ingests raw transactional data from **AWS S3**, processes it using **Databricks**, and transforms it through the **Medallion Architecture (Bronze → Silver → Gold)** to produce **analytics-ready datasets**.

The final **Gold Layer** is designed using a **Star Schema** to support business analytics, dashboards, and reporting.

---

## Business Objective

The objective of this pipeline is to enable business teams to analyze:

* Sales performance trends
* Customer purchasing behavior
* Product demand patterns
* Seller performance
* Logistics and delivery efficiency

The Gold Layer generates **business insights and analytics datasets** used for dashboards and strategic decision making.

---

## Architecture Overview

```
Raw Data Source (CSV files)
        │
        ▼
AWS S3 Data Lake
        │
        ▼
Databricks Ingestion (PySpark)
        │
        ▼
Bronze Layer
Raw Delta Tables
        │
        ▼
Silver Layer
Cleaned and Transformed Data
        │
        ▼
Gold Layer
Star Schema (Fact + Dimension Tables)
        │
        ▼
Business Insights
        │
        ▼
Dashboards / Analytics
```

---

## Medallion Architecture

This project follows the **Medallion Architecture**, a standard pattern used in modern data engineering systems.

### Bronze Layer (Raw Data)

Purpose:

* Store raw data ingested from the source
* Maintain historical raw datasets
* Minimal transformation

Characteristics:

* Data stored as **Delta tables**
* Append-only ingestion
* Schema inference applied

Example Bronze Tables:

* bronze_customers
* bronze_orders
* bronze_order_items
* bronze_payments
* bronze_products
* bronze_sellers

---

### Silver Layer (Data Cleaning & Transformation)

Purpose:

* Clean and standardize raw data
* Apply data quality checks
* Remove duplicates and handle missing values

Transformations include:

* Null handling
* Data type conversion
* Timestamp standardization
* Deduplication
* Data validation

Example Silver Tables:

* customers_clean
* orders_clean
* order_items_clean
* payments_clean
* products_clean
* sellers_clean

---

### Gold Layer (Analytics Layer)

Purpose:

* Create analytics-ready datasets
* Build dimensional data model
* Enable business intelligence queries

The Gold layer is designed using **Star Schema modeling**.

Fact Table:

fact_sales

Dimension Tables:

* dim_customers
* dim_products
* dim_sellers
* dim_orders
* dim_date

This model enables efficient analytical queries such as revenue trends, product performance, and customer behavior analysis.

---

## Star Schema Data Model

```
              dim_customers
                     │
                     │
dim_products ─── fact_sales ─── dim_sellers
                     │
                     │
                  dim_orders
                     │
                     │
                   dim_date
```

Fact Table Grain:

One row per **order item purchased**.

Metrics stored in fact table:

* product price
* freight value
* payment value

---

## Data Pipeline Workflow

### Step 1 – Data Ingestion

Raw CSV datasets are uploaded to **AWS S3 Raw Zone**.

Example path:

```
s3://ecommerce-data-lake/raw/
```

Databricks reads these files using **PySpark**.

---

### Step 2 – Bronze Layer Creation

Raw CSV files are converted into **Delta tables**.

Example process:

```
CSV → Spark DataFrame → Delta Table
```

Delta format provides:

* ACID transactions
* Schema enforcement
* Time travel
* Scalable storage

---

### Step 3 – Silver Layer Transformation

Data cleaning and transformations are performed using **PySpark and SQL**.

Key operations:

* Removing duplicates
* Handling missing values
* Standardizing timestamps
* Data validation

The cleaned datasets are stored as **Silver Delta tables**.

---

### Step 4 – Gold Layer Modeling

The Gold layer builds the **analytics data model**.

Operations performed:

* Joining Silver tables
* Creating fact table
* Creating dimension tables
* Aggregating business metrics

This layer supports analytical queries used for business insights.

---

## Example Business Insights

The Gold layer enables generation of multiple business insights.

Examples include:

### Revenue Trend Analysis

Track daily and monthly revenue trends.

### Top Selling Products

Identify products generating the highest sales.

### Customer Lifetime Value

Determine high-value customers.

### Seller Performance

Evaluate top performing sellers.

### Delivery Efficiency

Measure average delivery time and logistics performance.

---

## Example Analytical SQL Query

Example: Monthly Revenue

```sql
SELECT 
DATE_TRUNC('month', order_purchase_timestamp) AS month,
SUM(payment_value) AS monthly_revenue
FROM gold.fact_sales
GROUP BY month
ORDER BY month;
```

---

## Dashboards Built from Gold Layer

Three main dashboards can be built using this dataset.

### Sales Performance Dashboard

Metrics:

* Total revenue
* Total orders
* Top products
* Revenue by category

### Customer Analytics Dashboard

Metrics:

* Customer lifetime value
* Repeat customers
* Regional sales

### Strategic Insights Dashboard

Metrics:

* Seller performance
* Product demand trends
* Delivery performance

---

## Technology Stack

| Technology           | Purpose                               |
| -------------------- | ------------------------------------- |
| AWS S3               | Data lake storage                     |
| Databricks           | Data processing platform              |
| PySpark              | Distributed data transformation       |
| Delta Lake           | Storage format with ACID transactions |
| Unity Catalog        | Metadata management                   |
| Databricks SQL       | Analytical queries                    |
| Databricks Workflows | Pipeline scheduling                   |

---

## Pipeline Scheduling

The pipeline can be automated using **Databricks Workflows**.

Execution schedule:

Daily batch processing

Pipeline stages executed:

```
Bronze → Silver → Gold
```

---

## Monitoring and Logging

Monitoring mechanisms ensure pipeline reliability.

Logs include:

* pipeline execution logs
* error tracking
* data quality alerts

---

## Key Data Engineering Concepts Demonstrated

This project demonstrates several modern data engineering concepts:

* Medallion Architecture
* Delta Lake Data Management
* Dimensional Data Modeling
* Star Schema Design
* Distributed Data Processing
* Data Lakehouse Architecture
* Analytical SQL Queries

---

## Future Improvements

Possible enhancements include:

* Streaming ingestion using Spark Structured Streaming
* Real-time dashboards
* Machine learning models for demand forecasting
* Data quality monitoring using Delta Live Tables

---

## Author

Praveen Kumar

Data Engineering Trainee

Skills:

Python | SQL | PySpark | Databricks | Hadoop | Spark | AWS | Data Engineering
