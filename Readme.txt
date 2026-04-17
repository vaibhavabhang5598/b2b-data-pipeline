# 📊 B2B Sales Data Pipeline (Medallion Architecture)

## 🧾 Project Overview

This project implements an end-to-end data pipeline using **PySpark and Databricks**, following the **Medallion Architecture (Bronze → Silver → Gold)** pattern.

The pipeline ingests sales data from multiple e-commerce platforms, standardizes it, and produces a clean, aggregated dataset for business reporting.

---

## 🎯 Objective

* Consolidate multi-source sales data
* Standardize schema across platforms
* Enable reliable reporting and analytics
* Build a scalable data engineering pipeline

---

## 📂 Data Sources

The pipeline processes sales data from:

* Blinkit
* Zepto
* Nykaa
* Myntra

Each source has different formats and schemas, which are normalized in the Silver layer.

---

## 🏗️ Architecture

### 🔹 Bronze Layer (Raw Data)

* Source: CSV files uploaded to Databricks
* Data stored as Delta Tables
* No transformation applied
* Tables:

  * bronze_blinkit
  * bronze_zepto
  * bronze_nykaa
  * bronze_myntra

---

### 🔹 Silver Layer (Cleaned & Standardized)

* Data cleaning and transformation
* Schema standardization:

  * date
  * sku
  * total_units
  * total_revenue
  * data_source
* Handles:

  * Different date formats
  * Column name inconsistencies
  * Data type casting

Output Table:

* b2b_silver.silver_standardized_sales

---

### 🔹 Gold Layer (Business Aggregates)

* Aggregated fact table
* Grouped by:

  * date
  * sku
  * data_source

Metrics:

* total_units
* total_revenue

Output Table:

* b2b_gold.gold_daily_sales_fact

---

## ⚙️ Pipeline Steps

### Step 1: Data Ingestion (Bronze)

* CSV files loaded into Delta tables
* (Optional step if using Databricks direct upload)

### Step 2: Silver Transformation

* Read bronze tables
* Standardize schema
* Convert date formats
* Clean invalid records
* Combine all sources

### Step 3: Gold Aggregation

* Aggregate daily sales
* Create business-ready dataset

---

## 🛠️ Technologies Used

* PySpark
* Databricks
* Delta Lake
* SQL

---

## 📈 Output Use Cases

* Sales reporting dashboards
* SKU performance analysis
* Platform-wise revenue tracking
* Daily sales monitoring

---

## 🚀 Future Enhancements

* Incremental data processing
* Partitioning for performance
* Data quality validation layer
* Dashboard integration (Power BI / Tableau)
* Automation using Databricks Jobs

---

## ✅ Key Highlights

* Implements Medallion Architecture
* Handles multi-source schema variations
* Scalable and modular design
* Business-ready aggregated data

---

## 👨‍💻 Author
Vaibhav Abhang

---
