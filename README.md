# 🚀 Data Warehouse and Analytics Portfolio Project

This is my personal portfolio project, designed to showcase a **full-cycle Data Warehousing and Business Intelligence (BI) solution**. My primary goal was to apply industry best practices in **Data Engineering** to construct a robust, scalable data platform for consolidating sales data and generating high-value strategic insights.

The project demonstrates my ability to design, implement, and maintain an efficient analytical data infrastructure.

---

## 🏗️ Data Architecture: The Medallion Standard

I implemented the **Medallion Architecture** (Bronze, Silver, and Gold layers) to organize the data flow, ensuring data quality, traceability, and high performance:

* **Bronze Layer (Raw):** The ingestion layer. Stores raw, untouched data from source systems (CSV Files) within an **SQL Server** database.
* **Silver Layer (Cleaned):** The **Data Quality** layer. Data is cleansed, standardized, and normalized, preparing it for deeper analysis.
* **Gold Layer (BI-Ready):** The consumption layer. Data is modeled into an optimized **Star Schema** for direct use by BI tools and complex analytical queries.
