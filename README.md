# 🚀 Data Warehouse and Analytics Portfolio Project

This is my personal portfolio project, designed to showcase a **full-cycle Data Warehousing and Business Intelligence (BI) solution**. My primary goal was to apply industry best practices in **Data Engineering** to construct a robust, scalable data platform for consolidating sales data and generating high-value strategic insights.

The project demonstrates my ability to design, implement, and maintain an efficient analytical data infrastructure.

---

## 🏗️ Data Architecture: The Medallion Standard

I implemented the **Medallion Architecture** (Bronze, Silver, and Gold layers) to organize the data flow, ensuring data quality, traceability, and high performance:

* **Bronze Layer (Raw):** The ingestion layer. Stores raw, untouched data from source systems (CSV Files) within an **SQL Server** database.
* **Silver Layer (Cleaned):** The **Data Quality** layer. Data is cleansed, standardized, and normalized, preparing it for deeper analysis.
* **Gold Layer (BI-Ready):** The consumption layer. Data is modeled into an optimized **Star Schema** for direct use by BI tools and complex analytical queries.

## 📄 CSV DataFrame Naming Convention Standard

To ensure correct data processing and proper type mapping during the import process (as defined by the `column_type` below), your CSV files **must adhere to a specific naming standard** for key columns.

The system uses **substrings within the column names** to automatically infer the data type.

| Desired Data Type | Mandatory Substring in Column Name | Valid Column Name Examples |
| :---------------: | :--------------------------------: | :------------------------: |
| **`INT`** (Identifier) | **`_id`** | `collaborator_id`, `vehicle_id`, `route_id` |
| **`DATE`** (Date) | **`date`** | `boarding_date`, `scheduled_date`, `end_date` |

### 🔍 Detailed Mapping Logic 

The column type mapping logic in the system is executed as follows:

```python
column_type = {
    "_id": "INT",
    "date": "DATE",
    "object": "NVARCHAR(50)",
    "int64": "INT",
    "float64": "FLOAT"
}
```

You can dit the file located at:

```
scripts/config/column_type_config.json
```

### 💡 Essential Requirements:

* **Identification Columns:** Columns representing keys or IDs **must** contain the substring **`id`** in their name.
    * *Example: Use `collaborator_id` instead of `collaborator`.*
* **Date Columns:** Columns containing date values **must** contain the substring **`date`** in their name.
    * *Example: Use `travel_date` instead of `travel_day`.*

# 🚀 Getting Started with the dbt + SQL Server Project

This guide explains how to set up your SQL Server database, configure dbt, and create your initial schema (`bronze`) to start running your models.

---

## 🧰 Installation

Before running the dbt project, make sure you have **Python 3.8+** installed and your virtual environment activated (recommended).

Then install dbt with SQL Server support using the following command:

```bash
python -m pip install dbt-core dbt-sqlserver
```

This will install:

- dbt-core → the main dbt framework
- dbt-sqlserver → the adapter that allows dbt to connect to Microsoft SQL Server

💡 Tip:
If you are using a virtual environment (like .venv), make sure it’s activated before running the installation command:

## 🧱 Step 1 — Create the Database in SQL Server

Open **SQL Server Management Studio (SSMS)** and run the following command:

```sql
CREATE DATABASE DataWarehouse;
```
## ⚙️ Step 2 — Configure the profiles.yml File

Edit the file located at:

```
sqlcreator/.dbt/profiles.yml
```

Update the server and port fields according to your SQL Server host and connection settings.

💡 Important:
Make sure the top-level key (sql_dataware_house_project) matches the value of the profile: field in your dbt_project.yml.

## 🧩 Step 3 — Initialize Your Project and Create the Schemas/Tables

Once your connection settings are configured, you can initialize the database structure (schemas and tables) automatically by running the Python setup script.

### ✅ Run the Python script
From the root of your project directory, execute:

```bash
python scripts/start_project/main.py
```

🔍 What this step does
The script orchestrates the entire project initialization and analysis workflow by performing the following sequential steps:

#### Schema and Model Initialization:
- Reads the initial schema configuration (scripts/config/schema.json).
- Runs dbt run-operation commands to create the foundational database schemas (e.g., bronze, silver, gold) and their corresponding empty model folders within the project structure.
#### Data Analysis and Type Mapping (Profiling):
- Recursively scans all CSV files in the datasets/ directory.
- Generates an HTML profiling report for each dataset (e.g., using ydata-profiling).
- Infers and maps data types, combining Pandas' detected dtypes with custom standardization rules defined in your configuration.
#### Configuration Output:
- Exports the consolidated, standardized column definitions for all datasets to the main configuration file (scripts/config/bronze/column_types.json). This file is used in the next step.
#### Table Creation and Data Loading (DDL/DML):
- Iterates through the standardized column definitions (column_types.json).
- For each dataset, runs dbt run-operation commands to:
- Create the physical table (DDL) in the database (e.g., under the bronze schema) with the correct column types.
- Load data (DML) from the corresponding raw CSV file into the newly created table.