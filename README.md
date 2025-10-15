# sales-olap

# 📊 Data Warehouse Query Processing Project

This project was developed as part of the **Advanced Database** course. It demonstrates the complete lifecycle of **building and querying a data warehouse**, integrating concepts of **dimensional modeling, ETL pipelines, OLAP operations, and query optimization**.

---

## Development

1. Environment Setup
   Clone the repository.
   Install Python 3.x and required packages (pandas, sqlalchemy, mysql-connector-python).
   Set up a MySQL database instance for the warehouse.
2. ETL Pipeline
   Create a `.env` variable, look at the `.env.example` for the required fields
   Configure database connection in app.py.
   Run ETL scripts to extract, clean, and load data into the warehouse schema

## 🚀 Project Objectives

The project aims to achieve the following learning competencies:

### 🏗️ Data Warehouse Design

- Build a data warehouse using either **Star** or **Snowflake schema**.
- Perform restructuring of source tables and apply **denormalization** when needed.

### 🔄 ETL Process

- Develop **ETL scripts** in Python/SQL to load data from a public dataset into the warehouse.
- Apply data wrangling tasks such as **cleaning, splitting, merging, aggregating, and transforming** data.

### 📈 OLAP Application

- Implement analytical queries that demonstrate OLAP operations:
  - **Roll-up** → aggregation up the hierarchy
  - **Drill-down** → finer granularity of data
  - **Slice** → filter by dimension value
  - **Dice** → filter by multiple dimensions
- Build a **web-based interface** to display analytical reports in an organized manner.

### ⚡ Query Optimization & Performance

- Apply query optimization strategies:
  - Efficient **query formulation**
  - Use of **indexes**
  - **Database restructuring**
- Measure and evaluate their impact on **query performance** and **reporting speed**.

---

## 🛠️ Tech Stack

- **Database:** MySQL, PostgreSQL
- **ETL:** Python (Pandas, SQLAlchemy) / SQL scripts
- **OLAP & Queries:** SQL (aggregation, rollup, drilldown, window functions)
- **Frontend:** Web-based interface (React) for report visualization

---

---
