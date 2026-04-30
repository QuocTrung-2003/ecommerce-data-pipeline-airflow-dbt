# 🛒 E-commerce Data Pipeline (Airflow + dbt + PostgreSQL)

## 📌 Overview
This project builds an end-to-end data pipeline for an e-commerce system, transforming raw API data into analytics-ready datasets for BI tools (e.g., Power BI).

The architecture follows a modern data stack approach:

- Data Ingestion → Mock API  
- Orchestration → Airflow  
- Transformation → dbt  
- Storage → PostgreSQL  
- Consumption → BI (read-only user)

## 🏗️ Architecture
          +------------------+
          |    Mock API      |
          +--------+---------+
                   |
                   v
          +------------------+
          |    Airflow       |
          | (Orchestration)  |
          +--------+---------+
                   |
                   v
          +------------------+
          |   PostgreSQL     |
          |------------------|
          | stg_raw          |
          | analytics_int    |
          | analytics_marts  |
          +--------+---------+
                   |
                   v
          +------------------+
          |      dbt         |
          | (Transformations)|
          +--------+---------+
                   |
                   v
          +------------------+
          |   BI Tools       |
          | (Power BI)       |
          +------------------+
## ⚙️ Tech Stack
| Layer          | Tool                  |
|----------------|-----------------------|
| Orchestration  | Apache Airflow        |
| Transformation | dbt (Postgres adapter)|
| Database       | PostgreSQL 16         |
| API Source     | FastAPI (mock service)|
| Containerization| Docker Compose       |
## 📂 Project Structure


## 🔄 Data Flow
1. **Ingestion**  
   - Airflow calls Mock API  
   - Raw data → `stg_raw`

2. **Transformation (dbt)**  
   - `stg_raw` → cleaned into `analytics_int`  
   - `analytics_int` → modeled into `analytics_marts`

3. **Consumption**  
   - BI tools connect to `analytics_marts` via read-only user `bi_read`
## 🔐 BI User (Security Design)
- ✅ Allowed: `SELECT` on `stg_raw`, `analytics_marts`  
- ❌ Denied: `analytics_int`, `public`

## 🔑 Key Features
- Credentials stored in Docker secrets  
- User created via `.sh` script at container startup  
- Permissions managed separately via SQL  

## 🚀 Getting Started
*(Thêm hướng dẫn cài đặt chi tiết nếu cần)*

## 📈 Future Improvements
- Add data quality checks (dbt tests)  
- Implement incremental models  
- Add monitoring (Great Expectations / Airflow alerts)  
- Deploy to cloud (GCP / AWS)  

## 🎯 Learning Outcomes
This project demonstrates:
- Building end-to-end data pipelines  
- Using Airflow for orchestration  
- Applying dbt for transformation  
- Designing data warehouse layers  
- Implementing role-based access control 