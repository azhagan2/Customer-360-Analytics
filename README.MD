# AWS Medallion Data Engineering Pipeline
*(Terraform + Glue + Airflow)*

This repository implements an **end-to-end cloud-native data engineering platform** designed to **extract, transform, and store data for analytics and BI consumption** using AWS services and Infrastructure as Code (IaC).

The solution follows the **Medallion Architecture (Bronze → Silver → Gold)** and is fully automated, reproducible, and environment-agnostic.

---

## 🎯 Project Objective

The core goal of this project is to:
- Extract data from **RDS (inside VPC)** and **DynamoDB**
- Land raw data into **Amazon S3 (Bronze layer)**
- Transform data using **AWS Glue ETL (PySpark)**
- Promote curated datasets to **Silver & Gold layers**
- Orchestrate pipelines using **Apache Airflow**
- Manage infrastructure using **Terraform**
- Enable **secure BI access** via Athena + Lake Formation

---

## 🏗️ High-Level Architecture

**Data Flow Overview**
RDS (VPC) / DynamoDB → Terraform Ingestion → S3 Bronze → Glue ETL → S3 Silver → S3 Gold → Athena → BI Tools

![Architecture](service_architecture_2.png)

---

## 🧱 Medallion Architecture

| Layer | Purpose |
|------|--------|
| **Bronze** | Raw ingested data from RDS & DynamoDB |
| **Silver** | Cleaned, standardized, business-ready data |
| **Gold** | Aggregated, analytics-optimized datasets |

---

## 🛠️ Tech Stack & Services

| Category | Service | Logo | Description |
|-------|--------|:------:|------------|
| **IaC** | **Terraform** | <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/8/84/Sdcsdxc.jpg/640px-Sdcsdxc.jpg" width="40" height="40" style="background-color:white; padding:4px; border-radius:4px;"/> | Infrastructure provisioning across environments |
| **ETL** | **AWS Glue** | <img src="https://upload.wikimedia.org/wikipedia/commons/1/11/AWS_Glue.png" width="35"/> | Serverless ETL for Bronze → Silver → Gold |
| **Compute** | **AWS Lambda** | <img src="https://upload.wikimedia.org/wikipedia/commons/8/8f/Orange_lambda.svg" width="30"/> | Lightweight validation & triggers |
| **Orchestration** | **Apache Airflow** | <img src="https://airflow.apache.org/images/feature-image.png" width="60"/> | DAG-based orchestration & dependency control |
| **Storage** | **Amazon S3** | <img src="https://upload.wikimedia.org/wikipedia/commons/b/bc/Amazon-S3-Logo.svg" width="35"/> | Data Lake for all medallion layers |
| **Governance** | **Lake Formation** | <img src="https://icon.icepanel.io/AWS/svg/Analytics/Lake-Formation.svg" width="35"/> | Fine-grained data access control |
| **Analytics** | **Amazon Athena** | <img src="https://icon.icepanel.io/AWS/svg/Analytics/Athena.svg" width="35"/> | SQL analytics on S3 |

---

## ⚙️ Infrastructure as Code (Terraform)

### 1️⃣ Ingestion (Bronze Layer)
Responsible for **secure data extraction from RDS inside VPC**.
Creates: VPC Endpoints, Glue Connections (JDBC), Crawlers, and IAM Roles. Ensures **private ingestion** without public internet exposure.

### 2️⃣ Transformation (Silver & Gold)
Used to deploy **Glue ETL pipelines** across environments (`dev / stage / prod`).
Creates: Glue Jobs, Crawlers, and Job parameters. Terraform **reduces manual effort** and deployment errors.

---

## 🔄 ETL & CI/CD Workflow

- ETL code written in **PySpark** and packaged as a **Python Wheel (.whl)**.
- **GitHub Actions** triggers on code push:
  1. Builds the Wheel file.
  2. Uploads the latest artifact to **S3**.
  3. Ensures **version-controlled ETL code** in Glue.

---

## 📊 Analytics & BI Consumption

- Data queried using **Amazon Athena** and connected via **ODBC**.
- Visualized in **Power BI, Redash, or Metabase**.
- **Lake Formation** governs table-level and column-level permissions.

---

## ✅ Key Benefits

- **Fully Automated:** No manual console clicks.
- **Secure:** VPC-based ingestion and encrypted S3 buckets.
- **Scalable:** Serverless architecture handles growing data volumes.
- **Standardized:** Medallion architecture ensures data quality.

---

## 📄 License
MIT License
