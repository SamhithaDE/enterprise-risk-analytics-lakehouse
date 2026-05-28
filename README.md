# 🏦 Enterprise Risk Analytics Lakehouse Platform

[![Azure](https://img.shields.io/badge/Azure-0078D4?style=flat-square&logo=microsoftazure&logoColor=white)](https://azure.microsoft.com)
[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat-square&logo=databricks&logoColor=white)](https://databricks.com)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-003366?style=flat-square&logoColor=white)](https://delta.io)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=flat-square&logo=apachespark&logoColor=white)](https://spark.apache.org)
[![Azure Purview](https://img.shields.io/badge/Azure_Purview-0078D4?style=flat-square&logo=microsoftazure&logoColor=white)](https://azure.microsoft.com/en-us/products/purview)

> A governed financial lakehouse built on Azure for credit risk analytics, fraud monitoring, and regulatory reporting — processing multi-TB daily workloads with full audit traceability.

---

## Overview

This platform standardizes ingestion-to-curation workflows for financial datasets on **ADLS Gen2**, enabling AI-ready risk analytics pipelines and regulatory-grade reporting. Built to handle enterprise-scale transaction data with near-real-time fraud signal detection and complete dataset lineage for audit compliance.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     DATA SOURCES                            │
│   Retail Banking Feeds · Core Banking · Transaction APIs    │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│              INGESTION LAYER (Bronze)                       │
│        Azure Data Factory · Azure Event Hubs                │
│     Batch Pipelines + Spark Structured Streaming            │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│             CURATION LAYER (Silver)                         │
│         Databricks · Delta Lake · ADLS Gen2                 │
│      CDC Pipelines · Schema Enforcement · DQ Checks         │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│           ANALYTICS & AI LAYER (Gold)                       │
│    Synapse Analytics · Databricks Feature Engineering       │
│  Credit Risk Datasets · Fraud Signals · Regulatory Reports  │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│              GOVERNANCE LAYER                               │
│         Azure Purview · Lineage Tracking · Audit Trails     │
└─────────────────────────────────────────────────────────────┘
```

---

## Key Components

### 1. Batch Ingestion — Azure Data Factory
- Parameterized ADF pipelines ingesting retail banking transaction feeds
- Reusable ingestion templates across multiple upstream banking source systems
- Schema consistency enforcement across analytics and feature engineering datasets
- Accelerated onboarding of new source systems by **40%**

### 2. Real-Time Streaming — Event Hubs + Spark Structured Streaming
- Near-real-time transaction signal capture via **Azure Event Hubs**
- **Spark Structured Streaming** pipelines supporting fraud monitoring and anomaly detection
- Micro-batch processing for low-latency risk signal propagation

### 3. Lakehouse Storage — Delta Lake on ADLS Gen2
- Medallion architecture (Bronze → Silver → Gold zones)
- ACID transactions and time-travel on all financial datasets
- Optimized compaction and Z-ordering for analytics query performance
- Reconciliation effort reduced by **32%**

### 4. Analytics & Feature Engineering — Databricks
- Credit risk scoring feature datasets for ML model training
- Customer retention analytics experimentation datasets
- Synapse SQL workload optimization with partition tuning — reporting refresh **28% faster**

### 5. Governance — Azure Purview
- End-to-end dataset lineage tracking across all pipeline layers
- Audit-ready regulatory submission support
- Trusted ML dataset traceability for risk and compliance teams

---

## Results

| Metric | Before | After |
|--------|--------|-------|
| Reconciliation Effort | Baseline | **32% reduction** |
| ADF Onboarding Speed | Baseline | **40% faster** |
| Synapse Reporting Refresh | Baseline | **28% faster** |
| Data Traceability | Manual | Full Purview lineage |
| Fraud Signal Latency | Batch (hours) | Near real-time |

---

## Tech Stack

| Layer | Technologies |
|-------|-------------|
| Ingestion | Azure Data Factory, Azure Event Hubs |
| Processing | Apache Spark, PySpark, Databricks, Spark Structured Streaming |
| Storage | ADLS Gen2, Delta Lake |
| Warehousing | Azure Synapse Analytics, Azure SQL |
| Governance | Azure Purview |
| Orchestration | Azure Data Factory, Apache Airflow |
| IaC & CI/CD | Terraform, Azure DevOps |
| BI | Power BI |

---

## Repository Structure

```
enterprise-risk-analytics-lakehouse/
├── ingestion/
│   ├── adf_templates/          # Reusable ADF pipeline templates
│   ├── event_hubs/             # Streaming ingestion configs
│   └── source_connectors/      # Banking source system connectors
├── transformation/
│   ├── bronze_to_silver/       # Curation & DQ logic
│   ├── silver_to_gold/         # Feature engineering pipelines
│   └── delta_optimizations/    # Compaction, Z-order scripts
├── analytics/
│   ├── credit_risk/            # Risk scoring datasets
│   ├── fraud_monitoring/       # Anomaly detection pipelines
│   └── regulatory_reporting/   # Audit-ready report layers
├── governance/
│   └── purview_lineage/        # Lineage configs and policies
├── infrastructure/
│   └── terraform/              # Infrastructure as code
└── docs/
    └── architecture.md         # Detailed architecture docs
```

---

## Author

**Samhitha Alapati** — Senior Data Engineer

[![Portfolio](https://img.shields.io/badge/Portfolio-000000?style=flat-square)](https://applywizz-samhitha-26024.vercel.app/)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-0A66C2?style=flat-square&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/samhitha-alapati-data-engineer)
[![Email](https://img.shields.io/badge/Email-EA4335?style=flat-square&logo=gmail&logoColor=white)](mailto:samhitha3107@gmail.com)

