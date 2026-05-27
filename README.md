#  Azure Healthcare Data Platform

An end-to-end Azure Data Engineering project that ingests, transforms, and serves healthcare data using a **Medallion Architecture (Bronze → Silver → Gold)** on Azure.

![Architecture](architecture/architecture_diagram.png)

---

##  Project Overview

This project demonstrates a production-grade healthcare data pipeline built on Microsoft Azure. It ingests data from multiple sources (EHR systems, IoT devices, claims databases, public health APIs), processes it through a lakehouse medallion architecture, and serves insights via Power BI dashboards and machine learning models.

### Key Features
-  Batch & real-time data ingestion
-  Medallion architecture (Bronze / Silver / Gold)
-  Delta Lake on ADLS Gen2
-  PySpark transformations on Azure Databricks
-  Azure Synapse Analytics SQL Pools
-  Patient readmission ML model (Azure ML)
-  Power BI dashboards for clinical insights
-  HIPAA-compliant security (AAD, Key Vault, RBAC)
-  Infrastructure as Code (Bicep)

---

##  Architecture

```
Data Sources → ADF / Event Hubs → ADLS Gen2 (Bronze → Silver → Gold)
                                         ↓
                               Azure Databricks (PySpark)
                                         ↓
                               Azure Synapse Analytics
                                         ↓
                     Power BI | Azure ML | Azure SQL DB
```

---

##  Project Structure

```
azure-healthcare-data-platform/
│
├── README.md
├── architecture/
│   └── architecture_diagram.png
│
├── data_ingestion/
│   ├── adf_pipelines/
│   │   ├── pipeline_ehr_ingestion.json        # EHR batch ingestion pipeline
│   │   ├── pipeline_claims_ingestion.json     # Claims data pipeline
│   │   └── pipeline_public_api_ingestion.json # CDC/WHO API pipeline
│   ├── event_hubs/
│   │   └── iot_stream_config.json             # IoT streaming configuration
│   └── sample_data/
│       ├── patients.csv
│       └── claims.csv
│
├── databricks/
│   ├── 01_bronze_to_silver.py    # Data cleaning & validation
│   ├── 02_silver_to_gold.py      # Aggregation & business logic
│   └── 03_ml_readmission.py      # ML: 30-day readmission model
│
├── synapse/
│   ├── 01_create_external_tables.sql
│   ├── 02_gold_views.sql
│   └── 03_patient_analytics.sql
│
├── infrastructure/
│   ├── main.bicep                 # Main IaC entry point
│   ├── modules/
│   │   ├── storage.bicep
│   │   ├── databricks.bicep
│   │   └── synapse.bicep
│   └── parameters.json
│
├── docs/
│   └── data_dictionary.md
│
└── tests/
    └── test_transformations.py
```

---

##  Getting Started

### Prerequisites
- Azure Subscription
- Azure CLI installed
- Python 3.9+
- Databricks CLI

### 1. Clone the repository
```bash
git clone https://github.com/yourusername/azure-healthcare-data-platform.git
cd azure-healthcare-data-platform
```

### 2. Deploy Infrastructure
```bash
az login
az group create --name rg-healthcare-platform --location eastus
az deployment group create \
  --resource-group rg-healthcare-platform \
  --template-file infrastructure/main.bicep \
  --parameters infrastructure/parameters.json
```

### 3. Configure Azure Data Factory
- Import pipeline JSON files from `data_ingestion/adf_pipelines/` into your ADF instance
- Update linked service connection strings

### 4. Upload Databricks Notebooks
- Import `.py` files from `databricks/` into your Databricks workspace
- Configure cluster with Delta Lake runtime (13.x LTS)

### 5. Run Synapse SQL Scripts
- Execute scripts in `synapse/` in order (01 → 02 → 03)

---

##  Datasets Used

| Dataset | Source | Format |
|---|---|---|
| Synthetic Patients | [Synthea](https://synthea.mitre.org) | FHIR / CSV |
| Medicare Claims | [CMS Open Data](https://data.cms.gov) | CSV |
| ICU Vitals | [PhysioNet](https://physionet.org) | CSV |
| Disease Stats | [CDC API](https://data.cdc.gov) | JSON |

---

##  Security & Compliance

- Azure Active Directory (AAD) for authentication
- Azure Key Vault for secrets management
- Private Endpoints for network isolation
- Role-Based Access Control (RBAC)
- Designed for HIPAA compliance

---

##  Contributing

Pull requests are welcome. For major changes, please open an issue first.

---

##  License

MIT License
