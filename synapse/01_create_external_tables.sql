-- =============================================================
-- 01_create_external_tables.sql
-- Azure Synapse Analytics: External tables pointing to ADLS Gen2 Gold layer
-- =============================================================

-- Create external data source pointing to ADLS Gen2
IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'ADLS_Gold')
BEGIN
    CREATE EXTERNAL DATA SOURCE ADLS_Gold
    WITH (
        LOCATION = 'abfss://gold@adlshealthcaredev.dfs.core.windows.net',
        CREDENTIAL = [SynapseIdentity]
    );
END
GO

-- Create file format for Parquet/Delta
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'ParquetFormat')
BEGIN
    CREATE EXTERNAL FILE FORMAT ParquetFormat
    WITH (
        FORMAT_TYPE = PARQUET,
        DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
    );
END
GO

-- =============================================================
-- External Table: dim_patient
-- =============================================================
IF OBJECT_ID('gold.dim_patient_ext') IS NOT NULL
    DROP EXTERNAL TABLE gold.dim_patient_ext;
GO

CREATE EXTERNAL TABLE gold.dim_patient_ext (
    patient_id              NVARCHAR(20)     NOT NULL,
    gender                  NVARCHAR(10),
    date_of_birth           DATE,
    age                     INT,
    age_group               NVARCHAR(20),
    hospital_id             NVARCHAR(20),
    ward                    NVARCHAR(50),
    insurance_provider      NVARCHAR(100),
    diagnosis_code          NVARCHAR(20),
    diagnosis_description   NVARCHAR(500),
    risk_score              FLOAT,
    risk_category           NVARCHAR(20),
    readmission_flag        BIT,
    _processed_at           DATETIME2
)
WITH (
    LOCATION     = '/dim_patient/',
    DATA_SOURCE  = ADLS_Gold,
    FILE_FORMAT  = ParquetFormat
);
GO

-- =============================================================
-- External Table: fact_admissions
-- =============================================================
IF OBJECT_ID('gold.fact_admissions_ext') IS NOT NULL
    DROP EXTERNAL TABLE gold.fact_admissions_ext;
GO

CREATE EXTERNAL TABLE gold.fact_admissions_ext (
    patient_id              NVARCHAR(20),
    hospital_id             NVARCHAR(20),
    admission_date          DATE,
    discharge_date          DATE,
    length_of_stay          INT,
    diagnosis_code          NVARCHAR(20),
    ward                    NVARCHAR(50),
    attending_physician     NVARCHAR(200),
    age                     INT,
    age_group               NVARCHAR(20),
    readmission_flag        BIT,
    claim_id                NVARCHAR(30),
    total_amount            FLOAT,
    insurance_covered       FLOAT,
    patient_copay           FLOAT,
    claim_status            NVARCHAR(20),
    claim_type              NVARCHAR(20),
    coverage_ratio          FLOAT,
    admission_year          INT,
    admission_month         INT,
    admission_quarter       INT
)
WITH (
    LOCATION     = '/fact_admissions/',
    DATA_SOURCE  = ADLS_Gold,
    FILE_FORMAT  = ParquetFormat
);
GO

-- =============================================================
-- External Table: agg_hospital_metrics
-- =============================================================
IF OBJECT_ID('gold.agg_hospital_metrics_ext') IS NOT NULL
    DROP EXTERNAL TABLE gold.agg_hospital_metrics_ext;
GO

CREATE EXTERNAL TABLE gold.agg_hospital_metrics_ext (
    hospital_id             NVARCHAR(20),
    admission_year          INT,
    admission_month         INT,
    total_admissions        INT,
    unique_patients         INT,
    avg_length_of_stay      FLOAT,
    max_length_of_stay      INT,
    readmission_count       INT,
    readmission_rate_pct    FLOAT,
    unique_diagnoses        INT,
    active_physicians       INT,
    _processed_at           DATETIME2,
    _run_date               DATE
)
WITH (
    LOCATION     = '/agg_hospital_metrics/',
    DATA_SOURCE  = ADLS_Gold,
    FILE_FORMAT  = ParquetFormat
);
GO

-- =============================================================
-- External Table: agg_financial_summary
-- =============================================================
IF OBJECT_ID('gold.agg_financial_summary_ext') IS NOT NULL
    DROP EXTERNAL TABLE gold.agg_financial_summary_ext;
GO

CREATE EXTERNAL TABLE gold.agg_financial_summary_ext (
    hospital_id             NVARCHAR(20),
    insurance_provider      NVARCHAR(100),
    claim_type              NVARCHAR(20),
    claim_status            NVARCHAR(20),
    claim_count             INT,
    total_billed            FLOAT,
    total_covered           FLOAT,
    total_copay             FLOAT,
    avg_claim_amount        FLOAT,
    avg_coverage_pct        FLOAT,
    avg_processing_days     FLOAT,
    high_value_claims       INT,
    _processed_at           DATETIME2,
    _run_date               DATE
)
WITH (
    LOCATION     = '/agg_financial_summary/',
    DATA_SOURCE  = ADLS_Gold,
    FILE_FORMAT  = ParquetFormat
);
GO

PRINT 'External tables created successfully.';
