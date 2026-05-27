# Data Dictionary — Healthcare Azure Data Platform

## Overview

This document describes all tables, fields, and business rules across the Medallion layers.

---

## Bronze Layer (Raw)

### bronze/patients/
| Column | Type | Description |
|---|---|---|
| patient_id | string | Unique patient identifier (e.g. P-001001) |
| first_name | string | Patient first name (PII - masked in Silver) |
| last_name | string | Patient last name (PII - masked in Silver) |
| date_of_birth | string | Date of birth in yyyy-MM-dd format |
| gender | string | Raw gender value (M/F/Male/Female etc.) |
| diagnosis_code | string | ICD-10 diagnosis code |
| diagnosis_description | string | Human-readable diagnosis description |
| admission_date | string | Date patient was admitted |
| discharge_date | string | Date patient was discharged |
| attending_physician | string | Name of treating physician |
| hospital_id | string | Hospital identifier |
| insurance_provider | string | Insurance company name |
| insurance_id | string | Patient's insurance policy number |
| ward | string | Hospital ward/department |
| readmission_flag | integer | 1 = readmitted within 30 days, 0 = not |
| last_modified_date | string | Record last modified timestamp |

### bronze/claims/
| Column | Type | Description |
|---|---|---|
| claim_id | string | Unique claim identifier (e.g. CLM-2024-001001) |
| patient_id | string | FK → patients.patient_id |
| hospital_id | string | Hospital where service was rendered |
| diagnosis_code | string | ICD-10 code for the claim |
| procedure_code | string | CPT procedure code |
| procedure_description | string | Procedure description |
| total_amount | double | Total billed amount in INR |
| insurance_covered | double | Amount covered by insurance |
| patient_copay | double | Patient out-of-pocket amount |
| claim_status | string | APPROVED / DENIED / PENDING |
| submitted_date | string | Date claim was submitted |
| processed_date | string | Date claim was processed (null if pending) |
| insurance_provider | string | Insurance company |
| claim_type | string | INPATIENT / OUTPATIENT |

---

## Silver Layer (Cleaned)

### silver/patients/ (Delta Lake)
Inherits Bronze columns plus:

| Column | Type | Description |
|---|---|---|
| age | integer | Calculated age in years |
| length_of_stay | integer | Discharge date - Admission date (days) |
| age_group | string | Pediatric / Young Adult / Middle Aged / Senior / Elderly |
| first_name | string | Masked as `***` (HIPAA compliance) |
| last_name | string | Masked as `***` (HIPAA compliance) |
| _source_layer | string | Always `bronze` |
| _target_layer | string | Always `silver` |
| _processed_at | timestamp | When this record was processed |
| _run_date | string | Pipeline run date |
| _is_current | boolean | SCD Type 1 flag |

**Business Rules:**
- `gender` standardized to: `Male`, `Female`, `Unknown`
- `diagnosis_code` uppercased and trimmed
- Records with null `patient_id` or `admission_date` are dropped
- Duplicates resolved by keeping the latest `last_modified_date`

### silver/claims/ (Delta Lake)
Inherits Bronze columns plus:

| Column | Type | Description |
|---|---|---|
| coverage_ratio | double | insurance_covered / total_amount |
| processing_days | integer | processed_date - submitted_date |
| high_value_flag | boolean | total_amount > 500,000 INR |
| claim_status | string | Uppercased: APPROVED / DENIED / PENDING |

---

## Gold Layer (Business-Ready)

### gold/dim_patient/ (Delta Lake)
| Column | Type | Description |
|---|---|---|
| patient_id | string | PK |
| gender | string | Standardized gender |
| date_of_birth | date | Patient DOB |
| age | integer | Current age |
| age_group | string | Age bucket |
| hospital_id | string | Primary hospital |
| ward | string | Ward/department |
| insurance_provider | string | Insurance company |
| diagnosis_code | string | Primary ICD-10 code |
| diagnosis_description | string | Diagnosis description |
| risk_score | double | Computed 0-100 risk score |
| risk_category | string | Low Risk / Medium Risk / High Risk |
| readmission_flag | boolean | Whether patient has been readmitted |

**Risk Score Formula:**
```
risk_score = min(100,
  age × 0.3
  + length_of_stay × 2.5
  + (readmission_flag ? 20 : 0)
  + (age_group == 'Elderly' ? 10 : 0)
  + (age_group == 'Senior' ? 5 : 0)
)
```

### gold/fact_admissions/ (Delta Lake, partitioned by year/month)
One row per admission. Joined with claims data.

### gold/agg_hospital_metrics/ (Delta Lake)
Monthly KPI aggregates per hospital.

| Column | Description |
|---|---|
| total_admissions | Count of admissions in the month |
| avg_length_of_stay | Average LOS in days |
| readmission_rate_pct | % of patients readmitted within 30 days |
| active_physicians | Count of distinct attending physicians |

### gold/agg_financial_summary/ (Delta Lake)
Claims financial rollup by hospital, insurer, and claim type.

### gold/ml_readmission_predictions/ (Delta Lake)
ML model outputs.

| Column | Description |
|---|---|
| readmission_probability | Probability score 0.0–1.0 |
| readmission_predicted | Boolean prediction |
| risk_band | Very High / High / Medium / Low |

---

## ICD-10 Codes Used in Sample Data

| Code | Description | Ward |
|---|---|---|
| E11.9 | Type 2 diabetes without complications | Endocrinology |
| I25.10 | Atherosclerotic heart disease | Cardiology |
| J18.9 | Pneumonia unspecified | Pulmonology |
| N18.3 | Chronic kidney disease stage 3 | Nephrology |
| K35.80 | Acute appendicitis | Surgery |
| G43.909 | Migraine unspecified | Neurology |
| M16.11 | Primary osteoarthritis right hip | Orthopedics |
| C34.10 | Malignant neoplasm of lung | Oncology |
| I50.9 | Heart failure unspecified | Cardiology |
| G30.9 | Alzheimer disease | Neurology |

---

## Glossary

| Term | Definition |
|---|---|
| LOS | Length of Stay — number of days between admission and discharge |
| ICD-10 | International Classification of Diseases, 10th revision — standard diagnosis codes |
| CPT | Current Procedural Terminology — procedure billing codes |
| FHIR | Fast Healthcare Interoperability Resources — healthcare data standard |
| HL7 | Health Level 7 — healthcare messaging standard |
| HIPAA | Health Insurance Portability and Accountability Act — US data privacy law |
| Medallion | Bronze/Silver/Gold lakehouse architecture pattern |
| Delta Lake | Open-source ACID table storage layer for Spark |
