-- =============================================================
-- 02_gold_views.sql
-- Business-facing views on top of Gold external tables
-- =============================================================

-- =============================================================
-- View: Hospital Monthly Dashboard
-- =============================================================
CREATE OR ALTER VIEW gold.vw_hospital_monthly_dashboard AS
SELECT
    h.hospital_id,
    h.admission_year,
    h.admission_month,
    DATEFROMPARTS(h.admission_year, h.admission_month, 1) AS period_start,
    h.total_admissions,
    h.unique_patients,
    ROUND(h.avg_length_of_stay, 1)   AS avg_los_days,
    h.readmission_count,
    ROUND(h.readmission_rate_pct, 2) AS readmission_rate_pct,
    h.active_physicians,
    h.unique_diagnoses,

    -- Financial rollup for the same hospital + period
    ROUND(f.total_billed, 0)         AS total_revenue_inr,
    ROUND(f.total_covered, 0)        AS insurance_covered_inr,
    ROUND(f.total_copay, 0)          AS patient_copay_inr,
    f.claim_count,
    f.high_value_claims,
    ROUND(f.avg_coverage_pct, 1)     AS avg_coverage_pct,
    ROUND(f.avg_processing_days, 1)  AS avg_claim_processing_days
FROM
    gold.agg_hospital_metrics_ext h
LEFT JOIN (
    SELECT
        hospital_id,
        SUM(total_billed)        AS total_billed,
        SUM(total_covered)       AS total_covered,
        SUM(total_copay)         AS total_copay,
        SUM(claim_count)         AS claim_count,
        SUM(high_value_claims)   AS high_value_claims,
        AVG(avg_coverage_pct)    AS avg_coverage_pct,
        AVG(avg_processing_days) AS avg_processing_days
    FROM gold.agg_financial_summary_ext
    GROUP BY hospital_id
) f ON h.hospital_id = f.hospital_id;
GO

-- =============================================================
-- View: Patient Risk Summary
-- =============================================================
CREATE OR ALTER VIEW gold.vw_patient_risk_summary AS
SELECT
    p.patient_id,
    p.age,
    p.age_group,
    p.gender,
    p.hospital_id,
    p.ward,
    p.insurance_provider,
    p.diagnosis_code,
    p.diagnosis_description,
    p.risk_score,
    p.risk_category,
    p.readmission_flag,

    -- Latest admission details
    a.admission_date,
    a.discharge_date,
    a.length_of_stay,
    a.attending_physician,
    a.total_amount,
    a.claim_status
FROM
    gold.dim_patient_ext p
LEFT JOIN (
    SELECT
        patient_id,
        admission_date,
        discharge_date,
        length_of_stay,
        attending_physician,
        total_amount,
        claim_status,
        ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY admission_date DESC) AS rn
    FROM gold.fact_admissions_ext
) a ON p.patient_id = a.patient_id AND a.rn = 1;
GO

-- =============================================================
-- View: Top Diagnoses by Volume and Cost
-- =============================================================
CREATE OR ALTER VIEW gold.vw_diagnosis_analytics AS
SELECT
    f.diagnosis_code,
    p.diagnosis_description,
    f.ward,
    COUNT(DISTINCT f.patient_id)    AS patient_count,
    ROUND(AVG(CAST(f.length_of_stay AS FLOAT)), 1) AS avg_los,
    ROUND(AVG(CAST(f.age AS FLOAT)), 1)             AS avg_patient_age,
    SUM(CASE WHEN f.readmission_flag = 1 THEN 1 ELSE 0 END) AS readmissions,
    ROUND(
        CAST(SUM(CASE WHEN f.readmission_flag = 1 THEN 1 ELSE 0 END) AS FLOAT) /
        NULLIF(COUNT(f.patient_id), 0) * 100, 1
    ) AS readmission_rate_pct,
    ROUND(SUM(ISNULL(f.total_amount, 0)), 0)        AS total_revenue,
    ROUND(AVG(ISNULL(f.total_amount, 0)), 0)        AS avg_claim_amount
FROM
    gold.fact_admissions_ext f
LEFT JOIN (
    SELECT DISTINCT diagnosis_code, diagnosis_description
    FROM gold.dim_patient_ext
) p ON f.diagnosis_code = p.diagnosis_code
GROUP BY
    f.diagnosis_code,
    p.diagnosis_description,
    f.ward;
GO

-- =============================================================
-- View: Insurance Provider Analysis
-- =============================================================
CREATE OR ALTER VIEW gold.vw_insurance_analysis AS
SELECT
    insurance_provider,
    claim_type,
    SUM(claim_count)                        AS total_claims,
    SUM(CASE WHEN claim_status = 'APPROVED' THEN claim_count ELSE 0 END) AS approved_claims,
    SUM(CASE WHEN claim_status = 'DENIED'   THEN claim_count ELSE 0 END) AS denied_claims,
    SUM(CASE WHEN claim_status = 'PENDING'  THEN claim_count ELSE 0 END) AS pending_claims,
    ROUND(
        CAST(SUM(CASE WHEN claim_status = 'APPROVED' THEN claim_count ELSE 0 END) AS FLOAT) /
        NULLIF(SUM(claim_count), 0) * 100, 1
    )                                       AS approval_rate_pct,
    ROUND(SUM(total_billed), 0)             AS total_billed,
    ROUND(SUM(total_covered), 0)            AS total_covered,
    ROUND(SUM(total_copay), 0)              AS total_copay,
    ROUND(AVG(avg_processing_days), 1)      AS avg_processing_days,
    SUM(high_value_claims)                  AS high_value_claims
FROM
    gold.agg_financial_summary_ext
GROUP BY
    insurance_provider,
    claim_type;
GO

PRINT 'Gold views created successfully.';
