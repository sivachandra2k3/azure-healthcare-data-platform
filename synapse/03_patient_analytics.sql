-- =============================================================
-- 03_patient_analytics.sql
-- Ready-to-use analytical queries for Power BI and reporting
-- =============================================================

-- =============================================================
-- Query 1: Hospital Performance Overview (Power BI KPI cards)
-- =============================================================
SELECT
    hospital_id,
    SUM(total_admissions)                               AS ytd_admissions,
    ROUND(AVG(avg_los_days), 1)                         AS avg_los,
    ROUND(AVG(readmission_rate_pct), 2)                 AS avg_readmission_rate,
    ROUND(SUM(CAST(total_revenue_inr AS FLOAT)) / 1e6, 2) AS total_revenue_million_inr,
    ROUND(AVG(avg_coverage_pct), 1)                     AS avg_insurance_coverage_pct
FROM
    gold.vw_hospital_monthly_dashboard
WHERE
    admission_year = YEAR(GETDATE())
GROUP BY
    hospital_id
ORDER BY
    ytd_admissions DESC;
GO

-- =============================================================
-- Query 2: Monthly Admission Trend (Power BI Line Chart)
-- =============================================================
SELECT
    admission_year,
    admission_month,
    DATEFROMPARTS(admission_year, admission_month, 1) AS month_start,
    SUM(total_admissions)   AS total_admissions,
    SUM(readmission_count)  AS total_readmissions,
    ROUND(AVG(readmission_rate_pct), 2) AS readmission_rate_pct,
    ROUND(AVG(avg_los_days), 1)         AS avg_los_days
FROM
    gold.vw_hospital_monthly_dashboard
GROUP BY
    admission_year,
    admission_month
ORDER BY
    admission_year, admission_month;
GO

-- =============================================================
-- Query 3: Top 10 Diagnoses by Patient Volume
-- =============================================================
SELECT TOP 10
    diagnosis_code,
    diagnosis_description,
    patient_count,
    avg_los,
    avg_patient_age,
    readmission_rate_pct,
    ROUND(CAST(total_revenue AS FLOAT) / 1e6, 2) AS revenue_million_inr
FROM
    gold.vw_diagnosis_analytics
ORDER BY
    patient_count DESC;
GO

-- =============================================================
-- Query 4: High-Risk Patients Requiring Follow-up
-- =============================================================
SELECT
    patient_id,
    hospital_id,
    ward,
    age,
    age_group,
    gender,
    diagnosis_code,
    diagnosis_description,
    risk_score,
    risk_category,
    length_of_stay,
    attending_physician,
    admission_date,
    discharge_date
FROM
    gold.vw_patient_risk_summary
WHERE
    risk_category IN ('High Risk')
    AND discharge_date >= DATEADD(DAY, -30, GETDATE())
ORDER BY
    risk_score DESC;
GO

-- =============================================================
-- Query 5: Insurance Provider Performance Comparison
-- =============================================================
SELECT
    insurance_provider,
    SUM(total_claims)                   AS total_claims,
    SUM(approved_claims)                AS approved,
    SUM(denied_claims)                  AS denied,
    SUM(pending_claims)                 AS pending,
    ROUND(AVG(approval_rate_pct), 1)    AS avg_approval_rate_pct,
    ROUND(SUM(CAST(total_billed AS FLOAT)) / 1e6, 2)   AS total_billed_million,
    ROUND(SUM(CAST(total_covered AS FLOAT)) / 1e6, 2)  AS total_covered_million,
    ROUND(AVG(avg_processing_days), 1)  AS avg_processing_days,
    SUM(high_value_claims)              AS high_value_claims
FROM
    gold.vw_insurance_analysis
GROUP BY
    insurance_provider
ORDER BY
    total_claims DESC;
GO

-- =============================================================
-- Query 6: Ward Occupancy & Efficiency
-- =============================================================
SELECT
    f.ward,
    COUNT(DISTINCT f.patient_id)    AS total_patients,
    ROUND(AVG(CAST(f.length_of_stay AS FLOAT)), 1) AS avg_los,
    SUM(CASE WHEN f.readmission_flag = 1 THEN 1 ELSE 0 END) AS readmissions,
    ROUND(
        CAST(SUM(CASE WHEN f.readmission_flag = 1 THEN 1 ELSE 0 END) AS FLOAT) /
        NULLIF(COUNT(f.patient_id), 0) * 100, 1
    ) AS readmission_rate_pct,
    ROUND(AVG(ISNULL(f.total_amount, 0)), 0) AS avg_claim_per_patient,
    COUNT(DISTINCT f.attending_physician)    AS physician_count
FROM
    gold.fact_admissions_ext f
GROUP BY
    f.ward
ORDER BY
    total_patients DESC;
GO

-- =============================================================
-- Query 7: 30-Day Readmission Root Cause Analysis
-- =============================================================
WITH ReadmittedPatients AS (
    SELECT
        patient_id,
        admission_date,
        diagnosis_code,
        ward,
        length_of_stay,
        total_amount,
        ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY admission_date) AS visit_num
    FROM gold.fact_admissions_ext
    WHERE readmission_flag = 1
)
SELECT
    r1.diagnosis_code,
    r1.ward,
    COUNT(*)                                    AS readmission_count,
    ROUND(AVG(CAST(r1.length_of_stay AS FLOAT)), 1) AS avg_initial_los,
    ROUND(AVG(CAST(r1.total_amount AS FLOAT)), 0)   AS avg_initial_cost,
    ROUND(AVG(DATEDIFF(DAY, r1.admission_date, r2.admission_date)), 1) AS avg_days_to_readmission
FROM
    ReadmittedPatients r1
JOIN ReadmittedPatients r2
    ON r1.patient_id = r2.patient_id
    AND r2.visit_num = r1.visit_num + 1
    AND DATEDIFF(DAY, r1.admission_date, r2.admission_date) <= 30
GROUP BY
    r1.diagnosis_code,
    r1.ward
ORDER BY
    readmission_count DESC;
GO
