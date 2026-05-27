# Databricks notebook source
# MAGIC %md
# MAGIC # Silver → Gold: Healthcare Aggregations & Business Logic
# MAGIC
# MAGIC Reads cleaned Silver data and produces business-ready Gold tables:
# MAGIC - `gold.dim_patient` — Patient dimension
# MAGIC - `gold.fact_admissions` — Admission fact table
# MAGIC - `gold.agg_hospital_metrics` — Hospital KPI aggregates
# MAGIC - `gold.agg_disease_burden` — Disease burden by diagnosis
# MAGIC - `gold.agg_financial_summary` — Claims financial summary

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("silver_to_gold")

dbutils.widgets.text("run_date", datetime.now().strftime("%Y-%m-%d"), "Run Date")
RUN_DATE = dbutils.widgets.get("run_date")

STORAGE_ACCOUNT = "adlshealthcaredev"
SILVER_BASE     = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
GOLD_BASE       = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# COMMAND ----------

# MAGIC %md ## 1. Load Silver Tables

# COMMAND ----------

df_patients = spark.read.format("delta").load(f"{SILVER_BASE}/patients")
df_claims   = spark.read.format("delta").load(f"{SILVER_BASE}/claims")

print(f"Silver patients: {df_patients.count()} rows")
print(f"Silver claims:   {df_claims.count()} rows")

# COMMAND ----------

# MAGIC %md ## 2. Gold: Patient Dimension Table

# COMMAND ----------

def build_dim_patient():
    """
    Dimension table: one row per patient with latest demographics
    and computed risk score.
    """
    # Readmission rate per patient
    readmission_window = Window.partitionBy("patient_id")

    df_dim = (
        df_patients

        # Take latest record per patient
        .withColumn("rn", F.row_number().over(
            Window.partitionBy("patient_id")
                  .orderBy(F.col("admission_date").desc())
        ))
        .filter(F.col("rn") == 1)
        .drop("rn")

        # Risk score: combines age, LOS, readmission, diagnosis severity
        .withColumn("risk_score",
            F.least(F.lit(100),
                F.round(
                    F.col("age") * 0.3
                    + F.col("length_of_stay") * 2.5
                    + F.when(F.col("readmission_flag"), 20).otherwise(0)
                    + F.when(F.col("age_group") == "Elderly",  10).otherwise(0)
                    + F.when(F.col("age_group") == "Senior",    5).otherwise(0),
                1)
            )
        )
        .withColumn("risk_category",
            F.when(F.col("risk_score") >= 70, "High Risk")
             .when(F.col("risk_score") >= 40, "Medium Risk")
             .otherwise("Low Risk")
        )

        # Select final columns
        .select(
            "patient_id", "gender", "date_of_birth", "age",
            "age_group", "hospital_id", "ward",
            "insurance_provider", "diagnosis_code", "diagnosis_description",
            "risk_score", "risk_category", "readmission_flag",
            "_processed_at"
        )
    )

    gold_path = f"{GOLD_BASE}/dim_patient"
    df_dim.write.format("delta").mode("overwrite") \
          .option("overwriteSchema", "true") \
          .save(gold_path)

    spark.sql(f"OPTIMIZE delta.`{gold_path}` ZORDER BY (patient_id)")
    logger.info(f"dim_patient written: {df_dim.count()} rows")

build_dim_patient()

# COMMAND ----------

# MAGIC %md ## 3. Gold: Fact Admissions

# COMMAND ----------

def build_fact_admissions():
    """
    Fact table: one row per admission, enriched with claims data.
    """
    df_fact = (
        df_patients.alias("p")
        .join(
            df_claims.alias("c"),
            on=["patient_id"],
            how="left"
        )
        .select(
            F.col("p.patient_id"),
            F.col("p.hospital_id"),
            F.col("p.admission_date"),
            F.col("p.discharge_date"),
            F.col("p.length_of_stay"),
            F.col("p.diagnosis_code"),
            F.col("p.ward"),
            F.col("p.attending_physician"),
            F.col("p.age"),
            F.col("p.age_group"),
            F.col("p.readmission_flag"),
            F.col("c.claim_id"),
            F.col("c.total_amount"),
            F.col("c.insurance_covered"),
            F.col("c.patient_copay"),
            F.col("c.claim_status"),
            F.col("c.claim_type"),
            F.col("c.coverage_ratio"),
        )
        .withColumn("admission_year",  F.year("admission_date"))
        .withColumn("admission_month", F.month("admission_date"))
        .withColumn("admission_quarter",
            F.quarter(F.col("admission_date"))
        )
    )

    gold_path = f"{GOLD_BASE}/fact_admissions"
    df_fact.write.format("delta").mode("overwrite") \
           .partitionBy("admission_year", "admission_month") \
           .option("overwriteSchema", "true") \
           .save(gold_path)

    spark.sql(f"OPTIMIZE delta.`{gold_path}` ZORDER BY (patient_id, admission_date)")
    logger.info(f"fact_admissions written: {df_fact.count()} rows")

build_fact_admissions()

# COMMAND ----------

# MAGIC %md ## 4. Gold: Hospital Performance Metrics

# COMMAND ----------

def build_agg_hospital_metrics():
    """
    Aggregated KPIs per hospital per month.
    """
    df_agg = (
        df_patients
        .withColumn("admission_year",  F.year("admission_date"))
        .withColumn("admission_month", F.month("admission_date"))
        .groupBy("hospital_id", "admission_year", "admission_month")
        .agg(
            F.count("patient_id").alias("total_admissions"),
            F.countDistinct("patient_id").alias("unique_patients"),
            F.round(F.avg("length_of_stay"), 2).alias("avg_length_of_stay"),
            F.max("length_of_stay").alias("max_length_of_stay"),
            F.sum(F.when(F.col("readmission_flag"), 1).otherwise(0))
             .alias("readmission_count"),
            F.round(
                F.sum(F.when(F.col("readmission_flag"), 1).otherwise(0)) /
                F.count("patient_id") * 100, 2
            ).alias("readmission_rate_pct"),
            F.countDistinct("diagnosis_code").alias("unique_diagnoses"),
            F.countDistinct("attending_physician").alias("active_physicians"),
        )
        .withColumn("_processed_at", F.current_timestamp())
        .withColumn("_run_date",     F.lit(RUN_DATE))
    )

    gold_path = f"{GOLD_BASE}/agg_hospital_metrics"
    df_agg.write.format("delta").mode("overwrite") \
          .option("overwriteSchema", "true") \
          .save(gold_path)

    logger.info(f"agg_hospital_metrics written: {df_agg.count()} rows")
    df_agg.show(5, truncate=False)

build_agg_hospital_metrics()

# COMMAND ----------

# MAGIC %md ## 5. Gold: Disease Burden Aggregation

# COMMAND ----------

def build_agg_disease_burden():
    """
    Disease burden: patient counts, avg LOS, readmission rate by ICD code.
    """
    df_agg = (
        df_patients
        .groupBy("diagnosis_code", "diagnosis_description", "ward")
        .agg(
            F.count("patient_id").alias("patient_count"),
            F.round(F.avg("length_of_stay"), 2).alias("avg_length_of_stay"),
            F.round(F.avg("age"), 1).alias("avg_patient_age"),
            F.sum(F.when(F.col("readmission_flag"), 1).otherwise(0))
             .alias("readmission_count"),
            F.round(
                F.sum(F.when(F.col("readmission_flag"), 1).otherwise(0)) /
                F.count("patient_id") * 100, 2
            ).alias("readmission_rate_pct"),
        )
        .withColumn("disease_severity",
            F.when(F.col("avg_length_of_stay") > 14, "Severe")
             .when(F.col("avg_length_of_stay") > 7,  "Moderate")
             .otherwise("Mild")
        )
        .withColumn("_processed_at", F.current_timestamp())
        .orderBy(F.col("patient_count").desc())
    )

    gold_path = f"{GOLD_BASE}/agg_disease_burden"
    df_agg.write.format("delta").mode("overwrite") \
          .option("overwriteSchema", "true") \
          .save(gold_path)

    logger.info(f"agg_disease_burden written: {df_agg.count()} rows")

build_agg_disease_burden()

# COMMAND ----------

# MAGIC %md ## 6. Gold: Financial Summary

# COMMAND ----------

def build_agg_financial_summary():
    """
    Claims financial summary per hospital and insurance provider.
    """
    df_agg = (
        df_claims
        .groupBy("hospital_id", "insurance_provider", "claim_type", "claim_status")
        .agg(
            F.count("claim_id").alias("claim_count"),
            F.round(F.sum("total_amount"), 2).alias("total_billed"),
            F.round(F.sum("insurance_covered"), 2).alias("total_covered"),
            F.round(F.sum("patient_copay"), 2).alias("total_copay"),
            F.round(F.avg("total_amount"), 2).alias("avg_claim_amount"),
            F.round(F.avg("coverage_ratio") * 100, 2).alias("avg_coverage_pct"),
            F.round(F.avg("processing_days"), 1).alias("avg_processing_days"),
            F.sum(F.when(F.col("high_value_flag"), 1).otherwise(0))
             .alias("high_value_claims"),
        )
        .withColumn("_processed_at", F.current_timestamp())
        .withColumn("_run_date",     F.lit(RUN_DATE))
    )

    gold_path = f"{GOLD_BASE}/agg_financial_summary"
    df_agg.write.format("delta").mode("overwrite") \
          .option("overwriteSchema", "true") \
          .save(gold_path)

    logger.info(f"agg_financial_summary written: {df_agg.count()} rows")
    df_agg.show(5, truncate=False)

build_agg_financial_summary()

# COMMAND ----------

print(f"\n✅ Silver → Gold transformation complete for run date: {RUN_DATE}")
print("Gold tables written:")
print("  - dim_patient")
print("  - fact_admissions")
print("  - agg_hospital_metrics")
print("  - agg_disease_burden")
print("  - agg_financial_summary")
