# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze → Silver: Healthcare Data Cleaning & Validation
# MAGIC 
# MAGIC This notebook reads raw data from the Bronze layer (ADLS Gen2),
# MAGIC applies data quality checks, cleans and standardizes the data,
# MAGIC and writes the result to the Silver layer using Delta Lake format.
# MAGIC
# MAGIC **Author:** Healthcare Data Engineering Team  
# MAGIC **Layer:** Bronze → Silver  
# MAGIC **Schedule:** Triggered by ADF after ingestion completes

# COMMAND ----------

# MAGIC %md ## 1. Setup & Configuration

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType, TimestampType, BooleanType
)
from delta.tables import DeltaTable
from datetime import datetime
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bronze_to_silver")

# Retrieve parameters (passed from ADF or run manually)
dbutils.widgets.text("run_date", datetime.now().strftime("%Y-%m-%d"), "Run Date")
dbutils.widgets.text("data_source", "all", "Data Source (patients/claims/iot/all)")
dbutils.widgets.text("env", "dev", "Environment (dev/staging/prod)")

RUN_DATE     = dbutils.widgets.get("run_date")
DATA_SOURCE  = dbutils.widgets.get("data_source")
ENV          = dbutils.widgets.get("env")

# Storage paths
STORAGE_ACCOUNT = "adlshealthcaredev"
BRONZE_BASE     = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
SILVER_BASE     = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"

print(f"Run Date: {RUN_DATE}")
print(f"Data Source: {DATA_SOURCE}")
print(f"Environment: {ENV}")

# COMMAND ----------

# MAGIC %md ## 2. Schema Definitions

# COMMAND ----------

patient_schema = StructType([
    StructField("patient_id",           StringType(),    False),
    StructField("first_name",           StringType(),    True),
    StructField("last_name",            StringType(),    True),
    StructField("date_of_birth",        StringType(),    True),
    StructField("gender",               StringType(),    True),
    StructField("diagnosis_code",       StringType(),    True),
    StructField("diagnosis_description",StringType(),    True),
    StructField("admission_date",       StringType(),    True),
    StructField("discharge_date",       StringType(),    True),
    StructField("attending_physician",  StringType(),    True),
    StructField("hospital_id",          StringType(),    True),
    StructField("insurance_provider",   StringType(),    True),
    StructField("insurance_id",         StringType(),    True),
    StructField("ward",                 StringType(),    True),
    StructField("readmission_flag",     IntegerType(),   True),
    StructField("last_modified_date",   StringType(),    True),
])

claims_schema = StructType([
    StructField("claim_id",             StringType(),    False),
    StructField("patient_id",           StringType(),    False),
    StructField("hospital_id",          StringType(),    True),
    StructField("admission_date",       StringType(),    True),
    StructField("discharge_date",       StringType(),    True),
    StructField("diagnosis_code",       StringType(),    True),
    StructField("procedure_code",       StringType(),    True),
    StructField("procedure_description",StringType(),    True),
    StructField("total_amount",         DoubleType(),    True),
    StructField("insurance_covered",    DoubleType(),    True),
    StructField("patient_copay",        DoubleType(),    True),
    StructField("claim_status",         StringType(),    True),
    StructField("submitted_date",       StringType(),    True),
    StructField("processed_date",       StringType(),    True),
    StructField("insurance_provider",   StringType(),    True),
    StructField("claim_type",           StringType(),    True),
])

# COMMAND ----------

# MAGIC %md ## 3. Data Quality Functions

# COMMAND ----------

def check_null_counts(df, critical_cols):
    """Log null counts for critical columns."""
    null_counts = {}
    for col in critical_cols:
        count = df.filter(F.col(col).isNull()).count()
        null_counts[col] = count
        if count > 0:
            logger.warning(f"NULL values found in {col}: {count} rows")
    return null_counts

def validate_date_format(df, date_cols):
    """Flag rows with invalid date formats."""
    for col in date_cols:
        df = df.withColumn(
            f"{col}_valid",
            F.col(col).rlike(r"^\d{4}-\d{2}-\d{2}$")
        )
    return df

def remove_duplicates(df, primary_key_cols, order_col="last_modified_date"):
    """Keep the most recent record for each primary key."""
    from pyspark.sql.window import Window
    window = Window.partitionBy(primary_key_cols).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("row_num", F.row_number().over(window))
          .filter(F.col("row_num") == 1)
          .drop("row_num")
    )

def add_audit_columns(df, source_layer="bronze", target_layer="silver"):
    """Add standard audit columns."""
    return df.withColumns({
        "_source_layer":     F.lit(source_layer),
        "_target_layer":     F.lit(target_layer),
        "_processed_at":     F.current_timestamp(),
        "_run_date":         F.lit(RUN_DATE),
        "_is_current":       F.lit(True),
    })

# COMMAND ----------

# MAGIC %md ## 4. Process Patients Data

# COMMAND ----------

def process_patients():
    logger.info("Starting patients Bronze → Silver transformation")

    # Read raw bronze data
    bronze_path = f"{BRONZE_BASE}/patients/{RUN_DATE.replace('-', '/')}/"
    df_raw = (
        spark.read
             .schema(patient_schema)
             .option("header", "true")
             .csv(bronze_path)
    )

    print(f"Bronze patients count: {df_raw.count()}")

    # ── Data Quality Checks ──────────────────────────────────
    critical_cols = ["patient_id", "date_of_birth", "admission_date"]
    null_counts = check_null_counts(df_raw, critical_cols)

    # Drop rows with null primary keys
    df_clean = df_raw.dropna(subset=["patient_id"])

    # ── Standardize & Transform ──────────────────────────────

    df_silver = (
        df_clean

        # Standardize gender
        .withColumn("gender", F.upper(F.trim(F.col("gender"))))
        .withColumn("gender",
            F.when(F.col("gender").isin("M", "MALE"),   "Male")
             .when(F.col("gender").isin("F", "FEMALE"), "Female")
             .otherwise("Unknown")
        )

        # Parse dates
        .withColumn("date_of_birth",      F.to_date("date_of_birth",      "yyyy-MM-dd"))
        .withColumn("admission_date",     F.to_date("admission_date",     "yyyy-MM-dd"))
        .withColumn("discharge_date",     F.to_date("discharge_date",     "yyyy-MM-dd"))
        .withColumn("last_modified_date", F.to_date("last_modified_date", "yyyy-MM-dd"))

        # Derived columns
        .withColumn("age",
            F.floor(F.datediff(F.current_date(), F.col("date_of_birth")) / 365.25)
        )
        .withColumn("length_of_stay",
            F.datediff(F.col("discharge_date"), F.col("admission_date"))
        )
        .withColumn("age_group",
            F.when(F.col("age") < 18,  "Pediatric")
             .when(F.col("age") < 40,  "Young Adult")
             .when(F.col("age") < 60,  "Middle Aged")
             .when(F.col("age") < 80,  "Senior")
             .otherwise("Elderly")
        )

        # Mask PII (HIPAA compliance)
        .withColumn("first_name", F.lit("***"))
        .withColumn("last_name",  F.lit("***"))

        # Standardize strings
        .withColumn("diagnosis_code",
            F.upper(F.trim(F.col("diagnosis_code")))
        )
        .withColumn("hospital_id",
            F.upper(F.trim(F.col("hospital_id")))
        )

        # Cast readmission flag
        .withColumn("readmission_flag",
            F.col("readmission_flag").cast(BooleanType())
        )

        # Drop nulls on critical dates
        .dropna(subset=["admission_date"])
    )

    # Remove duplicates
    df_silver = remove_duplicates(df_silver, ["patient_id"])

    # Add audit columns
    df_silver = add_audit_columns(df_silver)

    print(f"Silver patients count: {df_silver.count()}")

    # ── Write to Delta Lake ──────────────────────────────────
    silver_path = f"{SILVER_BASE}/patients"

    if DeltaTable.isDeltaTable(spark, silver_path):
        # Upsert (merge) into existing Delta table
        delta_table = DeltaTable.forPath(spark, silver_path)
        delta_table.alias("target").merge(
            df_silver.alias("source"),
            "target.patient_id = source.patient_id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
        logger.info("Merged patients into existing Silver Delta table")
    else:
        # First run: create Delta table
        df_silver.write \
                 .format("delta") \
                 .mode("overwrite") \
                 .partitionBy("admission_date") \
                 .option("overwriteSchema", "true") \
                 .save(silver_path)
        logger.info("Created new Silver Delta table for patients")

    # Optimize Delta table
    spark.sql(f"OPTIMIZE delta.`{silver_path}` ZORDER BY (patient_id, admission_date)")
    logger.info("Patients Silver transformation complete.")

# COMMAND ----------

# MAGIC %md ## 5. Process Claims Data

# COMMAND ----------

def process_claims():
    logger.info("Starting claims Bronze → Silver transformation")

    bronze_path = f"{BRONZE_BASE}/claims/{RUN_DATE.replace('-', '/')}/"
    df_raw = (
        spark.read
             .schema(claims_schema)
             .option("header", "true")
             .csv(bronze_path)
    )

    print(f"Bronze claims count: {df_raw.count()}")

    df_silver = (
        df_raw

        # Drop null primary keys
        .dropna(subset=["claim_id", "patient_id"])

        # Parse dates
        .withColumn("admission_date",   F.to_date("admission_date",   "yyyy-MM-dd"))
        .withColumn("discharge_date",   F.to_date("discharge_date",   "yyyy-MM-dd"))
        .withColumn("submitted_date",   F.to_date("submitted_date",   "yyyy-MM-dd"))
        .withColumn("processed_date",   F.to_date("processed_date",   "yyyy-MM-dd"))

        # Standardize claim status
        .withColumn("claim_status",
            F.upper(F.trim(F.col("claim_status")))
        )

        # Derived financial columns
        .withColumn("coverage_ratio",
            F.round(F.col("insurance_covered") / F.col("total_amount"), 4)
        )
        .withColumn("processing_days",
            F.datediff(F.col("processed_date"), F.col("submitted_date"))
        )

        # Flag high-value claims (> 500000 INR)
        .withColumn("high_value_flag",
            F.when(F.col("total_amount") > 500000, True).otherwise(False)
        )

        # Standardize strings
        .withColumn("diagnosis_code",
            F.upper(F.trim(F.col("diagnosis_code")))
        )
        .withColumn("claim_type",
            F.upper(F.trim(F.col("claim_type")))
        )
    )

    df_silver = remove_duplicates(df_silver, ["claim_id"])
    df_silver = add_audit_columns(df_silver)

    print(f"Silver claims count: {df_silver.count()}")

    silver_path = f"{SILVER_BASE}/claims"

    if DeltaTable.isDeltaTable(spark, silver_path):
        delta_table = DeltaTable.forPath(spark, silver_path)
        delta_table.alias("target").merge(
            df_silver.alias("source"),
            "target.claim_id = source.claim_id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
    else:
        df_silver.write \
                 .format("delta") \
                 .mode("overwrite") \
                 .partitionBy("claim_type", "claim_status") \
                 .option("overwriteSchema", "true") \
                 .save(silver_path)

    spark.sql(f"OPTIMIZE delta.`{silver_path}` ZORDER BY (claim_id, patient_id)")
    logger.info("Claims Silver transformation complete.")

# COMMAND ----------

# MAGIC %md ## 6. Run Transformations

# COMMAND ----------

if DATA_SOURCE in ("patients", "all"):
    process_patients()

if DATA_SOURCE in ("claims", "all"):
    process_claims()

print(f"\n✅ Bronze → Silver transformation complete for: {DATA_SOURCE} on {RUN_DATE}")
