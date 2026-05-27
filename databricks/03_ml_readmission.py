# Databricks notebook source
# MAGIC %md
# MAGIC # ML Model: 30-Day Patient Readmission Prediction
# MAGIC
# MAGIC Trains a classification model to predict whether a patient will be
# MAGIC readmitted within 30 days of discharge, using Gold layer data.
# MAGIC
# MAGIC **Model:** Random Forest Classifier (with MLflow tracking)  
# MAGIC **Target:** `readmission_within_30_days` (binary)  
# MAGIC **Output:** Predictions written to Gold layer + model registered in Azure ML

# COMMAND ----------

import mlflow
import mlflow.spark
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    VectorAssembler, StringIndexer, OneHotEncoder, StandardScaler
)
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator, MulticlassClassificationEvaluator
)
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ml_readmission")

STORAGE_ACCOUNT = "adlshealthcaredev"
GOLD_BASE       = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net"
MODEL_PATH      = f"abfss://models@{STORAGE_ACCOUNT}.dfs.core.windows.net/readmission"

mlflow.set_experiment("/Healthcare/ReadmissionPrediction")

# COMMAND ----------

# MAGIC %md ## 1. Load & Prepare Features

# COMMAND ----------

df_patients  = spark.read.format("delta").load(f"{GOLD_BASE}/dim_patient")
df_facts     = spark.read.format("delta").load(f"{GOLD_BASE}/fact_admissions")
df_financial = spark.read.format("delta").load(f"{GOLD_BASE}/agg_financial_summary")

# Join to create ML feature set
df_ml = (
    df_facts.alias("f")
    .join(df_patients.alias("p"), on="patient_id", how="left")
    .select(
        "f.patient_id",
        "f.hospital_id",
        "f.admission_date",
        "f.length_of_stay",
        "f.diagnosis_code",
        "f.ward",
        "f.age",
        "f.age_group",
        "f.total_amount",
        "f.coverage_ratio",
        "f.claim_type",
        "f.readmission_flag",
        "p.risk_score",
        "p.gender",
        "p.insurance_provider",
    )
    .withColumn("label", F.col("readmission_flag").cast("int"))

    # Feature engineering
    .withColumn("admission_month", F.month("admission_date"))
    .withColumn("admission_dayofweek", F.dayofweek("admission_date"))
    .withColumn("is_weekend_admission",
        F.when(F.col("admission_dayofweek").isin(1, 7), 1).otherwise(0)
    )
    .withColumn("los_bucket",
        F.when(F.col("length_of_stay") <= 3,  "Short")
         .when(F.col("length_of_stay") <= 7,  "Medium")
         .when(F.col("length_of_stay") <= 14, "Long")
         .otherwise("Very Long")
    )
    .withColumn("total_amount",   F.coalesce(F.col("total_amount"),   F.lit(0.0)))
    .withColumn("coverage_ratio", F.coalesce(F.col("coverage_ratio"), F.lit(0.0)))
    .withColumn("risk_score",     F.coalesce(F.col("risk_score"),     F.lit(0.0)))
    .dropna(subset=["label", "age", "length_of_stay"])
)

print(f"ML dataset: {df_ml.count()} rows")
df_ml.groupBy("label").count().show()

# COMMAND ----------

# MAGIC %md ## 2. Feature Engineering Pipeline

# COMMAND ----------

# Categorical columns → indexed → one-hot encoded
cat_cols = ["gender", "age_group", "ward", "los_bucket", "claim_type"]
idx_cols = [f"{c}_idx" for c in cat_cols]
ohe_cols = [f"{c}_ohe" for c in cat_cols]

indexers = [
    StringIndexer(inputCol=c, outputCol=i, handleInvalid="keep")
    for c, i in zip(cat_cols, idx_cols)
]

encoder = OneHotEncoder(inputCols=idx_cols, outputCols=ohe_cols)

# Numeric features
num_cols = [
    "age", "length_of_stay", "total_amount", "coverage_ratio",
    "risk_score", "admission_month", "is_weekend_admission"
]

assembler = VectorAssembler(
    inputCols=ohe_cols + num_cols,
    outputCol="features_raw",
    handleInvalid="keep"
)

scaler = StandardScaler(
    inputCol="features_raw",
    outputCol="features",
    withMean=True,
    withStd=True
)

# COMMAND ----------

# MAGIC %md ## 3. Train Model with MLflow Tracking

# COMMAND ----------

train_df, test_df = df_ml.randomSplit([0.8, 0.2], seed=42)
print(f"Train: {train_df.count()} | Test: {test_df.count()}")

with mlflow.start_run(run_name=f"readmission_rf_{mlflow.active_run().info.run_id[:8]}"):

    mlflow.log_param("model_type",      "RandomForestClassifier")
    mlflow.log_param("train_size",       train_df.count())
    mlflow.log_param("test_size",        test_df.count())

    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol="label",
        numTrees=100,
        maxDepth=8,
        minInstancesPerNode=5,
        seed=42
    )

    pipeline = Pipeline(stages=indexers + [encoder, assembler, scaler, rf])

    # Hyperparameter grid
    param_grid = (
        ParamGridBuilder()
        .addGrid(rf.numTrees,  [50, 100])
        .addGrid(rf.maxDepth,  [6, 8])
        .build()
    )

    evaluator = BinaryClassificationEvaluator(
        labelCol="label",
        metricName="areaUnderROC"
    )

    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        numFolds=3,
        seed=42
    )

    cv_model = cv.fit(train_df)
    best_model = cv_model.bestModel

    # Evaluate on test set
    predictions = best_model.transform(test_df)

    auc_roc = evaluator.evaluate(predictions)
    auc_pr  = BinaryClassificationEvaluator(
                    labelCol="label", metricName="areaUnderPR"
               ).evaluate(predictions)
    accuracy = MulticlassClassificationEvaluator(
                    labelCol="label", metricName="accuracy"
               ).evaluate(predictions)
    f1       = MulticlassClassificationEvaluator(
                    labelCol="label", metricName="f1"
               ).evaluate(predictions)

    mlflow.log_metric("auc_roc",  auc_roc)
    mlflow.log_metric("auc_pr",   auc_pr)
    mlflow.log_metric("accuracy", accuracy)
    mlflow.log_metric("f1_score", f1)

    print(f"AUC-ROC:  {auc_roc:.4f}")
    print(f"AUC-PR:   {auc_pr:.4f}")
    print(f"Accuracy: {accuracy:.4f}")
    print(f"F1 Score: {f1:.4f}")

    # Feature importance
    rf_stage = best_model.stages[-1]
    feature_importance = rf_stage.featureImportances
    logger.info(f"Feature importances: {feature_importance}")

    # Save model
    mlflow.spark.log_model(best_model, "readmission_model")
    best_model.save(MODEL_PATH)
    logger.info(f"Model saved to {MODEL_PATH}")

# COMMAND ----------

# MAGIC %md ## 4. Write Predictions to Gold Layer

# COMMAND ----------

df_all_predictions = best_model.transform(df_ml)

df_output = (
    df_all_predictions
    .withColumn("readmission_probability",
        F.round(F.element_at(F.col("probability"), 2), 4)
    )
    .withColumn("readmission_predicted",
        F.col("prediction").cast("boolean")
    )
    .withColumn("risk_band",
        F.when(F.col("readmission_probability") >= 0.70, "Very High")
         .when(F.col("readmission_probability") >= 0.50, "High")
         .when(F.col("readmission_probability") >= 0.30, "Medium")
         .otherwise("Low")
    )
    .withColumn("_scored_at", F.current_timestamp())
    .select(
        "patient_id", "hospital_id", "admission_date",
        "label", "readmission_predicted", "readmission_probability",
        "risk_band", "_scored_at"
    )
)

predictions_path = f"{GOLD_BASE}/ml_readmission_predictions"
df_output.write.format("delta").mode("overwrite") \
         .option("overwriteSchema", "true") \
         .save(predictions_path)

print(f"\n✅ ML model training complete.")
print(f"Predictions written to: {predictions_path}")
df_output.groupBy("risk_band").count().orderBy("risk_band").show()
