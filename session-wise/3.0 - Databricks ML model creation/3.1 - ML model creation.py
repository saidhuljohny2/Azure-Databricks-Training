# Databricks notebook source
# DBTITLE 1,data loading
data_path = "dbfs:/FileStore/sample_financial_data.csv"

financial_df = spark.read.csv(data_path, header=True, inferSchema=True)

display(financial_df.limit(5))

# COMMAND ----------

# DBTITLE 1,data cleaning
financial_df = financial_df.dropDuplicates()

display(financial_df.limit(5))

# COMMAND ----------

# DBTITLE 1,feature engineering
# we'll use the open, high, low, and volume columns to predict the close price. We will transform the selected columns into features.

from pyspark.ml.feature import VectorAssembler

# Combine relevant columns into a single feature vector
assembler = VectorAssembler(inputCols=["open", "high", "low", "volume"], outputCol="features")
financial_df = assembler.transform(financial_df)

financial_df.select("*").show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,modelling
# Split the data into training (80%) and test (20%) datasets

train_data, test_data = financial_df.randomSplit([0.8, 0.2], seed=12345)

# COMMAND ----------

# DBTITLE 1,training the model Linear Regression
from pyspark.ml.regression import LinearRegression

# Create and train a Linear Regression model (y = mx+c)
lr = LinearRegression(featuresCol="features", labelCol="close")
lr_model = lr.fit(train_data)

print(f"coefficients: {lr_model.coefficients}")
print(f"intercept: {lr_model.intercept}")

# COMMAND ----------

# DBTITLE 1,make predictions
# Make predictions on the test data

predictions = lr_model.transform(test_data)
predictions.show(10, truncate=False)

# COMMAND ----------

# DBTITLE 1,evaludate model
from pyspark.ml.evaluation import RegressionEvaluator

# Create an evaluator to compute RMSE
evaluator = RegressionEvaluator(labelCol='close', predictionCol='prediction', metricName='rmse')

# Calculate RMSE
rmse = evaluator.evaluate(predictions)
print(f"Root Mean Squared Error (RMSE) on test data: {rmse}")

# COMMAND ----------

# DBTITLE 1,model deployment
import mlflow
import mlflow.spark
import mlflow.pyfunc

# Log the model for deployment
mlflow.spark.log_model(spark_model=lr_model, artifact_path="model")

# Deploy the model as a REST API for serving predictions
mlflow.pyfunc.log_model(artifact_path="model", python_model=lr_model)
