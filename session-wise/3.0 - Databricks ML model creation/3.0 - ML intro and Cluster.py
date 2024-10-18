# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## `Introduction to ML and Data Science Workflows in Databricks`
# MAGIC
# MAGIC
# MAGIC Machine Learning (ML) and Data Science workflows typically involve data preparation, model building, evaluation, deployment, and monitoring. Databricks provides a collaborative platform for running data science and machine learning workflows efficiently, with built-in support for big data and distributed computing.
# MAGIC
# MAGIC ### `Key Components:`
# MAGIC -   Data Ingestion: Loading and cleaning raw data.
# MAGIC -   Feature Engineering: Transforming raw data into features suitable for model training.
# MAGIC -   Modeling: Using algorithms to train predictive models.
# MAGIC -   Evaluation: Assessing model performance.
# MAGIC -   Deployment: Deploying the model for serving predictions.
# MAGIC
# MAGIC To run machine learning workloads, you need to create and configure clusters in Databricks. Follow these steps to set up a cluster optimized for ML:
# MAGIC
# MAGIC 1. Go to Clusters in the Databricks workspace.
# MAGIC 2. Click on Create Cluster.
# MAGIC 3. Select Cluster Type:
# MAGIC    - Choose a memory-optimized cluster or a GPU cluster (if you're working with deep learning models).
# MAGIC    - For ML workloads, you can use standard instances like r5.xlarge (16 vCPUs, 128 GB RAM) for CPU-based ML models.
# MAGIC    - For deep learning, use GPU instances like p3.2xlarge (8 vCPUs, 61 GB RAM, 1 Tesla V100 GPU).
# MAGIC
# MAGIC **Spark Configuration:**
# MAGIC
# MAGIC - You can specify the Spark configurations for your cluster based on the data size, compute needs, and optimizations.
# MAGIC - Example of enabling auto-scaling:
# MAGIC
# MAGIC   `spark.conf.set("spark.dynamicAllocation.enabled", "true")`
# MAGIC
# MAGIC **Install Libraries:**
# MAGIC
# MAGIC Add required ML libraries such as Scikit-learn, XGBoost, TensorFlow, PyTorch, or any other necessary package.
# MAGIC Go to Libraries > Install New > PyPI and add libraries like:
# MAGIC - scikit-learn
# MAGIC - pandas
# MAGIC - matplotlib
# MAGIC - mlflow

# COMMAND ----------

# DBTITLE 1,Cluster Clearing
# MAGIC %md
# MAGIC # Cluster Creation
# MAGIC

# COMMAND ----------

from IPython.display import Image, display

display(Image(filename="/Workspace/Users/saidhuljohny@gmail.com/Azure-Databricks-Training/session-wise/3.0 - Databricks ML model creation/ML CLUSTER.png"))
