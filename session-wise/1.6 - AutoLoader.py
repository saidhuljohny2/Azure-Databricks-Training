# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Exploring The Source Directory

# COMMAND ----------

files = dbutils.fs.ls(f"dbfs:/mnt/demo-datasets/bookstore/orders-raw")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Auto Loader

# COMMAND ----------

(spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/orders_checkpoint")
        .load("dbfs:/mnt/demo-datasets/bookstore/orders-raw")
      .writeStream
        .option("checkpointLocation", "dbfs:/mnt/demo/orders_checkpoint")
        .table("shaik.prod.orders_updates")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM shaik.prod.orders_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM shaik.prod.orders_updates

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Landing New Files

# COMMAND ----------

# DBTITLE 1,COPY ACTION
dbutils.fs.cp("dbfs:/mnt/demo-datasets/bookstore/orders-new/export_004.parquet", "dbfs:/mnt/demo-datasets/bookstore/orders-raw/export_004.parquet")

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/mnt/demo-datasets/bookstore/orders-raw/")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM SHAIK.prod.orders_updates

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exploring Table History

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY shaik.prod.orders_updates

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Cleaning Up

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/demo/orders_checkpoint", True)
