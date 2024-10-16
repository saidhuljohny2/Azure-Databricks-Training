-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Delta Lake
-- MAGIC
-- MAGIC - Delta Lake is an open-source storage layer developed by Databricks that brings ACID transactions (Atomicity, Consistency, Isolation, Durability) to Apache Spark and big data workloads. It enhances data lakes by providing reliability, scalability, and performance for batch and streaming data.
-- MAGIC 	
-- MAGIC ### Key Features of Delta Lake:
-- MAGIC - ACID Transactions: Delta Lake allows concurrent reads and writes by different users while ensuring data consistency.
-- MAGIC - Versioning: Delta Lake stores previous versions of the data, enabling time travel, which allows you to query older versions of your data or restore previous states.
-- MAGIC - Schema Enforcement: It prevents bad data from entering your data lakes by enforcing schemas on write.
-- MAGIC - Data Quality: Delta Lake ensures that only complete and accurate data is written, Builds upon standard data formats: Parquet + Json
-- MAGIC - Streaming and Batch Processing: Supports both streaming and batch data processing seamlessly.
-- MAGIC - Scalability: Designed to handle petabyte-scale datasets.
-- MAGIC

-- COMMAND ----------


CREATE CATALOG IF NOT EXISTS dbx_catalog
MANAGED LOCATION 'abfss://unity-catalog-storage@dbstorageyf6y4b6akjy6m.dfs.core.windows.net/170383088377031/dbx_catalog';

-- COMMAND ----------

USE CATALOG dbx_catalog;
CREATE SCHEMA IF NOT EXISTS dbx_catalog.dbx_schema;

-- COMMAND ----------

-- DBTITLE 1,CREATE
-- delta tables

CREATE TABLE dbx_catalog.dbx_schema.employees(
    id INT,
    name STRING,
    salary FLOAT
);

-- COMMAND ----------

-- DBTITLE 1,INSERT
INSERT INTO dbx_catalog.dbx_schema.employees
VALUES 
  (1, "Adam", 3500.0),
  (2, "Sarah", 4020.5);

INSERT INTO dbx_catalog.dbx_schema.employees
VALUES
  (3, "John", 2999.3),
  (4, "Thomas", 4000.3);

INSERT INTO dbx_catalog.dbx_schema.employees
VALUES
  (5, "Anna", 2500.0);

INSERT INTO dbx_catalog.dbx_schema.employees
VALUES
  (6, "Kim", 6200.3);

-- COMMAND ----------

SELECT * FROM dbx_catalog.dbx_schema.employees

-- COMMAND ----------

-- DBTITLE 1,describe
DESCRIBE DETAIL dbx_catalog.dbx_schema.employees;

-- COMMAND ----------

DESCRIBE HISTORY dbx_catalog.dbx_schema.employees;

-- COMMAND ----------

-- DBTITLE 1,VERSIONS
SELECT * FROM dbx_catalog.dbx_schema.employees@V4;

-- COMMAND ----------

-- DBTITLE 1,UPDATE
UPDATE dbx_catalog.dbx_schema.employees 
SET salary = salary + 100
WHERE id = 1;

-- COMMAND ----------

DESCRIBE HISTORY dbx_catalog.dbx_schema.employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Multiple files

-- COMMAND ----------

-- DBTITLE 1,External Delta Table
-- MAGIC %python
-- MAGIC
-- MAGIC books_df = spark.read.csv("dbfs:/mnt/adls_container/books-data.csv", header=True, sep=";")
-- MAGIC
-- MAGIC books_df.write.format("delta").mode("append").save("dbfs:/mnt/adls_container/books-data-delta/")

-- COMMAND ----------

-- DBTITLE 1,Managed Delta Table
-- MAGIC %python
-- MAGIC
-- MAGIC books_df = spark.read.csv("dbfs:/mnt/adls_container/books-data.csv", header=True, sep=";")
-- MAGIC
-- MAGIC books_df.write.format("delta").mode("append").saveAsTable("dbx_catalog.dbx_schema.books")

-- COMMAND ----------

-- DBTITLE 1,Managed Delta Table
-- MAGIC %python
-- MAGIC
-- MAGIC customers_df = spark.read.format("json").load("dbfs:/mnt/adls_container/customers-data.json")
-- MAGIC
-- MAGIC customers_df.write.format("delta").mode("overwrite").saveAsTable("dbx_catalog.dbx_schema.customers")

-- COMMAND ----------

-- DBTITLE 1,Managed Delta Table
-- MAGIC %python
-- MAGIC
-- MAGIC orders_df = spark.read.format("parquet").load("dbfs:/FileStore/export_001.parquet")
-- MAGIC
-- MAGIC orders_df.write.format("delta").mode("overwrite").saveAsTable("dbx_catalog.dbx_schema.orders")
