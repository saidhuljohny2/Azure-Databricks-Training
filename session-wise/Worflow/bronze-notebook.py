# Databricks notebook source
# DBTITLE 1,bronze-schema
# MAGIC %sql
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS dbx_catalog.bronze;

# COMMAND ----------

# Importing the logger
from pyspark.sql import SparkSession

# getting the logger
logger = SparkSession.builder.getOrCreate()._jvm.org.apache.log4j.LogManager.getLogger(__name__)

# COMMAND ----------

# input_path_customers = "dbfs:/FileStore/tables/customers/"
# schema_location_customers = "dbfs:/FileStore/tables/schemaLocation/customers/"
# checkpoint_location_customers = "dbfs:/FileStore/tables/checkpointLocation/customers/"
# output_delta_customers = "dbx_catalog.bronze.customers"

# input_path_orders = "dbfs:/FileStore/tables/orders/"
# schema_location_orders = "dbfs:/FileStore/tables/schemaLocation/orders/"
# checkpoint_location_orders = "dbfs:/FileStore/tables/checkpointLocation/orders/"
# output_delta_orders = "dbx_catalog.bronze.orders"

# input_path_products = "dbfs:/FileStore/tables/products/"
# schema_location_products = "dbfs:/FileStore/tables/schemaLocation/products/"
# checkpoint_location_products = "dbfs:/FileStore/tables/checkpointLocation/products/"
# output_delta_products = "dbx_catalog.bronze.products"

# input_path_sales = "dbfs:/FileStore/tables/sales/"
# schema_location_sales = "dbfs:/FileStore/tables/schemaLocation/sales/"
# checkpoint_location_sales = "dbfs:/FileStore/tables/checkpointLocation/sales/"
# output_delta_sales = "dbx_catalog.bronze.sales"

# input_path_countries = "dbfs:/FileStore/tables/countries/"
# schema_location_countries = "dbfs:/FileStore/tables/schemaLocation/countries/"
# checkpoint_location_countries = "dbfs:/FileStore/tables/checkpointLocation/countries/"
# output_delta_countries = "dbx_catalog.bronze.countries"

# COMMAND ----------

dbutils.widgets.text("input_path_customers", "dbfs:/FileStore/tables/customers/", "Input Path Customers")
dbutils.widgets.text("schema_location_customers", "dbfs:/FileStore/tables/schemaLocation/customers/", "Schema Location Customers")
dbutils.widgets.text("checkpoint_location_customers", "dbfs:/FileStore/tables/checkpointLocation/customers/", "Checkpoint Location Customers")
dbutils.widgets.text("output_delta_customers", "dbx_catalog.bronze.customers", "Output Delta Customers")

dbutils.widgets.text("input_path_orders", "dbfs:/FileStore/tables/orders/", "Input Path Orders")
dbutils.widgets.text("schema_location_orders", "dbfs:/FileStore/tables/schemaLocation/orders/", "Schema Location Orders")
dbutils.widgets.text("checkpoint_location_orders", "dbfs:/FileStore/tables/checkpointLocation/orders/", "Checkpoint Location Orders")
dbutils.widgets.text("output_delta_orders", "dbx_catalog.bronze.orders", "Output Delta Orders")

dbutils.widgets.text("input_path_products", "dbfs:/FileStore/tables/products/", "Input Path Products")
dbutils.widgets.text("schema_location_products", "dbfs:/FileStore/tables/schemaLocation/products/", "Schema Location Products")
dbutils.widgets.text("checkpoint_location_products", "dbfs:/FileStore/tables/checkpointLocation/products/", "Checkpoint Location Products")
dbutils.widgets.text("output_delta_products", "dbx_catalog.bronze.products", "Output Delta Products")

dbutils.widgets.text("input_path_sales", "dbfs:/FileStore/tables/sales/", "Input Path Sales")
dbutils.widgets.text("schema_location_sales", "dbfs:/FileStore/tables/schemaLocation/sales/", "Schema Location Sales")
dbutils.widgets.text("checkpoint_location_sales", "dbfs:/FileStore/tables/checkpointLocation/sales/", "Checkpoint Location Sales")
dbutils.widgets.text("output_delta_sales", "dbx_catalog.bronze.sales", "Output Delta Sales")

dbutils.widgets.text("input_path_countries", "dbfs:/FileStore/tables/countries/", "Input Path Countries")
dbutils.widgets.text("schema_location_countries", "dbfs:/FileStore/tables/schemaLocation/countries/", "Schema Location Countries")
dbutils.widgets.text("checkpoint_location_countries", "dbfs:/FileStore/tables/checkpointLocation/countries/", "Checkpoint Location Countries")
dbutils.widgets.text("output_delta_countries", "dbx_catalog.bronze.countries", "Output Delta Countries")

# COMMAND ----------

input_path_customers = dbutils.widgets.get("input_path_customers")
schema_location_customers = dbutils.widgets.get("schema_location_customers")
checkpoint_location_customers = dbutils.widgets.get("checkpoint_location_customers")
output_delta_customers = dbutils.widgets.get("output_delta_customers")

input_path_orders = dbutils.widgets.get("input_path_orders")
schema_location_orders = dbutils.widgets.get("schema_location_orders")
checkpoint_location_orders = dbutils.widgets.get("checkpoint_location_orders")
output_delta_orders = dbutils.widgets.get("output_delta_orders")

input_path_products = dbutils.widgets.get("input_path_products")
schema_location_products = dbutils.widgets.get("schema_location_products")
checkpoint_location_products = dbutils.widgets.get("checkpoint_location_products")
output_delta_products = dbutils.widgets.get("output_delta_products")

input_path_sales = dbutils.widgets.get("input_path_sales")
schema_location_sales = dbutils.widgets.get("schema_location_sales")
checkpoint_location_sales = dbutils.widgets.get("checkpoint_location_sales")
output_delta_sales = dbutils.widgets.get("output_delta_sales")

input_path_countries = dbutils.widgets.get("input_path_countries")
schema_location_countries = dbutils.widgets.get("schema_location_countries")
checkpoint_location_countries = dbutils.widgets.get("checkpoint_location_countries")
output_delta_countries = dbutils.widgets.get("output_delta_countries")


# COMMAND ----------

# DBTITLE 1,customers
logger.info("starting with customers")

df_customers = (spark
                    .readStream
                    .format("cloudFiles")
                    .option("cloudFiles.format", "json")
                    .option("cloudFiles.schemaLocation", schema_location_customers)
                    .load(input_path_customers)
)

logger.info(input_path_customers)

(df_customers
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_location_customers)
    .option("mergeSchema", "true")
    .outputMode("append")
    .table(output_delta_customers)
    )

# COMMAND ----------

# DBTITLE 1,orders
df_orders = (spark
                    .readStream
                    .format("cloudFiles")
                    .option("cloudFiles.format", "json")
                    .option("cloudFiles.schemaLocation", schema_location_orders)
                    .load(input_path_orders)
)

(df_orders
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_location_orders)
    .option("mergeSchema", "true")
    .outputMode("append")
    .table(output_delta_orders)
    )

# COMMAND ----------

# DBTITLE 1,products
df_products = (spark
                    .readStream
                    .format("cloudFiles")
                    .option("cloudFiles.format", "json")
                    .option("cloudFiles.schemaLocation", schema_location_products)
                    .load(input_path_products)
)

(df_products
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_location_products)
    .option("mergeSchema", "true")
    .outputMode("append")
    .table(output_delta_products)
    )

# COMMAND ----------

# DBTITLE 1,sales
df_sales = (spark
                    .readStream
                    .format("cloudFiles")
                    .option("cloudFiles.format", "json")
                    .option("cloudFiles.schemaLocation", schema_location_sales)
                    .load(input_path_sales)
)

(df_sales
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_location_sales)
    .option("mergeSchema", "true")
    .outputMode("append")
    .table(output_delta_sales)
    )

# COMMAND ----------

# DBTITLE 1,countries
df_countries = (spark
                    .readStream
                    .format("cloudFiles")
                    .option("cloudFiles.format", "json")
                    .option("cloudFiles.schemaLocation", schema_location_countries)
                    .load(input_path_countries)
)

(df_countries
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_location_countries)
    .option("mergeSchema", "true")
    .outputMode("append")
    .table(output_delta_countries)
    )
