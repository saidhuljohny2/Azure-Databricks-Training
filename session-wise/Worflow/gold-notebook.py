# Databricks notebook source
# DBTITLE 1,gold-schema
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS dbx_catalog.gold;

# COMMAND ----------

# DBTITLE 1,import libraries
from pyspark.sql.functions import sum, count, desc, rank, date_format, col, current_date,lit, from_utc_timestamp, current_timestamp
from pyspark.sql.types import StringType
from pyspark.sql import Window

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

# COMMAND ----------

products_df = spark.read.table('dbx_catalog.silver.products')
orders_df = spark.read.table('dbx_catalog.silver.orders')
sales_df = spark.read.table('dbx_catalog.silver.sales')

# COMMAND ----------

# Find the top 5 customers with the highest number of orders and write to a new table

top_customers_df = (orders_df
                        .groupBy('CustomerId')
                        .agg(count('OrderId').alias('order_count'))
                        .orderBy(desc('order_count'))
                        .limit(5)
)

top_customers_df.write.format("delta").mode("overwrite").option("mergeSchema", "True").saveAsTable("dbx_catalog.gold.top_customers")

# COMMAND ----------

# Find most sold product in each month and write to the gold table

sales_at_month_df = products_df.join(sales_df, sales_df.ProductId == products_df.ProductId,"inner")\
                                .join(orders_df, sales_df.OrderId == orders_df.OrderId, "inner")\
                                .withColumn("Order_MonthYear", date_format("Date","MMM-YYYY"))\
                                .withColumn("Order_Month", date_format("Date","MM"))\
                                .withColumn("Order_Year", date_format("Date","YYYY"))\
                                .groupBy("Name","Order_MonthYear","Order_Month","Order_Year")\
                                .agg(count("SaleId").alias("total_sales"))\
                                .orderBy(desc("total_sales"))        

window = rank().over(Window.partitionBy(sales_at_month_df.Order_MonthYear).orderBy(desc(sales_at_month_df.total_sales)))

top_sold_product_month_df = sales_at_month_df.withColumn("rank",window)\
                                             .filter("rank == 1")\
                                             .select(col('Order_MonthYear'), col('Name'),col('total_sales'))\
                                             .withColumn("InsertedDatetime", from_utc_timestamp(current_timestamp(),'America/New_York'))\
                                             .orderBy(desc(col('Order_Year')), desc(col('Order_Month')))

# Overwrite the gold table with the processed data:

top_sold_product_month_df.write.format("delta").mode("overwrite").option("mergeSchema","True").saveAsTable("dbx_catalog.gold.top_sold_products")




# COMMAND ----------

# Question 3: Calculate the average order value per customer and write to a new table

# Question 4: Find the total sales per product and write to a new table

# Question 5: Identify the top 3 countries by the number of products manufactured and write to a new table

