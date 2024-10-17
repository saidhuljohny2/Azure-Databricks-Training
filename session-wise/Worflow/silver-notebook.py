# Databricks notebook source
# DBTITLE 1,silver-schema
# MAGIC %sql
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS dbx_catalog.silver;

# COMMAND ----------

# DBTITLE 1,import library section
from pyspark.sql.functions import from_utc_timestamp, current_timestamp, lit, StringType, to_date
from delta.tables import DeltaTable
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

# COMMAND ----------

# Read the data from the delta tables and save it to dataframe

products_df = spark.read.table('dbx_catalog.bronze.products')
orders_df = spark.read.table('dbx_catalog.bronze.orders')
sales_df = spark.read.table('dbx_catalog.bronze.sales')
customers_df = spark.read.table('dbx_catalog.bronze.customers')
countries_df = spark.read.table('dbx_catalog.bronze.countries')

# COMMAND ----------

display(customers_df.limit(2))

# COMMAND ----------

# DBTITLE 1,UDF
def istableExists(tableName):
    status = True
    try:
        spark.table(tableName)
    except Exception as e:
        if( "TABLE_OR_VIEW_NOT_FOUND" in str(e)):
            status = False
    return status  

# COMMAND ----------

# DBTITLE 1,customers
# Clean, transform the customers data and save it to a silver layer.

customers_silver_merge_df = (customers_df
                                .withColumn("InsertedDatetime",from_utc_timestamp(current_timestamp(),'America/New_York'))
                                .withColumn("UpdatedDatetime", from_utc_timestamp(current_timestamp(),'America/New_York'))
)

customers_silver_insert_df = (customers_df
                                .withColumn("InsertedDatetime",from_utc_timestamp(current_timestamp(),'America/New_York')) \
                                .withColumn("UpdatedDatetime", lit(None).cast(StringType()))
)


# Define a window spec partitioned by CustomerID and ordered by UpdatedDatetime in descending order
windowSpec = Window.partitionBy("CustomerID").orderBy(col("UpdatedDatetime").desc())

# Add a row number for each row within each partition of CustomerID
customers_silver_merge_df = customers_silver_merge_df.withColumn("row_num", row_number().over(windowSpec))

# Filter to keep only the first row for each CustomerID, which is the most recent
customers_silver_merge_df = customers_silver_merge_df.filter(col("row_num") == 1).drop("row_num")

# Now proceed with the merge operation as before
if istableExists("dbx_catalog.silver.customers"):
    print("performing the merge operation")

    customers_silver_deltaTable = DeltaTable.forName(spark, "dbx_catalog.silver.customers")

    (customers_silver_deltaTable.alias("customers")
        .merge(customers_silver_merge_df.alias("updates"), "customers.CustomerID = updates.CustomerID")
        .whenMatchedUpdate(set={
            "CustomerId": "updates.CustomerId",
            "Email": "updates.Email",
            "Name": "updates.Name",
            "Active": "updates.Active",
            "Address":"updates.Address",
            "City":"updates.City",
            "Country":"updates.Country",
            "UpdatedDatetime" :"updates.UpdatedDatetime"
        })
        .whenNotMatchedInsert(values={
            "CustomerId": "updates.CustomerId",
            "Email": "updates.Email",
            "Name": "updates.Name",
            "Active": "updates.Active",
            "Address":"updates.Address",
            "City":"updates.City",
            "Country":"updates.Country",
            "InsertedDatetime" :"updates.InsertedDatetime"
        })
        .execute())

else:
    print("Creating table for the first time")
    customers_silver_insert_df.write.format("delta").mode("overwrite").saveAsTable("dbx_catalog.silver.customers")

# COMMAND ----------

# DBTITLE 1,orders
# Clean, transform the orders data and save it to a silver layer.

orders_silver_merge_df = (orders_df
                                .withColumn("InsertedDatetime",from_utc_timestamp(current_timestamp(),'America/New_York'))
                                .withColumn("UpdatedDatetime", from_utc_timestamp(current_timestamp(),'America/New_York'))
)

orders_silver_insert_df = (orders_df
                                .withColumn("InsertedDatetime",from_utc_timestamp(current_timestamp(),'America/New_York')) \
                                .withColumn("UpdatedDatetime", lit(None).cast(StringType()))
)


# Define a window spec partitioned by OrderId and ordered by UpdatedDatetime in descending order
windowSpec = Window.partitionBy("OrderId").orderBy(col("UpdatedDatetime").desc())

# Add a row number for each row within each partition of OrderId
orders_silver_merge_df = orders_silver_merge_df.withColumn("row_num", row_number().over(windowSpec))

# Filter to keep only the first row for each OrderId, which is the most recent
orders_silver_merge_df = orders_silver_merge_df.filter(col("row_num") == 1).drop("row_num")

# Now proceed with the merge operation as before
if istableExists("dbx_catalog.silver.orders"):
    print("performing the merge operation")

    orders_silver_deltaTable = DeltaTable.forName(spark, "dbx_catalog.silver.orders")

    (orders_silver_deltaTable.alias("orders")
        .merge(orders_silver_merge_df.alias("updates"), "orders.OrderId = updates.OrderId")
        .whenMatchedUpdate(set={
            "OrderId": "updates.OrderId",
            "CustomerId": "updates.CustomerId",
            "Date": "updates.Date",
            "UpdatedDatetime": "updates.UpdatedDatetime"
        })
        .whenNotMatchedInsert(values={
            "OrderId": "updates.OrderId",
            "CustomerId": "updates.CustomerId",
            "Date": "updates.Date",
            "UpdatedDatetime": "updates.UpdatedDatetime"
        })
        .execute())

else:
    print("Creating table for the first time")
    orders_silver_insert_df.write.format("delta").mode("overwrite").saveAsTable("dbx_catalog.silver.orders")

# COMMAND ----------

# DBTITLE 1,products
# Clean, transform the products data and save it to a silver layer.

products_silver_merge_df = (products_df
                                .withColumn("InsertedDatetime",from_utc_timestamp(current_timestamp(),'America/New_York'))
                                .withColumn("UpdatedDatetime", from_utc_timestamp(current_timestamp(),'America/New_York'))
)

products_silver_insert_df = (products_df
                                .withColumn("InsertedDatetime",from_utc_timestamp(current_timestamp(),'America/New_York')) \
                                .withColumn("UpdatedDatetime", lit(None).cast(StringType()))
)


# Define a window spec partitioned by ProductId and ordered by UpdatedDatetime in descending order
windowSpec = Window.partitionBy("ProductId").orderBy(col("UpdatedDatetime").desc())

# Add a row number for each row within each partition of ProductId
products_silver_merge_df = products_silver_merge_df.withColumn("row_num", row_number().over(windowSpec))

# Filter to keep only the first row for each ProductId, which is the most recent
products_silver_merge_df = products_silver_merge_df.filter(col("row_num") == 1).drop("row_num")

# Now proceed with the merge operation as before
if istableExists("dbx_catalog.silver.products"):
    print("performing the merge operation")

    products_silver_deltaTable = DeltaTable.forName(spark, "dbx_catalog.silver.products")

    (products_silver_deltaTable.alias("products")
        .merge(products_silver_merge_df.alias("updates"), "products.ProductId = updates.ProductId")
        .whenMatchedUpdate(set={
            "ProductId": "updates.ProductId",
            "Name": "updates.Name",
            "ManufacturedCountry":"updates.ManufacturedCountry",
            "WeightGrams": "updates.WeightGrams",
            "UpdatedDatetime" :"updates.UpdatedDatetime"
        })
        .whenNotMatchedInsert(values={
            "ProductId": "updates.ProductId",
            "Name": "updates.Name",
            "ManufacturedCountry":"updates.ManufacturedCountry",
            "WeightGrams": "updates.WeightGrams",
            "UpdatedDatetime" :"updates.UpdatedDatetime"
        })
        .execute())

else:
    print("Creating table for the first time")
    products_silver_insert_df.write.format("delta").mode("overwrite").saveAsTable("dbx_catalog.silver.products")

# COMMAND ----------

# DBTITLE 1,sales
# Clean, transform the sales data and save it to a silver layer.

sales_silver_merge_df = (sales_df
                                .withColumn("InsertedDatetime",from_utc_timestamp(current_timestamp(),'America/New_York'))
                                .withColumn("UpdatedDatetime", from_utc_timestamp(current_timestamp(),'America/New_York'))
)

sales_silver_insert_df = (sales_df
                                .withColumn("InsertedDatetime",from_utc_timestamp(current_timestamp(),'America/New_York')) \
                                .withColumn("UpdatedDatetime", lit(None).cast(StringType()))
)


# Define a window spec partitioned by SaleId and ordered by UpdatedDatetime in descending order
windowSpec = Window.partitionBy("SaleId").orderBy(col("UpdatedDatetime").desc())

# Add a row number for each row within each partition of SaleId
sales_silver_merge_df = sales_silver_merge_df.withColumn("row_num", row_number().over(windowSpec))

# Filter to keep only the first row for each SaleId, which is the most recent
sales_silver_merge_df = sales_silver_merge_df.filter(col("row_num") == 1).drop("row_num")

# Now proceed with the merge operation as before
if istableExists("dbx_catalog.silver.sales"):
    print("performing the merge operation")

    sales_silver_deltaTable = DeltaTable.forName(spark, "dbx_catalog.silver.sales")

    (sales_silver_deltaTable.alias("sales")
        .merge(sales_silver_merge_df.alias("updates"), "sales.SaleId = updates.SaleId")
        .whenMatchedUpdate(set={
            "SaleId": "updates.SaleId",
            "OrderId": "updates.OrderId",
            "ProductId": "updates.ProductId",
            "Quantity": "updates.Quantity",
            "UpdatedDatetime" :"updates.UpdatedDatetime"
        })
        .whenNotMatchedInsert(values={
            "SaleId": "updates.SaleId",
            "OrderId": "updates.OrderId",
            "ProductId": "updates.ProductId",
            "Quantity": "updates.Quantity",
            "UpdatedDatetime" :"updates.UpdatedDatetime"
        })
        .execute())

else:
    print("Creating table for the first time")
    sales_silver_insert_df.write.format("delta").mode("overwrite").saveAsTable("dbx_catalog.silver.sales")

# COMMAND ----------

# DBTITLE 1,counties
# Clean, transform the countries data and save it to a silver layer.

countries_silver_merge_df = (countries_df
                                .withColumn("InsertedDatetime",from_utc_timestamp(current_timestamp(),'America/New_York'))
                                .withColumn("UpdatedDatetime", from_utc_timestamp(current_timestamp(),'America/New_York'))
)

countries_silver_insert_df = (countries_df
                                .withColumn("InsertedDatetime",from_utc_timestamp(current_timestamp(),'America/New_York')) \
                                .withColumn("UpdatedDatetime", lit(None).cast(StringType()))
)


# Define a window spec partitioned by Country and ordered by UpdatedDatetime in descending order
windowSpec = Window.partitionBy("Country").orderBy(col("UpdatedDatetime").desc())

# Add a row number for each row within each partition of Country
countries_silver_merge_df = countries_silver_merge_df.withColumn("row_num", row_number().over(windowSpec))

# Filter to keep only the first row for each Country, which is the most recent
countries_silver_merge_df = countries_silver_merge_df.filter(col("row_num") == 1).drop("row_num")

# Now proceed with the merge operation as before
if istableExists("dbx_catalog.silver.countries"):
    print("performing the merge operation")

    countries_silver_deltaTable = DeltaTable.forName(spark, "dbx_catalog.silver.countries")

    (countries_silver_deltaTable.alias("countries")
        .merge(countries_silver_merge_df.alias("updates"), "countries.Country = updates.Country")
        .whenMatchedUpdate(set={
            "Country": "updates.Country",
            "Currency": "updates.Currency",
            "Name": "updates.Name",
            "Region": "updates.Region",
            "Population": "updates.Population",
            "Area_sq_mi": "updates.Area_sq_mi",
            "Pop_Density_per_sq_mi": "updates.Pop_Density_per_sq_mi",
            "Coastline_coast_per_area_ratio": "updates.Coastline_coast_per_area_ratio",
            "Net_migration": "updates.Net_migration",
            "Infant_mortality_per_1000_births": "updates.Infant_mortality_per_1000_births",
            "GDP_per_capita": "updates.GDP_per_capita",
            "Literacy_percent": "updates.Literacy_percent",
            "Phones_per_1000": "updates.Phones_per_1000",
            "Arable_percent": "updates.Arable_percent",
            "Crops_percent": "updates.Crops_percent",
            "Other_percent": "updates.Other_percent",
            "Climate": "updates.Climate",
            "Birthrate": "updates.Birthrate",
            "Deathrate": "updates.Deathrate",
            "Agriculture": "updates.Agriculture",
            "Industry": "updates.Industry",
            "Service": "updates.Service",
            "UpdatedDatetime": "updates.UpdatedDatetime"
        })
        .whenNotMatchedInsert(values={
            "Country": "updates.Country",
            "Currency": "updates.Currency",
            "Name": "updates.Name",
            "Region": "updates.Region",
            "Population": "updates.Population",
            "Area_sq_mi": "updates.Area_sq_mi",
            "Pop_Density_per_sq_mi": "updates.Pop_Density_per_sq_mi",
            "Coastline_coast_per_area_ratio": "updates.Coastline_coast_per_area_ratio",
            "Net_migration": "updates.Net_migration",
            "Infant_mortality_per_1000_births": "updates.Infant_mortality_per_1000_births",
            "GDP_per_capita": "updates.GDP_per_capita",
            "Literacy_percent": "updates.Literacy_percent",
            "Phones_per_1000": "updates.Phones_per_1000",
            "Arable_percent": "updates.Arable_percent",
            "Crops_percent": "updates.Crops_percent",
            "Other_percent": "updates.Other_percent",
            "Climate": "updates.Climate",
            "Birthrate": "updates.Birthrate",
            "Deathrate": "updates.Deathrate",
            "Agriculture": "updates.Agriculture",
            "Industry": "updates.Industry",
            "Service": "updates.Service",
            "UpdatedDatetime": "updates.UpdatedDatetime"
        })
        .execute())

else:
    print("Creating table for the first time")
    countries_silver_insert_df.write.format("delta").mode("overwrite").saveAsTable("dbx_catalog.silver.countries")
