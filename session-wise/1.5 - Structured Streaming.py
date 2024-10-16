# Databricks notebook source
# MAGIC %md
# MAGIC ## Reading Stream

# COMMAND ----------

# DBTITLE 1,considering the delta table as stream source
(
    spark.readStream
      .table("dbx_catalog.dbx_schema.books")
      .createOrReplaceTempView("books_streaming_tmp_vw")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from books_streaming_tmp_vw;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO dbx_catalog.dbx_schema.books
# MAGIC values ("B16", "Introduction to Modeling and Simulation", "Mark W. Spong", "Computer Science", 25),
# MAGIC         ("B17", "Robot Modeling and Control", "Mark W. Spong", "Computer Science", 30),
# MAGIC         ("B18", "Turing's Vision: The Birth of Computer Science", "Chris Bernhardt", "Computer Science", 35)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW author_counts_tmp_vw as (
# MAGIC SELECT 
# MAGIC   author,
# MAGIC   count(book_id) as total_books
# MAGIC FROM books_streaming_tmp_vw
# MAGIC GROUP BY author)
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC # Writing streaming data

# COMMAND ----------

(
    spark
        .table("author_counts_tmp_vw")
        .writeStream
        .outputMode("complete")
        .trigger(processingTime='10 seconds')
        .option("checkpointLocation", "/tmp/dbx_catalog/dbx_schema/author_counts_tmp_chkpnt")
        .table("dbx_catalog.dbx_schema.author_counts")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # you can work on dataframes also 

# COMMAND ----------

stream_df = spark.readStream.table("dbx_catalog.dbx_schema.books")

# COMMAND ----------

display(stream_df)

# COMMAND ----------


