# Databricks notebook source
# MAGIC %md
# MAGIC ## `Lack of Memory`
# MAGIC
# MAGIC  - Spark processes large datasets in memory. If the dataset is too large to fit in memory, Spark will spill data to disk, significantly slowing down performance.
# MAGIC
# MAGIC **Solution:**
# MAGIC - Increase memory per executor or decrease the number of tasks per executor.
# MAGIC - Use `persist()` to store intermediate data efficiently in memory.
# MAGIC - Optimize the size of your dataset before processing (e.g., by filtering unnecessary data).

# COMMAND ----------

# DBTITLE 1,executor memory
# Check the available memory per executor
spark.conf.get("spark.executor.memory")

# You can configure it when starting a cluster
spark.conf.set("spark.executor.memory", "2g")  # Increase executor memory to 2GB

# COMMAND ----------

# MAGIC %md
# MAGIC `**Caching:**` It involves storing the data in-memory (RAM) for faster access during repeated computations. It’s the default behavior when you cache a DataFrame.
# MAGIC
# MAGIC `**Persistence:**` Similar to caching, but it allows specifying different storage levels (e.g., disk, memory-only, memory-and-disk).

# COMMAND ----------

# DBTITLE 1,cache and persist
# Import the time module
import time
from pyspark.sql.functions import rand
from pyspark import StorageLevel

# Create a DataFrame with 10 million rows
large_df = spark.range(0, 10000000).withColumn("random_value", rand())

# caching
large_df.cache()

# persist
large_df.persist(StorageLevel.MEMORY_AND_DISK)


# COMMAND ----------

# MAGIC %md
# MAGIC ## `Partition Size Across Dataset`
# MAGIC
# MAGIC   - Large datasets with improperly sized partitions (either too large or too small) can result in inefficient processing.
# MAGIC
# MAGIC **Solution:** 
# MAGIC   - Repartition or coalesce your dataset for better parallelism. Each partition should ideally fit into the available executor memory.

# COMMAND ----------

# Check the current number of partitions ##4
large_df.rdd.getNumPartitions()

# Repartition to a more optimal size : increase/decrease the number of partitions (shuffling)
df_repartition = large_df.repartition(6)

# coalesce to reduce the num of partitions (no shuffling)
df_coalesce = large_df.coalesce(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `Data Movement Between Executors (Shuffling)`
# MAGIC
# MAGIC   - Data shuffling (movement between executors) happens during wide transformations like groupBy(), join(), reduceByKey(). Shuffling is very costly.
# MAGIC
# MAGIC Solution:
# MAGIC - Avoid wide transformations when possible.
# MAGIC - Use broadcast joins for smaller datasets.

# COMMAND ----------

# Broadcast join
from pyspark.sql.functions import broadcast

small_df = spark.read.csv("dbfs:/FileStore/sample_financial_data.csv", header=True, inferSchema=True)

df = df.join(broadcast(small_df), "close")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `Small File Size Problem`
# MAGIC   - Spark creates many small tasks to process many small files, leading to inefficient task scheduling and overhead.
# MAGIC
# MAGIC Solution:
# MAGIC - Combine small files into fewer, larger files.
# MAGIC - Use optimize with Delta Lake to compact small files.

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE TABLE "dbx_catalog.dbx_schema.dbx_table"

# COMMAND ----------

# If using Delta Lake, you can optimize small files
df.write.format("delta").save("/path/to/delta-table")

# Compact the files
spark.sql("OPTIMIZE delta.`/path/to/delta-table`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `Predicate Push-down`
# MAGIC
# MAGIC - Predicate push-down pushes filters and column selections to the data source (e.g., Parquet or Delta) to minimize the amount of data read into Spark.
# MAGIC It works for file formats that support it like Parquet, ORC, and JDBC sources.

# COMMAND ----------

# When loading data, use column pruning and filtering to optimize data ingestion

df = spark.read.parquet("/path/to/data").select("column1", "column2").filter("column1 > 10")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `Optimizing with AQE (Adaptive Query Execution)`
# MAGIC
# MAGIC - AQE optimizes query plans at runtime based on the data being processed. It can adjust shuffle partition numbers, handle skew joins, and optimize join strategies dynamically.
# MAGIC - Enable AQE: Spark 3.x supports AQE.

# COMMAND ----------

# Enable AQE
spark.conf.set("spark.sql.adaptive.enabled", "true")

# AQE will automatically optimize skew joins, and shuffle partitions
spark.conf.set("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "64MB")  # Optimize shuffle partitions

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## From my experiance:
# MAGIC
# MAGIC `%sql:`
# MAGIC     optimize tekion_poc.bronze_schema.target_xxxlarge
# MAGIC
# MAGIC `%sql:`
# MAGIC -   ALTER TABLE tekion_poc.bronze_schema.target_xxxlarge SET TBLPROPERTIES (
# MAGIC -   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC -   'delta.autoOptimize.autoCompact' = 'true',
# MAGIC -   'delta.enableDeletionVectors' = 'true');
# MAGIC
# MAGIC `spark config:`
# MAGIC
# MAGIC - spark.executor.memory 20G
# MAGIC - spark.executor.cores 3
# MAGIC - spark.databricks.photon.scan.batchsize 256
# MAGIC - spark.config.set(“spark.databricks.delta.targetFileSize”, “128M”)

# COMMAND ----------

# MAGIC %md
# MAGIC # spark calcluations:

# COMMAND ----------

# MAGIC %md
# MAGIC - 20 node cluster => 
# MAGIC - each node ==> 16 cpu cores, 64 gb ram
# MAGIC - 
# MAGIC - 
# MAGIC - lets say each node => 3 executors
# MAGIC - 	executor size => 5 cores, 21 gb ram
# MAGIC - 
# MAGIC - total capacity of cluster ==> 20 * 3 ==> 60 executors
# MAGIC - 							  60 * 5 ==> 300 cores 
# MAGIC - 							  60 * 21 ==> 1260 gb ram 
# MAGIC - 							  
# MAGIC - total how many tasks can run on this cluster ===> 300 cores ==> 300 parallel tasks
# MAGIC - 
# MAGIC - (APPR ==> 300-400 MB per executor needs for reserved memory)

# COMMAND ----------

# MAGIC %md
# MAGIC - 4 node cluster => 
# MAGIC - each node ==> 20 cpu cores, 84 gb ram
# MAGIC - 
# MAGIC - lets say each node => 4 executors
# MAGIC - 	executor size => 5 cores, 21 gb ram
# MAGIC - 
# MAGIC - total capacity of cluster ==> 4 * 4 ==> 16 executors
# MAGIC - 							  16 * 5 ==> 80 cores 
# MAGIC - 							  16 * 21 ==> 336 gb ram 
# MAGIC - 							  
# MAGIC - total how many tasks can run on this cluster ===> 80 cores ==> 80 parallel tasks
# MAGIC - 
# MAGIC - csv file ==> 10.1gb ==> df ==> create 81 partitions (as default block size is 128 mb)
# MAGIC - 							=> means 81 total tasks but 80 cores (with 80 parallel tasks)
# MAGIC - 							=> lets say each task 10 sec 
# MAGIC - 							=> 10 sec to process all tasks as they can run in parallel  
# MAGIC - 
# MAGIC - possibility of out of memory issue :
# MAGIC - 	executor size => 5 cores, 
# MAGIC - 					21 gb ram 
# MAGIC - 						(300 mb reserved memory,
# MAGIC - 						 40% user memory to store user defined variables, data, hashmap
# MAGIC - 						 60% spark memory => 50:50 between storage and execution memory)
# MAGIC - 					we can consider 28% as execution memory => 28% of 21 gb ==> 5.88 gb for 5 cores
# MAGIC - 					for each core ==> 5.88gb/5 ==> approx 1.18 gb 
# MAGIC - 					1 partition => 128 mb => 1 core => 1 task => approx 1.18 gb =======> no memory issue
