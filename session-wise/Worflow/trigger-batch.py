# Databricks notebook source
# Importing the logger
from pyspark.sql import SparkSession

# Getting the logger
logger = SparkSession.builder.getOrCreate()._jvm.org.apache.log4j.LogManager.getLogger(__name__)

# Triggering the silver-notebook
logger.info("Running silver-notebook")
dbutils.notebook.run("/Workspace/Users/saidhuljohny@gmail.com/Azure-Databricks-Training/session-wise/Worflow/silver-notebook", 500)

# Triggering the gold-notebook
logger.info("Running gold-notebook")
dbutils.notebook.run("/Workspace/Users/saidhuljohny@gmail.com/Azure-Databricks-Training/session-wise/Worflow/gold-notebook", 500)

