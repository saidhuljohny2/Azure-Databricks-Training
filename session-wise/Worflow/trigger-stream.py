# Databricks notebook source
# Importing the logger
from pyspark.sql import SparkSession

# Getting the logger
logger = SparkSession.builder.getOrCreate()._jvm.org.apache.log4j.LogManager.getLogger(__name__)

# Triggering the bronze-notebook
logger.info("Running bronze-notebook")
dbutils.notebook.run("/Workspace/Users/saidhuljohny@gmail.com/Azure-Databricks-Training/session-wise/Worflow/bronze-notebook", 100000000)
