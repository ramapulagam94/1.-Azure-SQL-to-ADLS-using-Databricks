# Databricks notebook source
# MAGIC %md
# MAGIC ### Import the common functions into this notebook.

# COMMAND ----------

# DBTITLE 1,Import JDBC functions from common notebook
# MAGIC %run "/Workspace/Users/ramapulagam12@gmail.com/Drafts/Project/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Column Pruning before reading the into dataframe

# COMMAND ----------

# DBTITLE 1,Column-Pruning (Read only selected columns)
customer_df= exec_sqlQuery("""
                           Select 
                           CustomerId,Title, FirstName, MiddleName, LastName, CompanyName, Phone, ModifiedDate
                           from SalesLT.customer
                           """)
display(customer_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data validations

# COMMAND ----------

# DBTITLE 1,Data Validation-Validate Source Record Count
customer_count = customer_df.count()
print(f"Customer Records : {customer_count}")

# COMMAND ----------

# DBTITLE 1,Validate Source Schema
customer_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Validate Data Types
customer_df.dtypes

# COMMAND ----------

# DBTITLE 1,Check for Null Values
from pyspark.sql.functions import col, count, when

customer_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in customer_df.columns
]).show()

# COMMAND ----------

# DBTITLE 1,Check Duplicate Records
from pyspark.sql.functions import count

customer_df.groupBy("CustomerId") \
           .count() \
           .filter("count > 1") \
           .show()

# COMMAND ----------

# DBTITLE 1,Check Duplicate Records
#If we want to find duplicate rows across all columns without explicitly listing column names, you can use customer_df.columns.

dup_df= customer_df.groupBy(customer_df.columns) \
           .count() \
           .filter("count > 1") \
           .show()

# COMMAND ----------

# DBTITLE 1,distinct rows
print(customer_df.distinct().count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load raw data into bronze layer

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, date_format

bronze_df = customer_df \
    .withColumn("ingestion_date", date_format(current_timestamp(), "dd-MM-yyyy"))\
    .withColumn("source_system", lit("Azure SQL"))

display(bronze_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing to the ALDS-Bronze location
# MAGIC - .save(path) → writes only Delta files to the specified ADLS location, doesnt create tables in UC
# MAGIC - .saveAsTable("dev_catalog.bronze.customer_enriched") will create a delta table in UC, delta files stored ADLS

# COMMAND ----------



bronze_df.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .save("abfss://global@storageaccount94adls.dfs.core.windows.net/dev/bronze/customer_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver transformations

# COMMAND ----------

# DBTITLE 1,Read ADLS data into databricks
# 2 ways to read data from ADLS, 1. from files, 2. from table

# reading delta file from ADLS location
df= spark.read.format('delta')\
              .option('header', True)\
              .option('inferSchema', True)\
              .load("abfss://global@storageaccount94adls.dfs.core.windows.net/dev/bronze/customer_bronze")

display(df)

# COMMAND ----------

# DBTITLE 1,Remove Duplicate Customers
silver_df = df.dropDuplicates(['CustomerId'])\
              .orderBy('CustomerId')

display(silver_df)

# COMMAND ----------

# DBTITLE 1,Trim Leading/Trailing Spaces
from pyspark.sql.functions import trim

silver_df = silver_df \
    .withColumn("Title", trim("Title")) \
    .withColumn("FirstName", trim("FirstName")) \
    .withColumn("MiddleName", trim("MiddleName")) \
    .withColumn("LastName", trim("LastName")) \
    .withColumn("CompanyName", trim("CompanyName")) \
    .withColumn("Phone", trim("Phone"))

display(silver_df)

# COMMAND ----------

# DBTITLE 1,Standardize Name Case
from pyspark.sql.functions import initcap

silver_df = silver_df \
    .withColumn("Title", initcap("Title")) \
    .withColumn("FirstName", initcap("FirstName")) \
    .withColumn("MiddleName", initcap("MiddleName")) \
    .withColumn("LastName", initcap("LastName"))

display(silver_df)

# COMMAND ----------

# DBTITLE 1,Create Full Name
# coalesce() replaces NULL with an empty string.
# concat_ws(" ", ...) joins the columns with a single space and ignores empty strings, so you don't end up with extra spaces.

from pyspark.sql.functions import lit, coalesce, col, concat_ws

silver_df = silver_df\
    .withColumn(
        "FullName", 
        concat_ws(
            " ",
            coalesce(col("FirstName"), lit("")),
            coalesce(col("MiddleName"), lit("")),
            coalesce(col("LastName"), lit(""))
            )
        )

display(silver_df)    



# COMMAND ----------

# DBTITLE 1,Standardize Phone Number
# Remove spaces, hyphens, and brackets.

from pyspark.sql.functions import regexp_replace

silver_df = silver_df.withColumn(
    "phone",
    regexp_replace("phone", "[^0-9]", "")
)

display(silver_df)

# COMMAND ----------

# DBTITLE 1,regexp_replace fucntion
# MAGIC %sql
# MAGIC /*
# MAGIC Breaking it down
# MAGIC
# MAGIC regexp_replace("phone", "[^0-9]", "")
# MAGIC phone → Column to modify.
# MAGIC
# MAGIC [^0-9] → Regular expression (pattern to match).
# MAGIC "" → Replace matched characters with nothing (delete them).
# MAGIC
# MAGIC What does [^0-9] mean?
# MAGIC [0-9] → Match any digit from 0 to 9.
# MAGIC ^ inside [] means NOT.
# MAGIC
# MAGIC So, [^0-9] means:
# MAGIC
# MAGIC Match every character that is NOT a digit.
# MAGIC
# MAGIC That includes:
# MAGIC Spaces
# MAGIC Hyphens (-)
# MAGIC Parentheses ((, ))
# MAGIC Plus signs (+)
# MAGIC Dots (.)
# MAGIC Any letters or symbols

# COMMAND ----------

# DBTITLE 1,Add Data Processing Timestamp
from pyspark.sql.functions import current_timestamp

silver_df = silver_df.withColumn("processed_timestamp",current_timestamp())

display(silver_df)

# COMMAND ----------

# DBTITLE 1,Reorder columns
silver_df = silver_df.select(
    "CustomerId",
    "Title",
    "FullName",
    "CompanyName",
    "phone",
    "ingestion_date",
    "processed_timestamp",
    "source_system"
)

display(silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to ADLS Gen2 Silver layer

# COMMAND ----------

# DBTITLE 1,write file to ADLS /Silver
silver_df.write\
    .mode("overwrite")\
    .format("delta")\
    .option("overwriteSchema", "true") \
    .save("abfss://global@storageaccount94adls.dfs.core.windows.net/dev/silver/customer_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read ADLS -silver delta files into databricks

# COMMAND ----------

gold_df = spark.read.format('delta')\
                    .option("header", True)\
                    .option("inferSchema", True)\
                    .load("abfss://global@storageaccount94adls.dfs.core.windows.net/dev/silver/customer_silver")
display(gold_df)

# COMMAND ----------

# DBTITLE 1,Transformations
from pyspark.sql.functions import *

gold_df = gold_df.withColumn("Master_full_name", concat_ws(" ",col("Title"),col("FullName"))) \
                 .withColumn("record_status",lit("Active"))\
                 .select("CustomerId","Title","Master_full_name","CompanyName","phone","ingestion_date","processed_timestamp","source_system","record_status")

display(gold_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write into Gold Layer-ADLS

# COMMAND ----------

gold_df.write\
    .mode("overwrite")\
    .format("delta")\
    .option("overwriteSchema", "true") \
    .save("abfss://global@storageaccount94adls.dfs.core.windows.net/dev/gold/customer_gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resigter ADLS- delta files as external tables in Unity catalog
# MAGIC the external tables are created on top of the files located in ADLS Gen2

# COMMAND ----------

# DBTITLE 1,create external tables
# MAGIC %sql
# MAGIC
# MAGIC --Note: schemas-bronze, silver, gold are created in UC using the path defined in the storage account
# MAGIC --      No need to define schema as we copying from existing file
# MAGIC
# MAGIC --bronze table
# MAGIC CREATE TABLE IF NOT EXISTS dev_catalog.bronze.bz_customer
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://global@storageaccount94adls.dfs.core.windows.net/dev/bronze/customer_bronze';
# MAGIC
# MAGIC --Silver table
# MAGIC CREATE TABLE IF NOT EXISTS dev_catalog.silver.slv_customer
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://global@storageaccount94adls.dfs.core.windows.net/dev/silver/customer_silver';
# MAGIC
# MAGIC --gold table
# MAGIC CREATE TABLE IF NOT EXISTS dev_catalog.gold.gld_customer
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://global@storageaccount94adls.dfs.core.windows.net/dev/gold/customer_gold';

# COMMAND ----------

# DBTITLE 1,Read the external tables
bz_df = spark.read.table("dev_catalog.bronze.bz_customer")
slv_df = spark.read.table("dev_catalog.silver.slv_customer")
gold_df = spark.read.table("dev_catalog.gold.gld_customer")

display(gold_df)
display(bz_df)
display(slv_df)
