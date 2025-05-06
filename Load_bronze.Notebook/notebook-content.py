# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "631b5b46-a146-4039-afaf-8b053c25d5f2",
# META       "default_lakehouse_name": "Bronze",
# META       "default_lakehouse_workspace_id": "08919537-de39-4b97-b808-74015bf5b473",
# META       "known_lakehouses": [
# META         {
# META           "id": "631b5b46-a146-4039-afaf-8b053c25d5f2"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Load_Bronze
# 
# ### This notebook's purposes is to load the different tables into the bronze lakehouse delta table. 

# MARKDOWN ********************

# ### Parameters

# PARAMETERS CELL ********************

#Infer base parameters from the pipeline context

schemaName = ""
tableName = ""
filePath = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import current_date

# Create schema
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {schemaName}')

# Drop table
spark.sql(f'DROP TABLE IF EXISTS {schemaName}.{tableName}')

# Read data
df = spark.read.parquet(f"Files/{schemaName}/{filePath}/{tableName}.parquet")

# Add metadata loading_date column using current date
df2 = df.withColumn("loading_date", current_date().cast("string"))

# Overwrite table
df2.write.mode("Overwrite").saveAsTable(f"{schemaName}.{tableName}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
