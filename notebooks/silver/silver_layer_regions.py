# Databricks notebook source
df = spark.read.table("databricks_catalog.bronze.regions")
df.display()

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
        .save("abfss://silver@<storage-account-name>.dfs.core.windows.net/regions")

# COMMAND ----------

df = spark.read.format("delta")\
    .load("abfss://silver@<storage-account-name>.dfs.core.windows.net/regions")

df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists databricks_catalog.silver.regions_silver
# MAGIC using delta 
# MAGIC location 'abfss://silver@<storage-account-name>.dfs.core.windows.net/regions'