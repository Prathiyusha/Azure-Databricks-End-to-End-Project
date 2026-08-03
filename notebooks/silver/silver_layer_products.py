# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Reading

# COMMAND ----------

df = spark.read.format("parquet")\
    .load("abfss://bronze@<storage-account-name>.dfs.core.windows.net/products")

df.display()

# COMMAND ----------

df = df.drop("_rescued_data")
df.display()

# COMMAND ----------

df.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Functions

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function databricks_catalog.bronze.discount_func(p_price double)
# MAGIC returns double
# MAGIC language sql
# MAGIC return p_price * 0.90

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id, price, databricks_catalog.bronze.discount_func(price) as discounted_price
# MAGIC from products

# COMMAND ----------

df = df.withColumn("discounted_price",expr("databricks_catalog.bronze.discount_func(price)"))
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function databricks_catalog.bronze.upper_func(p_brand string)
# MAGIC returns string
# MAGIC language python
# MAGIC as 
# MAGIC $$
# MAGIC     return p_brand.upper()
# MAGIC $$

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id, brand, databricks_catalog.bronze.upper_func(brand) as brand_upper 
# MAGIC from products

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Writing

# COMMAND ----------

df.write.format("delta")\
    .mode("append")\
    .option("path","abfss://silver@<storage-account-name>.dfs.core.windows.net/products")\
        .save()

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists databricks_catalog.silver.products_silver
# MAGIC using delta 
# MAGIC location 'abfss://silver@<storage-account-name>.dfs.core.windows.net/products'