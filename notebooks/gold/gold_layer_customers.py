# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

init_load_flag = int(dbutils.widgets.get("init_load_flag"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Reading

# COMMAND ----------

df = spark.sql("select * from databricks_catalog.silver.customers_silver")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Removing Duplicates

# COMMAND ----------

df = df.dropDuplicates(subset=['customer_id'])
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Dividing New vs Old Records

# COMMAND ----------

if init_load_flag == 0:
    df_old = spark.sql('''select DimCustomerKey, customer_id, create_date, update_date from databricks_catalog.gold.DimCustomers''')

else:
    df_old = spark.sql('''select 0 DimCustomerKey, 0 customer_id, 0 create_date, 0 update_date from databricks_catalog.silver.customers_silver where 1=0''')


# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Renaming column of df_old

# COMMAND ----------

df_old = df_old.withColumnRenamed("DimCustomerKey", "OldDimCustomerKey")\
    .withColumnRenamed("customer_id","Oldcustomerid")\
        .withColumnRenamed("create_date","Oldcreate_date")\
            .withColumnRenamed("update_date","Oldupdate_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Applying Join with Old Records 

# COMMAND ----------

df_join = df.join(df_old,df['customer_id'] == df['oldcustomerid'], 'left')

# COMMAND ----------

df_join.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Separating New vs Old Records

# COMMAND ----------

df_new = df_join.filter(df_join['oldDimCustomerKey'].isNull())

# COMMAND ----------

df_old = df_join.filter(df_join['oldDimCustomerKey'].isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ###Preparing df_old

# COMMAND ----------

# Dropping all the columns which are not required

df_old = df_old.drop('Oldcustomerid','Oldupdate_date')

# Renaming OldDimCustomerKey to DimCustomerKey
df_old = df_old.withColumnRenamed('OldDimCustomerKey','DimCustomerKey')

# Renaming "Oldcreate_date" column to "create_date"

df_old = df_old.withColumnRenamed("Oldcreate_date","create_date")
df_old = df_old.withColumn("create_date",to_timestamp(col("create_date")))

# Recreating "update_date" column with current timestamp
df_old = df_old.withColumn("update_date", current_timestamp())


# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Preparing df_new

# COMMAND ----------

# Dropping all the columns which are not required

df_new = df_new.drop('OldDimCustomerKey','Oldcustomerid','Oldupdate_date','oldcreate_date')

# Recreating "update_date", "current_date" columns with current timestamp

df_new = df_new.withColumn("update_date", current_timestamp())
df_new = df_new.withColumn("create_date",current_timestamp())

# COMMAND ----------

df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Surrogate Key - From 1

# COMMAND ----------

df_new = df_new.withColumn("DimCustomerKey",monotonically_increasing_id()+lit(1))

# COMMAND ----------

df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Adding Max Surrogate Key

# COMMAND ----------

if init_load_flag == 1:
    max_surrogate_key = 0

else:
    df_maxsur= spark.sql("select max(DimCustomerKey) as max_surrogate_key from databricks_catalog.gold.DimCustomers")

    # Converting df_maxsur to df_maxsurrogate_key variable
    max_surrogate_key = df_maxsur.collect()[0]['max_surrogate_key']

# COMMAND ----------

df_new = df_new.withColumn("DimCustomerKey",lit(max_surrogate_key)+col("DimCustomerKey"))

# COMMAND ----------

df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Union of df_old and df_new

# COMMAND ----------

df_final = df_new.unionByName(df_old)

# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###SCD Type 1

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if (spark.catalog.tableExists("databricks_catalog.gold.DimCustomers")):
    dlt_obj = DeltaTable.forPath(spark,"abfss://gold@<storage-account-name>.dfs.core.windows.net/DimCustomers")

    dlt_obj.alias("trg").merge(df_final.alias("src"),"trg.DimCustomerKey = src.DimCustomerKey")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
else:
    df_final.write.mode("overwrite")\
    .format("delta")\
    .option("path","abfss://gold@<storage-account-name>.dfs.core.windows.net/DimCustomers")\
    .saveAsTable("databricks_catalog.gold.DimCustomers")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_catalog.gold.dimcustomers