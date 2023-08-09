# Databricks notebook source
# Enable storage as a temporary directory
storage_account_name = "xxxxxx"
storage_account_key = "xxxxxx=="
storage_container_name = "dbtemp"
temp_dir_url =  "wasbs://xxxxxx.blob.core.windows.net/".format(storage_container_name, storage_account_name)
spark_config_key = "fs.azure.account.key.ubariskdl.blob.core.windows.net".format(storage_account_name)
spark_config_value = storage_account_key

spark.conf.set(spark_config_key, spark_config_value)

# COMMAND ----------

# connect to SQL datawarehouse
servername = "xxxxxx"
databasename = "xxxxxx"
password = "xxxxxx#"
user = "xxxxxx"
sql_dw_connection_string = "jdbc:sqlserver://xxxxxx.database.windows.net:1433;database=xxxxxx;user=xxxxxx;password=xxxxxx#;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;".format(servername, databasename, servername, password,user)

# COMMAND ----------

#querying data in SQL datawarehouse
query = "SELECT  * from biuser.rpt_predict_loan_data_corp"

df = spark.read \
  .format("com.databricks.spark.sqldw") \
  .option("url", sql_dw_connection_string) \
  .option("tempdir", temp_dir_url) \
  .option("forward_spark_azure_storage_credentials", "true") \
  .option("query", query) \
  .load()

# COMMAND ----------

df = df.toPandas()

# COMMAND ----------

df.head()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

