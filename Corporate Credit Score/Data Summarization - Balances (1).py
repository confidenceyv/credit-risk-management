# Databricks notebook source
#start a spark session
from pyspark.sql import SparkSession, SQLContext
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# Enable storage as a temporary directory
storage_account_name = "ubariskdl"

storage_account_key = "WIltzoJWmkC9x1pmXadJggmodCMFHU5A8JgS8eoInZkaoNZ9acraHrkALVOQgjtYCHgcHiQbTPhyO1DK1slrew=="
storage_container_name = "staging"

temp_dir_url =  "wasbs://staging@ubariskdl.blob.core.windows.net/".format(storage_container_name, storage_account_name)

spark_config_key = "fs.azure.account.key.ubariskdl.blob.core.windows.net".format(storage_account_name)
spark_config_value = storage_account_key

spark.conf.set(spark_config_key, spark_config_value)

# COMMAND ----------

# connect to SQL datawarehouse
servername = "ubariskdbsvr.database.windows.net"
databasename = "biadmin02"
password = "sqlAdmin123#"


sql_dw_connection_string = "jdbc:sqlserver://ubariskdbsvr.database.windows.net:1433;database=ubariskmldb;user=biadmin02@ubariskdbsvr;password=sqlAdmin123#;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;".format(servername, databasename, servername, password)

# COMMAND ----------

# DBTITLE 1,Get Data
#querying data in SQL datawarehouse
query = "SELECT  *  FROM [biuser].[rpt_predict_data_final_corp]"

df = spark.read \
  .format("com.databricks.spark.sqldw") \
  .option("url", sql_dw_connection_string) \
  .option("tempdir", temp_dir_url) \
  .option("forward_spark_azure_storage_credentials", "true") \
  .option("query", query) \
  .load()

# COMMAND ----------

pd_df = df.toPandas()

# COMMAND ----------

# DBTITLE 1,One Year Balance
one_year_balance_columns = pd_df.iloc[:,53:65]
pd_df['one_year_average_balance'] = one_year_balance_columns.mean(axis =1)
#pd_df.head()

# COMMAND ----------

# DBTITLE 1,Six Months Balance
# for First Six Months
first_six_months_balance_columns = pd_df.iloc[:,53:59]
pd_df['first_six_months_average_balance'] = first_six_months_balance_columns.mean(axis =1)
#pd_df.head()

# COMMAND ----------

# for last Six Months
last_six_months_balance_columns = pd_df.iloc[:,59:65]
pd_df['last_six_months_average_balance'] = last_six_months_balance_columns.mean(axis =1)
#pd_df.head()

# COMMAND ----------

# DBTITLE 1,Last 3 Months
# for last Three Months
last_three_months_balance_columns = pd_df.iloc[:,62:65]
pd_df['last_three_months_average_balance'] = last_three_months_balance_columns.mean(axis =1)
#pd_df.head()

# COMMAND ----------

# DBTITLE 1,Convert To Spark DataFrame
#convert the dataset back to Pyspark and Export it back to the database
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
sqlCtx = SQLContext(sc)
df = sqlCtx.createDataFrame(pd_df)

# COMMAND ----------

# DBTITLE 1,Export to Database
#Writing the result of the Data Preparation to the database
df.write \
.format("com.databricks.spark.sqldw")\
.option("url", "jdbc:sqlserver://ubariskdbsvr.database.windows.net:1433;database=ubariskmldb")\
.option("user", "biadmin02@ubariskdbsvr")\
.option("password", "sqlAdmin123#")\
.option("tempDir", "wasbs://staging@ubariskdl.blob.core.windows.net/")\
.option("forward_spark_azure_storage_credentials", "true")\
.option("dbtable", "stg.dataprep_summarized_balances_corp")\
.mode("overwrite")\
.save()

# COMMAND ----------

