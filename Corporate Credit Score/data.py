# Databricks notebook source
#start a spark session
from pyspark.sql import SparkSession, SQLContext
spark = SparkSession.builder.appName('cluster').getOrCreate()

# COMMAND ----------

# Enable storage as a temporary directory
storage_account_name = "ubariskdl"
storage_account_key = "WIltzoJWmkC9x1pmXadJggmodCMFHU5A8JgS8eoInZkaoNZ9acraHrkALVOQgjtYCHgcHiQbTPhyO1DK1slrew=="
storage_container_name = "dbtemp"
temp_dir_url =  "wasbs://dbtemp@ubariskdl.blob.core.windows.net/".format(storage_container_name, storage_account_name)
spark_config_key = "fs.azure.account.key.ubariskdl.blob.core.windows.net".format(storage_account_name)
spark_config_value = storage_account_key

spark.conf.set(spark_config_key, spark_config_value)

# COMMAND ----------

# connect to SQL datawarehouse
servername = "ubamlasvr.database.windows.net"
databasename = "ubariskmldb"
password = "sqlAdmin123#"
user = "biadmin02"
sql_dw_connection_string = "jdbc:sqlserver://ubariskdbsvr.database.windows.net:1433;database=ubariskmldb;user=biadmin02@ubariskdbsvr;password=sqlAdmin123#;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;".format(servername, databasename, servername, password,user)

# COMMAND ----------

#querying data in SQL datawarehouse
#query = "select * from biuser.rpt_currency_rates"

def load(query):
  df = spark.read \
    .format("com.databricks.spark.sqldw") \
    .option("url", sql_dw_connection_string) \
    .option("tempdir", temp_dir_url) \
    .option("forward_spark_azure_storage_credentials", "true") \
    .option("query", query) \
    .load()
  return df

# COMMAND ----------

#loan_data = load("select * from biuser.rpt_predict_loan_data_corp")
avg_balance = load("select * from biuser.rpt_predict_avg_bal_sum_corp")
repymt = load("select * from biuser.rpt_predict_loan_repymt_corp")
repymt_nig = load("select * from biuser.RPT_PREDICT_LOAN_REPYMT_CORP_NIG")
transaction = load("select * from biuser.rpt_predict_txn_sum_corp")

# COMMAND ----------

#loan_data_df = loan_data.toPandas()
avg_balance_df = avg_balance.toPandas()
repymt_df = repymt.toPandas()
repymt_nig_df = repymt_nig.toPandas()
transaction_df = transaction.toPandas()

# COMMAND ----------

#transaction_df.head()

# COMMAND ----------

#repymt_nig_df.head()


# COMMAND ----------

#avg_balance_df.head()

# COMMAND ----------

import functools as ft
import pandas as pd

dfs = [avg_balance_df, transaction_df]
df_final = ft.reduce(lambda left, right: pd.merge(left, right, on=["COUNTRY","CUSTOMER_ID"]), dfs)

# COMMAND ----------

#df_final.head()

# COMMAND ----------

rfm_df = df_final.copy()

# COMMAND ----------

rfm_df["F6M_AVG_BAL"] = (rfm_df["BALANCE_01"]+rfm_df["BALANCE_02"]+rfm_df["BALANCE_03"]+rfm_df["BALANCE_04"]+rfm_df["BALANCE_05"]+rfm_df["BALANCE_06"])/6
rfm_df["L6M_AVG_BAL"] = (rfm_df["BALANCE_07"]+rfm_df["BALANCE_08"]+rfm_df["BALANCE_09"]+rfm_df["BALANCE_10"]+rfm_df["BALANCE_11"]+rfm_df["BALANCE_12"])/6
rfm_df["AVG_BAL_1YR"] = (rfm_df["BALANCE_01"]+rfm_df["BALANCE_02"]+rfm_df["BALANCE_03"]+rfm_df["BALANCE_04"]+rfm_df["BALANCE_05"]+rfm_df["BALANCE_06"]+
                      rfm_df["BALANCE_07"]+rfm_df["BALANCE_08"]+rfm_df["BALANCE_09"]+rfm_df["BALANCE_10"]+rfm_df["BALANCE_11"]+rfm_df["BALANCE_12"])/12


rfm_df["F6M_CR_TRAN_COUNT"] = (rfm_df["CR_TRAN_01"]+rfm_df["CR_TRAN_02"]+rfm_df["CR_TRAN_03"]+rfm_df["CR_TRAN_04"]+
                               rfm_df["CR_TRAN_05"]+rfm_df["CR_TRAN_06"])
rfm_df["L6M_CR_TRAN_COUNT"] = (rfm_df["CR_TRAN_07"]+rfm_df["CR_TRAN_08"]+rfm_df["CR_TRAN_09"]+rfm_df
                               ["CR_TRAN_10"]+rfm_df["CR_TRAN_11"]+rfm_df["CR_TRAN_12"])
rfm_df["1YR_CR_TRAN_COUNT"] = (rfm_df["CR_TRAN_01"]+rfm_df["CR_TRAN_02"]+rfm_df["CR_TRAN_03"]+rfm_df["CR_TRAN_04"]+rfm_df["CR_TRAN_05"]+
                               rfm_df["CR_TRAN_06"]+rfm_df["CR_TRAN_07"]+rfm_df["CR_TRAN_08"]+rfm_df["CR_TRAN_09"]+
                               rfm_df["CR_TRAN_10"]+rfm_df["CR_TRAN_11"]+rfm_df["CR_TRAN_12"])


rfm_df["F6M_DR_TRAN_COUNT"] = (rfm_df["DR_TRAN_01"]+rfm_df["DR_TRAN_02"]+rfm_df["DR_TRAN_03"]+rfm_df["DR_TRAN_04"]+
                               rfm_df["DR_TRAN_05"]+rfm_df["DR_TRAN_06"])
rfm_df["L6M_DR_TRAN_COUNT"] = (rfm_df["DR_TRAN_07"]+rfm_df["DR_TRAN_08"]+rfm_df["DR_TRAN_09"]+rfm_df
                               ["DR_TRAN_10"]+rfm_df["DR_TRAN_11"]+rfm_df["DR_TRAN_12"])
rfm_df["1YR_DR_TRAN_COUNT"] = (rfm_df["DR_TRAN_01"]+rfm_df["DR_TRAN_02"]+rfm_df["DR_TRAN_03"]+rfm_df["DR_TRAN_04"]+rfm_df["DR_TRAN_05"]+
                               rfm_df["DR_TRAN_06"]+rfm_df["DR_TRAN_07"]+rfm_df["DR_TRAN_08"]+rfm_df["DR_TRAN_09"]+
                               rfm_df["DR_TRAN_10"]+rfm_df["DR_TRAN_11"]+rfm_df["DR_TRAN_12"])


rfm_df["F6M_CR_AMT"] = (rfm_df["CR_AMT_01"]+rfm_df["CR_AMT_02"]+rfm_df["CR_AMT_03"]+rfm_df["CR_AMT_04"]+
                               rfm_df["CR_AMT_05"]+rfm_df["CR_AMT_06"])
rfm_df["L6M_CR_AMT"] = (rfm_df["CR_AMT_07"]+rfm_df["CR_AMT_08"]+rfm_df["CR_AMT_09"]+rfm_df
                               ["CR_AMT_10"]+rfm_df["CR_AMT_11"]+rfm_df["CR_AMT_12"])
rfm_df["1YR_CR_AMT"] = (rfm_df["CR_AMT_01"]+rfm_df["CR_AMT_02"]+rfm_df["CR_AMT_03"]+rfm_df["CR_AMT_04"]+rfm_df["CR_AMT_05"]+
                               rfm_df["CR_AMT_06"]+rfm_df["CR_AMT_07"]+rfm_df["CR_AMT_08"]+rfm_df["CR_AMT_09"]+
                               rfm_df["CR_AMT_10"]+rfm_df["CR_AMT_11"]+rfm_df["CR_AMT_12"])

rfm_df["F6M_DR_AMT"] = (rfm_df["DR_AMT_01"]+rfm_df["DR_AMT_02"]+rfm_df["DR_AMT_03"]+rfm_df["DR_AMT_04"]+
                               rfm_df["DR_AMT_05"]+rfm_df["DR_AMT_06"])
rfm_df["L6M_DR_AMT"] = (rfm_df["DR_AMT_07"]+rfm_df["DR_AMT_08"]+rfm_df["DR_AMT_09"]+rfm_df
                               ["DR_AMT_10"]+rfm_df["DR_AMT_11"]+rfm_df["DR_AMT_12"])
rfm_df["1YR_DR_AMT"] = (rfm_df["DR_AMT_01"]+rfm_df["DR_AMT_02"]+rfm_df["DR_AMT_03"]+rfm_df["DR_AMT_04"]+rfm_df["DR_AMT_05"]+
                               rfm_df["DR_AMT_06"]+rfm_df["DR_AMT_07"]+rfm_df["DR_AMT_08"]+rfm_df["DR_AMT_09"]+
                               rfm_df["DR_AMT_10"]+rfm_df["DR_AMT_11"]+rfm_df["DR_AMT_12"])

# COMMAND ----------

rfm_df = rfm_df.drop(columns=['BALANCE_01', 'BALANCE_02',
       'BALANCE_03', 'BALANCE_04', 'BALANCE_05', 'BALANCE_06', 'BALANCE_07',
       'BALANCE_08', 'BALANCE_09', 'BALANCE_10', 'BALANCE_11', 'BALANCE_12',
       'TXN_YEAR', 'CR_TRAN_01', 'CR_TRAN_02', 'CR_TRAN_03', 'CR_TRAN_04',
       'CR_TRAN_05', 'CR_TRAN_06', 'CR_TRAN_07', 'CR_TRAN_08', 'CR_TRAN_09',
       'CR_TRAN_10', 'CR_TRAN_11', 'CR_TRAN_12', 'DR_TRAN_01', 'DR_TRAN_02',
       'DR_TRAN_03', 'DR_TRAN_04', 'DR_TRAN_05', 'DR_TRAN_06', 'DR_TRAN_07',
       'DR_TRAN_08', 'DR_TRAN_09', 'DR_TRAN_10', 'DR_TRAN_11', 'DR_TRAN_12',
       'CR_AMT_01', 'CR_AMT_02', 'CR_AMT_03', 'CR_AMT_04', 'CR_AMT_05',
       'CR_AMT_06', 'CR_AMT_07', 'CR_AMT_08', 'CR_AMT_09', 'CR_AMT_10',
       'CR_AMT_11', 'CR_AMT_12', 'DR_AMT_01', 'DR_AMT_02', 'DR_AMT_03',
       'DR_AMT_04', 'DR_AMT_05', 'DR_AMT_06', 'DR_AMT_07', 'DR_AMT_08',
       'DR_AMT_09', 'DR_AMT_10', 'DR_AMT_11', 'DR_AMT_12'])

# COMMAND ----------

#rfm_df.head()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
sqlCtx = SparkSession.builder.getOrCreate()
rfm_df = sqlCtx.createDataFrame(rfm_df)

# COMMAND ----------

#Export Model Output to Database
rfm_df.write \
.format("com.databricks.spark.sqldw")\
.option("url", "jdbc:sqlserver://ubariskdbsvr.database.windows.net:1433;database=ubariskmldb")\
.option("user", user)\
.option("password",password)\
.option("tempDir",temp_dir_url)\
.option("forward_spark_azure_storage_credentials", "true")\
.option("dbtable", "rfm_corp")\
.mode("overwrite")\
.save()

# COMMAND ----------

repymt_df = repymt_df.append(repymt_nig_df)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
sqlCtx = SparkSession.builder.getOrCreate()
repymt_df = sqlCtx.createDataFrame(repymt_df)

# COMMAND ----------

#Export Model Output to Database
repymt_df.write \
.format("com.databricks.spark.sqldw")\
.option("url", "jdbc:sqlserver://ubariskdbsvr.database.windows.net:1433;database=ubariskmldb")\
.option("user", user)\
.option("password",password)\
.option("tempDir",temp_dir_url)\
.option("forward_spark_azure_storage_credentials", "true")\
.option("dbtable", "biuser_rpt_predict_repymt_combined_corp")\
.mode("overwrite")\
.save()

# COMMAND ----------

