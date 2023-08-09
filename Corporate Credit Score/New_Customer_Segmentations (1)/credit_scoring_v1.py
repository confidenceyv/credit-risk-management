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
query = "SELECT  * from [stg].[vw_seg_rfm_datase_view]"

df = spark.read \
  .format("com.databricks.spark.sqldw") \
  .option("url", sql_dw_connection_string) \
  .option("tempdir", temp_dir_url) \
  .option("forward_spark_azure_storage_credentials", "true") \
  .option("query", query) \
  .load()

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.toPandas()

# COMMAND ----------

df.shape

# COMMAND ----------

df.info()

# COMMAND ----------

df.describe()

# COMMAND ----------

import pandas as pd
import numpy as np

# COMMAND ----------

df['AVG_BAL_1YR'].mean()

# COMMAND ----------

df['AVG_BAL_1YR'].max()

# COMMAND ----------

def grade_a1(data1):
  if data1['AVG_BAL_1YR'] >= 1000000000:
    return 850
  elif (data1['AVG_BAL_1YR'] >= 500000000 and data1['AVG_BAL_1YR'] < 1000000000):
    return 750
  elif (data1['AVG_BAL_1YR'] >= 100000000 and data1['AVG_BAL_1YR'] < 500000000):
    return 650
  elif (data1['AVG_BAL_1YR'] >= 50000000 and data1['AVG_BAL_1YR'] < 100000000):
    return 550
  elif (data1['AVG_BAL_1YR'] >= 10000000 and data1['AVG_BAL_1YR'] < 50000000):
    return 450
  elif (data1['AVG_BAL_1YR'] >= 1000000 and data1['AVG_BAL_1YR'] < 10000000):
    return 350
  elif (data1['AVG_BAL_1YR'] >= 500000 and data1['AVG_BAL_1YR'] < 1000000):
    return 300
  else:
    return 250
df['balance_score'] = df.apply(grade_a1, axis=1)

# COMMAND ----------

df['L6M_CR_TRAN_COUNT'].max()

# COMMAND ----------

def grade_a2(data2):
  if data2['L6M_CR_TRAN_COUNT'] >= 20242:
    return 850
  elif (data2['L6M_CR_TRAN_COUNT'] >= 15000 and data2['L6M_CR_TRAN_COUNT'] < 20242):
    return 750
  elif (data2['L6M_CR_TRAN_COUNT'] >= 10000 and data2['L6M_CR_TRAN_COUNT'] < 15000):
    return 650
  elif (data2['L6M_CR_TRAN_COUNT'] >= 5000 and data2['L6M_CR_TRAN_COUNT'] < 10000):
    return 550
  elif (data2['L6M_CR_TRAN_COUNT'] >= 1000 and data2['L6M_CR_TRAN_COUNT'] < 5000):
    return 450
  elif (data2['L6M_CR_TRAN_COUNT'] >= 500 and data2['L6M_CR_TRAN_COUNT'] < 5000):
    return 350
  else:
    return 250
df['credit_score'] = df.apply(grade_a2, axis=1)

# COMMAND ----------

df['HAS_SAVINGS'].replace(0, 'None', inplace=True)
df['HAS_SAVINGS'].replace(1, 'Single', inplace=True)

df['HAS_LOAN'].replace(0, 'None', inplace=True)
df['HAS_LOAN'].replace(1, 'Single', inplace=True)

df['HAS_OD'].replace(0, 'None', inplace=True)
df['HAS_OD'].replace(1, 'Single', inplace=True)

df['HAS_CURRENT'].replace(0, 'None', inplace=True)
df['HAS_CURRENT'].replace(1, 'Single', inplace=True)

df['HASDEBIT'].replace(0, 'None', inplace=True)
df['HASDEBIT'].replace(0, 'Single', inplace=True)

# COMMAND ----------

df.HAS_SAVINGS[df.HAS_SAVINGS == 'Single'] = 1
df.HAS_SAVINGS[df.HAS_SAVINGS == 'None'] = 0
df.HAS_SAVINGS[df.HAS_SAVINGS == 'Multiple'] = 3

df.HAS_LOAN[df.HAS_LOAN == 'Single'] = 1
df.HAS_LOAN[df.HAS_LOAN == 'None'] = 0
df.HAS_LOAN[df.HAS_LOAN == 'Multiple'] = 3

df.HAS_OD[df.HAS_OD == 'Single'] = 1
df.HAS_OD[df.HAS_OD == 'None'] = 0
df.HAS_OD[df.HAS_OD == 'Multiple'] = 3

df.HAS_CURRENT[df.HAS_CURRENT == 'Single'] = 1
df.HAS_CURRENT[df.HAS_CURRENT == 'None'] = 0
df.HAS_CURRENT[df.HAS_CURRENT == 'Multiple'] = 3

df.HASDEBIT[df.HASDEBIT == 'Single'] = 1
df.HASDEBIT[df.HASDEBIT == 'None'] = 0

# COMMAND ----------

df["HAS_LOAN"]=pd.to_numeric(df["HAS_LOAN"])
df["HAS_OD"]=pd.to_numeric(df["HAS_OD"])
df["HAS_CURRENT"]=pd.to_numeric(df["HAS_CURRENT"])
df["HASDEBIT"]=pd.to_numeric(df["HASDEBIT"])

# COMMAND ----------

import datetime
df['MAX_TRAN_DATE'] = pd.to_datetime(df['MAX_TRAN_DATE'])

data_date = datetime.datetime(2020,12,31)
data_date

# COMMAND ----------

df['recency'] = (data_date - df['MAX_TRAN_DATE']).dt.days

# COMMAND ----------

df.head(2)

# COMMAND ----------

df.dtypes

# COMMAND ----------

def grade_a3(data3):
  if data3['recency'] >= 730:
    return 250
  elif (data3['recency'] >= 570 and data3['recency'] < 730):
    return 450
  elif (data3['recency'] >= 370 and data3['recency'] < 570):
    return 550
  elif (data3['recency'] >= 170 and data3['recency'] < 370):
    return 650
  elif (data3['recency'] >= 90 and data3['recency'] < 170):
    return 700
  elif (data3['recency'] >= 60 and data3['recency'] < 90):
    return 750
  else:
    return 850
df['recency_score'] = df.apply(grade_a3, axis=1)

# COMMAND ----------

def grade_a4(data4):
  if data4['HAS_LOAN'] == 1:
    return 550
  elif data4['HAS_LOAN'] == 3:
    return 850
  else:
    return 250
df['loan_score'] = df.apply(grade_a4, axis=1)

# COMMAND ----------

def grade_a5(data5):
  if data5['HAS_SAVINGS'] == 1:
    return 550
  elif data5['HAS_SAVINGS'] == 3:
    return 850
  else:
    return 250
df['savings_score'] = df.apply(grade_a5, axis=1)

# COMMAND ----------

def grade_a6(data6):
  if data6['HAS_OD'] == 1:
    return 550
  elif data6['HAS_OD'] == 3:
    return 850
  else:
    return 250
df['OD_score'] = df.apply(grade_a6, axis=1)

# COMMAND ----------

def grade_a7(data7):
  if data7['HAS_CURRENT'] == 1:
    return 550
  elif data7['HAS_CURRENT'] == 3:
    return 850
  else:
    return 250
df['HAS_CURRENT'] = df.apply(grade_a7, axis=1)

# COMMAND ----------

df.head()

# COMMAND ----------

df['final_score']= df['credit_score'] = ((df['balance_score'] + df['recency_score'] + df['credit_score'] * 0.35) + (df['loan_score'] + df['savings_score'] + df['OD_score'] * 0.3) + (df['recency_score'] * 0.15) + (df['loan_score'] + df['savings_score'] + df['OD_score'] + df['HAS_CURRENT'] * 0.1) + (df['recency_score'] * 0.1))/ 100

# COMMAND ----------

df.head(20)

# COMMAND ----------

#convert the dataset back to Pyspark and Export it back to the database
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
sqlCtx = SQLContext(sc)
df = sqlCtx.createDataFrame(df)

# COMMAND ----------

#Writing the result of the Data Preparation to the database
df.write \
.format("com.databricks.spark.sqldw")\
.option("url", "jdbc:sqlserver://ubariskdbsvr.database.windows.net:1433;database=ubariskmldb")\
.option("user", "biadmin02@ubariskdbsvr")\
.option("password", "sqlAdmin123#")\
.option("tempDir", "wasbs://staging@ubariskdl.blob.core.windows.net/")\
.option("forward_spark_azure_storage_credentials", "true")\
.option("dbtable", "stg.customer_credit_scoring")\
.mode("overwrite")\
.save()

# COMMAND ----------

