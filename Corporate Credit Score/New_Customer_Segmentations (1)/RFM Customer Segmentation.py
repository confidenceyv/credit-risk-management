# Databricks notebook source
#start a spark session
from pyspark.sql import SparkSession, SQLContext
spark = SparkSession.builder.appName('cluster').getOrCreate()

# COMMAND ----------

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
query = "SELECT  [country] Country ,[customer_id] Customer_id,([one_year_cr_tran] + [one_year_dr_tran] ) Frequency \
      ,[recency] Recency_in_days ,[one_year_average_balance] Monetary \
  FROM [stg].[combine_customer_tfrm_data_corp]"

df = spark.read \
  .format("com.databricks.spark.sqldw") \
  .option("url", sql_dw_connection_string) \
  .option("tempdir", temp_dir_url) \
  .option("forward_spark_azure_storage_credentials", "true") \
  .option("query", query) \
  .load()

# COMMAND ----------

display(df, 5)

# COMMAND ----------

df.count()

# COMMAND ----------

df = df.toPandas()

# COMMAND ----------

df.shape

# COMMAND ----------

import pandas as pd
import numpy as np

import calendar
import warnings
warnings.filterwarnings("ignore")

# COMMAND ----------

import numpy as np
df['Recency_Band'] = np.select(
    [
        df['Recency_in_days'].between(0, 30, inclusive=True), 
        df['Recency_in_days'].between(31, 90, inclusive=True),
        df['Recency_in_days'].between(91, 180, inclusive=True),
        df['Recency_in_days'].between(180, 365, inclusive=True),
        df['Recency_in_days'].between(366, 730, inclusive=True)
    ], 
    [
        'Between 0 and 30 Days',
        'Between 31 and 90 Days',
        'Between 91 and 180 Days',
        'Between 181 and 365 Days',
        'Between 366 and 730 Days',
    ], 
    default='Above 730 Days'
)

# COMMAND ----------

df[df["Recency_in_days"]>1]

# COMMAND ----------

df.loc[df['Recency_Band'] == 'Between 0 and 30 Days' , 'R_Score'] = 5
df.loc[df['Recency_Band'] == 'Between 31 and 90 Days' , 'R_Score'] = 4
df.loc[df['Recency_Band'] == 'Between 91 and 180 Days' , 'R_Score'] = 3
df.loc[df['Recency_Band'] == 'Between 181 and 365 Days' , 'R_Score'] = 2
df.loc[df['Recency_Band'] == 'Between 366 and 730 Days' , 'R_Score'] = 1
df.loc[df['Recency_Band'] == 'Above 730 Days' , 'R_Score'] = 0


# COMMAND ----------

print('max Frequency is {} and min Frequency is {} and the average Frequency is {}'.format((max(df.Frequency)), (min(df.Frequency)), (np.mean(df.Frequency)))) 

# COMMAND ----------

import numpy as np
df['Frequency_Band'] = np.select(
    [
        df['Frequency'].between(0, 0, inclusive=True),
        df['Frequency'].between(1, 12, inclusive=True), 
        df['Frequency'].between(13, 60, inclusive=True),
        df['Frequency'].between(61, 120, inclusive=True),
        df['Frequency'].between(121, 180, inclusive=True),
        df['Frequency'].between(181, 288, inclusive=True),
        df['Frequency'].between(289, 600, inclusive=True),
        df['Frequency'].between(600, 1200, inclusive=True)
        
    ], 
    [
        
        'Zero Transaction',
        'Between 1 and 12 Transactions',
        'Between 13 and 60 Transactions',
        'Between 61 and 120 Transactions',
        'Between 121 and 180 Transactions',
        'Between 181 and 288 Transactions',
        'Between 289 and 600 Transactions',
        'Between 601 and 1200 Transactions',
        
      
    ], 
    default='Above 1200 Transactions'
)

# COMMAND ----------

df.loc[df['Frequency_Band'] == 'Zero Transaction' , 'F_Score'] = 0
df.loc[df['Frequency_Band'] == 'Between 1 and 12 Transactions' , 'F_Score'] = 1
df.loc[df['Frequency_Band'] == 'Between 13 and 60 Transactions' , 'F_Score'] = 2
df.loc[df['Frequency_Band'] == 'Between 61 and 120 Transactions' , 'F_Score'] = 3
df.loc[df['Frequency_Band'] == 'Between 121 and 180 Transactions' , 'F_Score'] = 4
df.loc[df['Frequency_Band'] == 'Between 181 and 288 Transactions' , 'F_Score'] = 5
df.loc[df['Frequency_Band'] == 'Between 289 and 600 Transactions' , 'F_Score'] = 6
df.loc[df['Frequency_Band'] == 'Between 601 and 1200 Transactions' , 'F_Score'] = 7
df.loc[df['Frequency_Band'] == 'Above 1200 Transactions' , 'F_Score'] = 8

# COMMAND ----------

print('max Monetary is {} and min Monetary is {} and the average Monetary is {}'.format((max(df.Monetary)), (min(df.Monetary)), (np.mean(df.Monetary)))) 

# COMMAND ----------

df['Monetary'] = round(df.Monetary,0)
min_mon = min(df.Monetary)
max_mon = max(df.Monetary)

# COMMAND ----------

import numpy as np
df['Monetary_Band'] = np.select(
    [
        df['Monetary'].between(min_mon, 0, inclusive=True), 
        df['Monetary'].between(1, 4999, inclusive=True),
        df['Monetary'].between(5000, 49999, inclusive=True),
        df['Monetary'].between(50000, 100000, inclusive=True),
        df['Monetary'].between(100001, 499999, inclusive=True),
        df['Monetary'].between(500000, 999999, inclusive=True),
        df['Monetary'].between(1000000, 5000000, inclusive=True),
        df['Monetary'].between(5000001, max_mon, inclusive=True)
    ], 
    [
        
        'Negative Balance',
        'Balance between 0 and 4999',
        'Between 5K and 50K',
        'Balance Between 50K and 100K',
        'Balance Between 100K and 500K',
        'Balance Between 500K and 1M',
        'Balance Between 1M and 5M',
        'Balance Above 5M',
      
    ], 
    default='Unclassified'
)


# COMMAND ----------

df.loc[df['Monetary_Band'] == 'Negative Balance' , 'M_Score'] = 0
df.loc[df['Monetary_Band'] == 'Balance between 0 and 4999' , 'M_Score'] = 1
df.loc[df['Monetary_Band'] == 'Between 5K and 50K' , 'M_Score'] = 2
df.loc[df['Monetary_Band'] == 'Balance Between 50K and 100K' , 'M_Score'] = 3
df.loc[df['Monetary_Band'] == 'Balance Between 100K and 500K' , 'M_Score'] = 4
df.loc[df['Monetary_Band'] == 'Balance Between 500K and 1M' , 'M_Score'] = 5
df.loc[df['Monetary_Band'] == 'Balance Between 1M and 5M' , 'M_Score'] = 6
df.loc[df['Monetary_Band'] == 'Balance Above 5M' , 'M_Score'] = 7

# COMMAND ----------

df.head(2)

# COMMAND ----------

df.head()

# COMMAND ----------

df['RFM_Score'] = df.R_Score + df.F_Score + df.M_Score


# COMMAND ----------

df['tier'] = np.select(
    [
        df['RFM_Score'].between(0, 1, inclusive=True),   
        df['RFM_Score'].between(2, 4, inclusive=True), 
        df['RFM_Score'].between(5, 8, inclusive=True),
        df['RFM_Score'].between(9, 12, inclusive=True),
        df['RFM_Score'].between(13, 16, inclusive=True),
        df['RFM_Score'].between(17, 20, inclusive=True),
       
        
    ], 
    [
        'Inactive',
        'Bronze',
        'Silver',
        'Gold',
        'Diamond',
        'Platinum',
        
      
    ], 
    default='Unclassified'
)


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
.option("dbtable", "stg.RFM_customer_segments")\
.mode("overwrite")\
.save()

# COMMAND ----------

