# Databricks notebook source
# Enable storage as a temporary directory
storage_account_name = "xxxxxx"
storage_account_key = "xxxxxx"
storage_container_name = "dbtemp"
temp_dir_url =  "wasbs://xxxxxx.blob.core.windows.net/".format(storage_container_name, storage_account_name)
spark_config_key = "fs.azure.account.key.xxxxxx.blob.core.windows.net".format(storage_account_name)
spark_config_value = storage_account_key
spark.conf.set(spark_config_key, spark_config_value)

# COMMAND ----------

# connect to SQL datawarehouse
servername = "xxxxxx"
databasename = "xxxxxx"
password = "xxxxxx#"
user = "xxxxxx"
sql_dw_connection_string = "jdbc:sqlserver://ubariskdbsvr.database.windows.net:1433;database=xxxxxx;user=xxxxxx;password=xxxxxx;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;".format(servername, databasename, servername, password,user)

# COMMAND ----------

#querying data in SQL datawarehouse
query = "SELECT  * from stg.cs_data_corp"

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

df.count()

# COMMAND ----------

import pandas as pd
import numpy as np
pd.set_option('max_columns', None)
pd.set_option('max_rows', None)

import numpy as np
import seaborn as sns
import matplotlib.style as style
import matplotlib.pylab as plt
%matplotlib inline

# COMMAND ----------



# COMMAND ----------

pd_df = df.toPandas()

# COMMAND ----------

pd_df["CUSTOMER_ID"].nunique()

# COMMAND ----------

pd_df.head()

# COMMAND ----------

pd_df.info()

# COMMAND ----------

def find_missing(data):
    count_missing = data.isnull().sum().values
    total = data.shape[0]
    percent_missing = ((count_missing/total) * 100)
    return pd.DataFrame(data = {'missing_count':count_missing, 'percent_missing': percent_missing},
                       index = data.columns.values)

find_missing(pd_df)

# COMMAND ----------

# Fill categorical columns with a new category - missing.
for col in pd_df.select_dtypes('object').columns:
    pd_df[col].fillna('Missing', inplace=True)

# COMMAND ----------

# Fill numerical missing columns with 0
for col in pd_df.select_dtypes('float64').columns:
    pd_df[col].fillna(0, inplace=True)

# COMMAND ----------

find_missing(pd_df)

# COMMAND ----------

pd_df.head()

# COMMAND ----------

# DBTITLE 1,Loan Repayment
pd_df['prompt_Repayment_Rate_Score'] = np.select(
    [
        pd_df['prompt_Repayment_Rate'].between(.91, 1, inclusive=True), 
        pd_df['prompt_Repayment_Rate'].between(.71, .90, inclusive=True),
        pd_df['prompt_Repayment_Rate'].between(.51, .70, inclusive=True),
        pd_df['prompt_Repayment_Rate'].between(.31, .50, inclusive=True),
        pd_df['prompt_Repayment_Rate'].between(.11, .30, inclusive=True)
    ], 
    [
        
        65,
        40,
        30,
        20,
        10,
        
      
    ], 
    default= 0
)


# COMMAND ----------

pd_df['Repayment_Probability_Score'] = np.select(
    [
        pd_df['Repayment_Probability'].between(.91, 1, inclusive=True), 
        pd_df['Repayment_Probability'].between(.81, .90, inclusive=True),
        pd_df['Repayment_Probability'].between(.71, .80, inclusive=True),
        pd_df['Repayment_Probability'].between(.61, .70, inclusive=True),
        pd_df['Repayment_Probability'].between(.51, .60, inclusive=True),
        pd_df['Repayment_Probability'].between(.41, .50, inclusive=True),
        pd_df['Repayment_Probability'].between(.31, .40, inclusive=True),
        pd_df['Repayment_Probability'].between(.21, .30, inclusive=True),
        pd_df['prompt_Repayment_Rate'].between(.11, .20, inclusive=True)
    ], 
    [
        
        315,
        280,
        250,
        200,
        150,
        100,
        75,
        50,
        25,
        
      
    ], 
    default= 0
)


# COMMAND ----------

# Fill numerical missing columns with 0
for col in pd_df.select_dtypes('float64').columns:
    pd_df[col].fillna(0, inplace=True)

# COMMAND ----------

#The Loan Repayment Score is 
pd_df['Loan_Repayment_Score'] = pd_df.prompt_Repayment_Rate_Score + pd_df.Repayment_Probability_Score

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Financial History
#Value of Inflow

pd_df['Inflow_Value_Score'] = np.select(
    [
        pd_df['Inflow_Value'].between(0, 1000, inclusive=True), 
        pd_df['Inflow_Value'].between(1001, 100000, inclusive=True),
        pd_df['Inflow_Value'].between(100001, 500000, inclusive=True),
        pd_df['Inflow_Value'].between(500001, 1000000, inclusive=True),
        pd_df['Inflow_Value'].between(1000001, 10000000, inclusive=True),
        pd_df['Inflow_Value'].between(10000001, 50000000, inclusive=True),
        pd_df['Inflow_Value'].between(50000001, 100000000, inclusive=True),
        pd_df['Inflow_Value'].between(100000001, 1000000000000, inclusive=True)
    ], 
    [
        
        0,
        3,
        7,
        10,
        12,
        15,
        18,
        35,
      
        
      
    ], 
    default= 0
)

# COMMAND ----------

#Value of Outflow
pd_df['OutFlow_Value_Score'] = np.select(
    [
        pd_df['OutFlow_Value'].between(0, 1000, inclusive=True), 
        pd_df['OutFlow_Value'].between(1001, 100000, inclusive=True),
        pd_df['OutFlow_Value'].between(100001, 500000, inclusive=True),
        pd_df['OutFlow_Value'].between(500001, 1000000, inclusive=True),
        pd_df['OutFlow_Value'].between(1000001, 10000000, inclusive=True),
        pd_df['OutFlow_Value'].between(10000001, 50000000, inclusive=True),
        pd_df['OutFlow_Value'].between(50000001, 100000000, inclusive=True),
        pd_df['OutFlow_Value'].between(100000001, 1000000000000, inclusive=True)
    ], 
    [
        
        0,
        3,
        7,
        10,
        12,
        15,
        18,
        35,
      
        
      
    ], 
    default= 0
)

# COMMAND ----------

#Volume of Inflows
pd_df['Inflow_Volume_Score'] = np.select(
    [
        pd_df['Inflow_Volume'].between(0, 0, inclusive=True), 
        pd_df['Inflow_Volume'].between(1, 12, inclusive=True),
        pd_df['Inflow_Volume'].between(13, 60, inclusive=True),
        pd_df['Inflow_Volume'].between(61, 120, inclusive=True),
        pd_df['Inflow_Volume'].between(121, 180, inclusive=True),
        pd_df['Inflow_Volume'].between(181, 250, inclusive=True),
        pd_df['Inflow_Volume'].between(251, 300, inclusive=True),
        pd_df['Inflow_Volume'].between(301, 600, inclusive=True),
        pd_df['Inflow_Volume'].between(601, 800, inclusive=True),
        pd_df['Inflow_Volume'].between(801, 1000, inclusive=True),
        pd_df['Inflow_Volume'].between(1001, 1000000000000000, inclusive=True)
    ], 
    [
        
        0,
        2,
        4,
        6,
        8,
        10,
        12,
        14,
        16,
        18,
        35,
      
      
    ], 
    default= 0
)


# COMMAND ----------

#Volume of Outflows
pd_df['Outflow_Volume_Score'] = np.select(
    [
        pd_df['Outflow_Volume'].between(0, 0, inclusive=True), 
        pd_df['Outflow_Volume'].between(1, 12, inclusive=True),
        pd_df['Outflow_Volume'].between(13, 60, inclusive=True),
        pd_df['Outflow_Volume'].between(61, 120, inclusive=True),
        pd_df['Outflow_Volume'].between(121, 180, inclusive=True),
        pd_df['Outflow_Volume'].between(181, 250, inclusive=True),
        pd_df['Outflow_Volume'].between(251, 300, inclusive=True),
        pd_df['Outflow_Volume'].between(301, 600, inclusive=True),
        pd_df['Outflow_Volume'].between(601, 800, inclusive=True),
        pd_df['Outflow_Volume'].between(801, 1000, inclusive=True),
        pd_df['Outflow_Volume'].between(1001, 1000000000000000, inclusive=True)
    ], 
    [
        
        0,
        2,
        4,
        6,
        8,
        10,
        12,
        14,
        16,
        18,
        35,    
    ], 
    default= 0
)


# COMMAND ----------

#Deposit Value 
min_mon = min(pd_df.Deposit_Value)
pd_df['Deposit_Value'] = round(pd_df.Deposit_Value,0)

pd_df['Deposit_Value_Score'] = np.select(
    [
        pd_df['Deposit_Value'].between(min_mon, 0, inclusive=True), 
        pd_df['Deposit_Value'].between(1, 5000, inclusive=True),
        pd_df['Deposit_Value'].between(5001, 50000, inclusive=True),
        pd_df['Deposit_Value'].between(50001, 100000, inclusive=True),
        pd_df['Deposit_Value'].between(100001, 500000, inclusive=True),
        pd_df['Deposit_Value'].between(500001, 1000000, inclusive=True),
        pd_df['Deposit_Value'].between(1000001, 5000000, inclusive=True),
        pd_df['Deposit_Value'].between(5000001, 50000000, inclusive=True)
    ], 
    [
        
        0,
       15,
       30,
       40,
       50,
       70,
       80,
       90,
       
    ], 
    default= 115
)

# COMMAND ----------

#Inflow Retention Rate
pd_df['Inflow_Retention_Rate_Score'] = np.select(
    [
        pd_df['Inflow_Retention_Rate'].between(.91, 1, inclusive=True), 
        pd_df['Inflow_Retention_Rate'].between(.71, .90, inclusive=True),
        pd_df['Inflow_Retention_Rate'].between(.51, .70, inclusive=True),
        pd_df['Inflow_Retention_Rate'].between(.31, .50, inclusive=True),
        pd_df['Inflow_Retention_Rate'].between(.11, .30, inclusive=True)
    ], 
    [
        
        65,
        40,
        30,
        20,
        10,
        
      
    ], 
    default= 0
)


# COMMAND ----------

#Inflow Retention Value 
min_mon = min(pd_df.Inflow_Retention_Value)
pd_df['Inflow_Retention_Value'] = round(pd_df.Inflow_Retention_Value,0)

pd_df['Inflow_Retention_Value_Score'] = np.select(
    [
        pd_df['Inflow_Retention_Value'].between(min_mon, 0, inclusive=True), 
        pd_df['Inflow_Retention_Value'].between(1, 5000, inclusive=True),
        pd_df['Inflow_Retention_Value'].between(5001, 50000, inclusive=True),
        pd_df['Inflow_Retention_Value'].between(50001, 100000, inclusive=True),
        pd_df['Inflow_Retention_Value'].between(100001, 500000, inclusive=True),
        pd_df['Inflow_Retention_Value'].between(500001, 1000000, inclusive=True),
        pd_df['Inflow_Retention_Value'].between(1000001, 5000000, inclusive=True),
        pd_df['Inflow_Retention_Value'].between(5000001, 500000000000000000, inclusive=True)
    ], 
    [
        
        0,
       10,
       15,
       20,
       30,
       40,
       45,
       65,
       
    ], 
    default= 0
)

# COMMAND ----------

#Average Inflow Value 
min_mon = min(pd_df.Avg_Inflow_value)
pd_df['Avg_Inflow_value'] = round(pd_df.Avg_Inflow_value,0)

pd_df['Avg_Inflow_value_Score'] = np.select(
    [
        pd_df['Avg_Inflow_value'].between(min_mon, 0, inclusive=True), 
        pd_df['Avg_Inflow_value'].between(1, 5000, inclusive=True),
        pd_df['Avg_Inflow_value'].between(5001, 50000, inclusive=True),
        pd_df['Avg_Inflow_value'].between(50001, 100000, inclusive=True),
        pd_df['Avg_Inflow_value'].between(100001, 500000, inclusive=True),
        pd_df['Avg_Inflow_value'].between(500001, 1000000, inclusive=True),
        pd_df['Avg_Inflow_value'].between(1000001, 5000000, inclusive=True),
        pd_df['Avg_Inflow_value'].between(5000001, 500000000000000000, inclusive=True)
    ], 
    [
        
        0,
       5,
       10,
       15,
       20,
       25,
       30,
       50,
       
    ], 
    default= 0
)

# COMMAND ----------

#Average Outflow Value 
min_mon = min(pd_df.Avg_Outflow_value)
pd_df['Avg_Outflow_value'] = round(pd_df.Avg_Outflow_value,0)

pd_df['Avg_Outflow_value_Score'] = np.select(
    [
        pd_df['Avg_Outflow_value'].between(min_mon, 0, inclusive=True), 
        pd_df['Avg_Outflow_value'].between(1, 5000, inclusive=True),
        pd_df['Avg_Outflow_value'].between(5001, 50000, inclusive=True),
        pd_df['Avg_Outflow_value'].between(50001, 100000, inclusive=True),
        pd_df['Avg_Outflow_value'].between(100001, 500000, inclusive=True),
        pd_df['Avg_Outflow_value'].between(500001, 1000000, inclusive=True),
        pd_df['Avg_Outflow_value'].between(1000001, 5000000, inclusive=True),
        pd_df['Avg_Outflow_value'].between(5000001, 500000000000000000, inclusive=True)
    ], 
    [
        
        0,
       5,
       10,
       15,
       20,
       25,
       30,
       50,
       
    ], 
    default= 0
)

# COMMAND ----------

# Fill numerical missing columns with 0
for col in pd_df.select_dtypes('float64').columns:
    pd_df[col].fillna(0, inplace=True)

# COMMAND ----------

#Create The Financial History Score
pd_df['Financial_History_Score'] = (pd_df.Inflow_Value_Score + pd_df.OutFlow_Value_Score + pd_df.Inflow_Volume_Score + pd_df.Outflow_Volume_Score + pd_df.Deposit_Value_Score + pd_df.Inflow_Retention_Rate_Score + pd_df.Inflow_Retention_Value_Score + pd_df.Avg_Inflow_value_Score + pd_df.Avg_Outflow_value_Score)

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Account Activity
#Customer Tenure 
pd_df.loc[pd_df['TENURE_BAND'] == '0 TO 6 Months', 'Tenure_Score'] = 5
pd_df.loc[pd_df['TENURE_BAND'] == '7 TO 12 MONTHS' , 'Tenure_Score'] = 10
pd_df.loc[pd_df['TENURE_BAND'] == '13 TO 36 MONTHS', 'Tenure_Score'] = 15
pd_df.loc[pd_df['TENURE_BAND'] == '37 TO 60 MONTHS' , 'Tenure_Score'] = 20
pd_df.loc[pd_df['TENURE_BAND'] == '61 TO 120 MONTHS' , 'Tenure_Score'] = 25
pd_df.loc[pd_df['TENURE_BAND'] == '120 TO 180 MONTHS', 'Tenure_Score'] = 28
pd_df.loc[pd_df['TENURE_BAND'] == 'ABOVE 180 MONTHS', 'Tenure_Score'] = 45
pd_df.loc[pd_df['TENURE_BAND'] == 'Undefined' , 'Tenure_Score'] = 0


# COMMAND ----------

#Customer Recency
pd_df.loc[pd_df['LAST_TRAN_DATE_BAND'] == '0 TO 30 Days', 'Customer_Recency_Score'] = 40
pd_df.loc[pd_df['LAST_TRAN_DATE_BAND'] == '31 TO 60 Days' , 'Customer_Recency_Score'] = 20
pd_df.loc[pd_df['LAST_TRAN_DATE_BAND'] == '61 TO 90 Days', 'Customer_Recency_Score'] = 15
pd_df.loc[pd_df['LAST_TRAN_DATE_BAND'] == '91 TO 180 Days' , 'Customer_Recency_Score'] = 10
pd_df.loc[pd_df['LAST_TRAN_DATE_BAND'] == '181 TO 365 Days' , 'Customer_Recency_Score'] = 5
pd_df.loc[pd_df['LAST_TRAN_DATE_BAND'] == 'Above 365 Days', 'Customer_Recency_Score'] = 0
pd_df.loc[pd_df['LAST_TRAN_DATE_BAND'] == 'Undefined', 'Customer_Recency_Score'] = 0


# COMMAND ----------

#Recent Inflow Count

pd_df['Recent_Inflow_Count_Score'] = np.select(
    [
        pd_df['Recent_Inflow_Count'].between(0, 0, inclusive=True), 
        pd_df['Recent_Inflow_Count'].between(1, 4, inclusive=True),
        pd_df['Recent_Inflow_Count'].between(5, 8, inclusive=True),
        pd_df['Recent_Inflow_Count'].between(9, 11, inclusive=True),
        pd_df['Recent_Inflow_Count'].between(12, 30, inclusive=True),
        pd_df['Recent_Inflow_Count'].between(31, 40, inclusive=True),
        pd_df['Recent_Inflow_Count'].between(41, 60, inclusive=True)
     
    ], 
    [
        
        0,
       5,
       8,
       10,
       12,
       15,
       18,
    
       
    ], 
    default= 35
)

# COMMAND ----------

# Fill numerical missing columns with 0
for col in pd_df.select_dtypes('float64').columns:
    pd_df[col].fillna(0, inplace=True)

# COMMAND ----------

#Compute Account Activity Score
pd_df['Account_Activity_Score'] = (pd_df.Recent_Inflow_Count_Score + pd_df.Customer_Recency_Score + pd_df.Tenure_Score)

# COMMAND ----------



# COMMAND ----------

# Fill numerical missing columns with 0
for col in pd_df.select_dtypes('float64').columns:
    pd_df[col].fillna(0, inplace=True)

# COMMAND ----------

# Segmentation Flag Score
pd_df.loc[pd_df['Segmentation_Flag'] ==  0 , 'Segmentation_Flag_Score'] = 13
pd_df.loc[pd_df['Segmentation_Flag'] == 1, 'Segmentation_Flag_Score'] = 5
pd_df.loc[pd_df['Segmentation_Flag'] == 2, 'Segmentation_Flag_Score'] = 10
pd_df.loc[pd_df['Segmentation_Flag'] == 3, 'Segmentation_Flag_Score'] = 7
pd_df.loc[pd_df['Segmentation_Flag'] == 4, 'Segmentation_Flag_Score'] = 5
pd_df.loc[pd_df['Segmentation_Flag'] == 5, 'Segmentation_Flag_Score'] = 9
pd_df.loc[pd_df['Segmentation_Flag'] == 6, 'Segmentation_Flag_Score'] = 5

# COMMAND ----------

# Fill numerical missing columns with 0
for col in pd_df.select_dtypes('float64').columns:
    pd_df[col].fillna(0, inplace=True)

# COMMAND ----------

find_missing(pd_df)

# COMMAND ----------

# DBTITLE 1,Credit Scores
pd_df['Credit_Score'] = (pd_df.Loan_Repayment_Score + pd_df.Financial_History_Score + pd_df.Account_Activity_Score + pd_df.Segmentation_Flag_Score)

# COMMAND ----------

out_columns = ['COUNTRY','CUSTOMER_ID','Loan_Repayment_Score', 'Financial_History_Score', 'Account_Activity_Score', 'Credit_Score']
final_df = pd_df[out_columns]
final_df.head()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
sqlCtx = SparkSession.builder.getOrCreate()
final_df = sqlCtx.createDataFrame(final_df)

# COMMAND ----------

#display(final_df)

# COMMAND ----------

#Export Model Output to Database
final_df.write \
.format("com.databricks.spark.sqldw")\
.option("url", "jdbc:sqlserver://xxxxxx.database.windows.net:1433;database=xxxxxx")\
.option("user", user)\
.option("password",password)\
.option("tempDir",temp_dir_url)\
.option("forward_spark_azure_storage_credentials", "true")\
.option("dbtable", "stg.uba_credit_risk_retail_CS_corp")\
.mode("overwrite")\
.save()

# COMMAND ----------

