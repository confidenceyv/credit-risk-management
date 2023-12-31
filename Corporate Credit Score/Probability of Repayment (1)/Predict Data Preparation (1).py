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
#query = "select * from stg.[all_loan_cust]"
query = "select * from stg.[all_loan_cust_corp]"
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

pd_df = df.toPandas()

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

import calendar
import warnings
warnings.filterwarnings("ignore")

import scipy.stats.stats as stats
import re
import traceback
import string
import itertools

from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import LabelEncoder

# COMMAND ----------

pd_df.shape

# COMMAND ----------

pd_df.size

# COMMAND ----------

pd_df.info()

# COMMAND ----------

pd_df.head()

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Handle Missing Data
# Replace "Null" fields with Python recognized NaN
pd_df = pd_df.replace('NA',np.nan)
pd_df = pd_df.replace('Null',np.nan)

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

find_missing(pd_df)

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Exploratory Data Analysis
pd_df.describe().transpose()

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Country
pd_df.COUNTRY.value_counts()
#Because of the Dominace of Nigeria, We create a Flag Called is Nigeria 
pd_df['country_is_nigeria'] = np.where(pd_df['COUNTRY'] == 'NG', 1,0)


# COMMAND ----------

# DBTITLE 1,Average Balance
print("Minimum Average Balance is {} and the Maximum Average Balance is {}".format((min(pd_df.AVG_BAL_1YR)), (max(pd_df.AVG_BAL_1YR))))

# COMMAND ----------

# To Treat the Outliers, We find the Log of The Avergae Balance 
#pd_df['AVG_BAL_1YR'] = np.log(pd_df['AVG_BAL_1YR'])
plt.figure(figsize=(10,5))
sns.distplot(pd_df['AVG_BAL_1YR'])

# COMMAND ----------

# DBTITLE 1,Days Since Last Transaction
print("Minimum Days Since Last Transaction is {} and Maximum Days Since Last Transaction is {}".format((min(pd_df.Day_Since_Last_Tran)), (max(pd_df.Day_Since_Last_Tran))))

# COMMAND ----------

plt.figure(figsize=(10,5))
sns.distplot(pd_df['Day_Since_Last_Tran'])

# COMMAND ----------

#The Above Shows that there is a big gap in Values, To Deal with this we create a band to handle the disparity 
pd_df['Day_Since_Last_Tran_band'] = np.where( pd_df['Day_Since_Last_Tran'].between(0, 30, inclusive=True), 1, np.where( pd_df['Day_Since_Last_Tran'].between(31, 90, inclusive=True), 2, np.where( pd_df['Day_Since_Last_Tran'].between(91, 180, inclusive=True), 3, np.where( pd_df['Day_Since_Last_Tran'].between(181, 365, inclusive=True), 4, 5 ))))

#Visualise the Output 
plt.figure(figsize=(10,5))
sns.countplot('Day_Since_Last_Tran_band', data= pd_df)

# COMMAND ----------

#Drop The Day_Since_Last_Tran Column
pd_df.drop('Day_Since_Last_Tran', axis = 1, inplace = True) 

# COMMAND ----------

# DBTITLE 1,Volume of Inflow
print("Minimum Credit Count is {} and the Maximum Credit Count Transaction is {}".format((min(pd_df['1YR_CR_TRAN_COUNT'])),max(pd_df['1YR_CR_TRAN_COUNT']) ))

# COMMAND ----------

plt.figure(figsize=(10,5))
sns.distplot(pd_df['1YR_CR_TRAN_COUNT'])

# COMMAND ----------

pd_df['Inflow_Volume_Bin'] = pd.cut(pd_df['1YR_CR_TRAN_COUNT'],
                            bins=[-1,0, 4, 6, 8, 10, (max(pd_df['1YR_CR_TRAN_COUNT']))],
                            labels=[1,2,3,4,5,6])

#Visualise the Output 
plt.figure(figsize=(10,5))
sns.countplot('Inflow_Volume_Bin', data= pd_df)

# COMMAND ----------

#Drop The Column
pd_df.drop('1YR_CR_TRAN_COUNT', axis = 1, inplace = True)

# COMMAND ----------

# DBTITLE 1,Outflow Volume
print("Minimum Debit Count is {} and the Maximum Debit Count Transaction is {}".format((min(pd_df['1YR_DR_TRAN_COUNT'])),max(pd_df['1YR_DR_TRAN_COUNT']) ))

# COMMAND ----------

plt.figure(figsize=(10,5))
sns.distplot(pd_df['1YR_DR_TRAN_COUNT'])

# COMMAND ----------

pd_df['Outflow_Volume_Bin'] = pd.cut(pd_df['1YR_DR_TRAN_COUNT'],
                            bins=[-1,0, 1, 3, 4, 5, 6, (max(pd_df['1YR_DR_TRAN_COUNT']))],
                            labels=[1,2,3,4,5,6,7])

#Visualise the Output 
plt.figure(figsize=(10,5))
sns.countplot('Outflow_Volume_Bin', data= pd_df)

# COMMAND ----------

#Drop The Column
pd_df.drop('1YR_DR_TRAN_COUNT', axis = 1, inplace = True)

# COMMAND ----------

# DBTITLE 1,Inflow Value
print("Minimum Credit Value is {} and the Maximum Credit Count Transaction is {}".format((min(pd_df['1YR_CR_AMT'])),max(pd_df['1YR_CR_AMT'])))

# COMMAND ----------

plt.figure(figsize=(10,5))
sns.distplot(pd_df['1YR_CR_AMT'])

# COMMAND ----------

#We need to Bind this Column to get the best result from it 
pd_df['1YR_CR_AMT'] = round(pd_df['1YR_CR_AMT'],0)
cut_labels = [1, 2, 3, 4,5,6,7,8,9]
cut_bins = [-1,0, 25000, 100000, 500000, 1000000,10000000,50000000,100000000,(max(pd_df['1YR_CR_AMT']))]
pd_df['Inflow_value_bins'] = pd.cut(pd_df['1YR_CR_AMT'], bins=cut_bins, labels = cut_labels)

#Visualise the Output 
plt.figure(figsize=(10,5))
sns.countplot('Inflow_value_bins', data= pd_df)

# COMMAND ----------

#drop the Inflow Value Column
pd_df.drop('1YR_CR_AMT', axis = 1, inplace = True)

# COMMAND ----------

# DBTITLE 1,Outflow Value
plt.figure(figsize=(10,5))
sns.distplot(pd_df['1YR_DR_AMT'])

# COMMAND ----------

#We need to Bind this Column to get the best result from it 
pd_df['1YR_DR_AMT'] = round(pd_df['1YR_DR_AMT'],0)
cut_labels = [1, 2, 3, 4,5,6,7,8,9]
cut_bins = [-1,0, 250, 1000, 5000, 10000,100000,150000,200000,(max(pd_df['1YR_DR_AMT']))]
pd_df['Outflow_value_bins'] = pd.cut(pd_df['1YR_DR_AMT'], bins=cut_bins, labels = cut_labels)

#Visualise the Output 
plt.figure(figsize=(10,5))
sns.countplot('Outflow_value_bins', data= pd_df)

# COMMAND ----------

pd_df.drop('1YR_DR_AMT', axis =1, inplace = True)

# COMMAND ----------

# DBTITLE 1,Tenure
pd_df.Tenure.value_counts()
plt.figure(figsize=(10,5))
sns.distplot(pd_df['Tenure'])

# COMMAND ----------

# DBTITLE 1,Currency
  pd_df.CURRENCY.value_counts()
  plt.figure(figsize=(20,6))
sns.countplot('CURRENCY', data= pd_df)

# COMMAND ----------

# DBTITLE 1,SOL_ID
pd_df.SOL_ID.value_counts()
pd_df.drop('SOL_ID', axis = 1, inplace = True)

# COMMAND ----------

# DBTITLE 1,Prod Code
pd_df.PROD_CODE.value_counts()
series = pd.value_counts(pd_df.PROD_CODE)
mask = (series/series.sum() * 100).lt(1.5)
pd_df['new_prod_code'] = np.where(pd_df['PROD_CODE'].isin(series[mask].index),'OTHERS',pd_df['PROD_CODE'])
pd_df.drop('PROD_CODE', axis = 1, inplace = True)

# COMMAND ----------

plt.figure(figsize=(15,5))
sns.countplot('new_prod_code', data= pd_df)

# COMMAND ----------

# DBTITLE 1,Delinquent Check
pd_df['DELINQUENT_CHK'] = pd_df['DELINQUENT_CHK'].astype('int') #Covert TO the Right Data Type
pd_df.DELINQUENT_CHK.value_counts()
plt.figure(figsize=(10,5))
sns.countplot('DELINQUENT_CHK', data= pd_df)

# COMMAND ----------

# DBTITLE 1,Interest Rate 
print("Minimum Interest Rate is {} and the Maximum Interest Rate is {}".format((min(pd_df.INTEREST_RATE_DR)), (max(pd_df.INTEREST_RATE_DR))))


# COMMAND ----------

plt.figure(figsize=(10,5))
sns.distplot(pd_df['INTEREST_RATE_DR'])

# COMMAND ----------

# DBTITLE 1,Loan Duration
plt.figure(figsize=(10,5))
sns.distplot(pd_df['LOAN_DURATION'])

# COMMAND ----------

pd_df['Loan_Duration_Cat'] = np.where( pd_df['LOAN_DURATION'].between(0, 12, inclusive=True), 'Short', np.where( pd_df['LOAN_DURATION'].between(13, 60, inclusive=True), 'Mid', 'Long' ))
#Visualise the Output 
plt.figure(figsize=(10,5))
sns.countplot('Loan_Duration_Cat', data= pd_df)

# COMMAND ----------

# DBTITLE 1,Principal Amount
print((min(pd_df.PRINCIPAL_AMOUNT)))
print((max(pd_df.PRINCIPAL_AMOUNT)))

# COMMAND ----------

plt.figure(figsize=(15,8))
sns.distplot(pd_df['PRINCIPAL_AMOUNT'])

# COMMAND ----------

plt.figure(figsize = (12, 9))
plt.scatter(pd_df['INTEREST_RATE_DR'], pd_df['PRINCIPAL_AMOUNT'])
plt.xlabel('Interest Rate')
plt.ylabel('Principal Amount')
plt.title('Visualization of Loan Pricipal Amount')

# COMMAND ----------

plt.figure(figsize = (12, 9))
plt.scatter(pd_df['INTEREST_RATE_DR'], pd_df['LOAN_DURATION'])
plt.xlabel('Interest Rate')
plt.ylabel('Principal Amount')
plt.title('Visualization of Loan Duration and Interest Rate')

# COMMAND ----------

# DBTITLE 1,No of Past Loans
plt.figure(figsize=(8,5))
sns.distplot(pd_df['NO_OF_PAST_LOANS'])


# COMMAND ----------

# DBTITLE 1,Previous Loan Defaults
plt.figure(figsize=(8,5))
sns.distplot(pd_df['NO_OF_PREVIOUS_LOAN_DEFAULTS'])


# COMMAND ----------

# DBTITLE 1,Previous Loan Settled
plt.figure(figsize=(8,5))
sns.distplot(pd_df['NO_OF_PREVIOUS_LOAN_SETTLED'])

# COMMAND ----------

#The above Variables do not make Sense, so we drop them
pd_df.drop(['NO_OF_PREVIOUS_LOAN_DEFAULTS', 'NO_OF_PREVIOUS_LOAN_SETTLED'], axis = 1, inplace = True)

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Prompt Paid Repayment
plt.figure(figsize=(8,5))
sns.distplot(pd_df['Paid_Repayment_count'])


# COMMAND ----------

# DBTITLE 1,Default Repayment Count
plt.figure(figsize=(8,5))
sns.distplot(pd_df['Default_Repayment_count'])

# COMMAND ----------

# DBTITLE 1,Repayment Count
plt.figure(figsize=(8,5))
sns.distplot(pd_df['Repayment_Count'])


# COMMAND ----------

# DBTITLE 1,Paid Repayment Amount
plt.figure(figsize=(8,5))
sns.distplot(pd_df['Paid_Repayment_Amount'])


# COMMAND ----------

# DBTITLE 1,Default Repayment Amount
plt.figure(figsize=(8,5))
sns.distplot(pd_df['Default_Repayment_Amount'])


# COMMAND ----------

# DBTITLE 1,Repayment Amount
plt.figure(figsize=(8,5))
sns.distplot(pd_df['Repayment_Amount'])


# COMMAND ----------

pd_df.head()

# COMMAND ----------

#convert dpd_status to 1 and O
pd_df['DPD_Status_30Days'] = np.where(pd_df['DPD_Status_30Days'] == 'Default', 1,0)
pd_df['DPD_Status_60Days'] = np.where(pd_df['DPD_Status_60Days'] == 'Default', 1,0)
pd_df['DPD_Status_90Days'] = np.where(pd_df['DPD_Status_90Days'] == 'Default', 1,0)
pd_df['Status_At_maturity'] = np.where(pd_df['Status_At_maturity'] == 'Default', 1,0)

# COMMAND ----------

# DBTITLE 1,Others
#Drop Not Revelevant Columns
pd_df.drop(['F6M_DR_AMT','L6M_DR_AMT' ], axis = 1 , inplace = True)
# Fill numerical missing columns with 0
for col in pd_df.select_dtypes('float64').columns:
    pd_df[col].fillna(0, inplace=True)


# COMMAND ----------

find_missing(pd_df)

# COMMAND ----------

# DBTITLE 1,Data Preparation 
pd_df.info()

# COMMAND ----------

#correct columns to right datatype
a_cols = ['Inflow_Volume_Bin','Outflow_Volume_Bin', 'Inflow_value_bins' , 'Outflow_value_bins']
for a in a_cols:
  pd_df[a] = pd_df[a].astype('int')
  

# COMMAND ----------

list(pd_df.select_dtypes('object'))

# COMMAND ----------

#Encode Categorical Variables

le = LabelEncoder()

leFeatures = ['CURRENCY', 'new_prod_code', 'Loan_Duration_Cat']

for col in leFeatures:
  pd_df[col] = le.fit_transform(pd_df[col])

# COMMAND ----------

# Initialize a scaler, then apply it to the features
scaler = MinMaxScaler() # default=(0, 1)

for col in pd_df.select_dtypes('float64'):
    pd_df[col]= scaler.fit_transform(pd_df[col].values.reshape(-1,1))

# COMMAND ----------

corr = pd_df.corr()
# Generate a mask for the upper triangle
mask = np.zeros_like(corr, dtype=np.bool)
mask[np.triu_indices_from(mask)] = True

# Set up the matplotlib figure
f, ax = plt.subplots(figsize=(15, 12))

# Generate a custom diverging colormap
cmap = sns.diverging_palette(220, 10, as_cmap=True)

# Draw the heatmap with the mask and correct aspect ratio
sns.heatmap(corr, mask=mask, cmap=cmap, vmax=.3, center=0,
            square=True, linewidths=.5, cbar_kws={"shrink": .5})

# COMMAND ----------

# import ppscore as pps

# matrix = pps.matrix(pd_df)

# def heatmap(df):
#     df = df[['x', 'y', 'ppscore']].pivot(columns='x', index='y', values='ppscore')
#     plt.figure(figsize=(50,20))
#     ax = sns.heatmap(df, vmin=0, vmax=1, cmap="Blues", linewidths=0.5, annot=True)
#     ax.set_title("PPS matrix")
#     ax.set_xlabel("feature")
#     ax.set_ylabel("target")
#     return ax

# heatmap(matrix)

# COMMAND ----------

pd_df.head()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
sqlCtx = SQLContext(sc)
final_df = sqlCtx.createDataFrame(pd_df)

# COMMAND ----------

#Export Model Output to Database
final_df.write \
.format("com.databricks.spark.sqldw")\
.option("url", "jdbc:sqlserver://ubariskdbsvr.database.windows.net:1433;database=ubariskmldb")\
.option("user", user)\
.option("password",password)\
.option("tempDir",temp_dir_url)\
.option("forward_spark_azure_storage_credentials", "true")\
.option("dbtable", "stg.LD_proc_all_loans_cust_corp")\
.mode("overwrite")\
.save()

# COMMAND ----------

