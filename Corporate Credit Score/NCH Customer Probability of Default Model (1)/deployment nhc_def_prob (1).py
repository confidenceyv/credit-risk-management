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
query = "select * from [stg].[nhc_predict_dataset]"

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


from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler,OneHotEncoder
from sklearn.feature_selection import SelectFromModel
from BorutaShap import BorutaShap
#from imblearn.over_sampling import SMOTE, SMOTENC
from sklearn.metrics import f1_score,accuracy_score

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

# DBTITLE 1,Country
plt.figure(figsize=(10,5))
sns.countplot('COUNTRY', data= pd_df)

# COMMAND ----------

#Because of the Dominace of Nigeria, We create a Flag Called is Nigeria 
pd_df['country_is_nigeria'] = np.where(pd_df['COUNTRY'] == 'NG', 1,0)
plt.figure(figsize=(10,5))
sns.countplot('country_is_nigeria', data= pd_df)

# COMMAND ----------

# DBTITLE 1,Days Since Last Tran
#print("Minimum Days Since Last Transaction is {} and Maximum Days Since Last Transaction is {}".format((min(pd_df.Day_Since_Last_Tran)), (max(pd_df.Day_Since_Last_Tran))))
plt.figure(figsize=(10,5))
sns.distplot(pd_df['Day_Since_Last_Tran'])

#The Above Shows that there is a big gap in Values, To Deal with this we create a band to handle the disparity 
pd_df['Day_Since_Last_Tran_band'] = np.where( pd_df['Day_Since_Last_Tran'].between(0, 30, inclusive=True), 1, np.where( pd_df['Day_Since_Last_Tran'].between(31, 90, inclusive=True), 2, np.where( pd_df['Day_Since_Last_Tran'].between(91, 180, inclusive=True), 3, np.where( pd_df['Day_Since_Last_Tran'].between(181, 365, inclusive=True), 4, 5 ))))

#Visualise the Output 
plt.figure(figsize=(10,5))
sns.countplot('Day_Since_Last_Tran_band', data= pd_df)

# COMMAND ----------

# DBTITLE 1,1YR_CR_TRAN_COUNT
#print("Minimum Credit Count is {} and the Maximum Credit Count Transaction is {}".format((min(pd_df['1YR_CR_TRAN_COUNT'])),max(pd_df['1YR_CR_TRAN_COUNT']) ))
plt.figure(figsize=(10,5))
sns.distplot(pd_df['1YR_CR_TRAN_COUNT'])

pd_df['1YR_CR_TRAN_COUNT_BIN'] = pd.cut(pd_df['1YR_CR_TRAN_COUNT'],
                            bins=[-1,0, 12, 60, 120, 250, (max(pd_df['1YR_CR_TRAN_COUNT']))],
                            labels=[1,2,3,4,5,6])

#Visualise the Output 
plt.figure(figsize=(10,5))
sns.countplot('1YR_CR_TRAN_COUNT_BIN', data= pd_df)

# COMMAND ----------

# DBTITLE 1,1YR_DR_TRAN_COUNT	
#print("Minimum Credit Count is {} and the Maximum Credit Count Transaction is {}".format((min(pd_df['1YR_DR_TRAN_COUNT'])),max(pd_df['1YR_DR_TRAN_COUNT']) ))
plt.figure(figsize=(10,5))
sns.distplot(pd_df['1YR_DR_TRAN_COUNT'])

pd_df['1YR_DR_TRAN_COUNT_BIN'] = pd.cut(pd_df['1YR_DR_TRAN_COUNT'],
                            bins=[-1,0, 12, 60, 120, 250, (max(pd_df['1YR_DR_TRAN_COUNT']))],
                            labels=[1,2,3,4,5,6])

#Visualise the Output 
plt.figure(figsize=(10,5))
sns.countplot('1YR_DR_TRAN_COUNT_BIN', data= pd_df)

# COMMAND ----------

# DBTITLE 1,1YR_CR_AMT
#print("Minimum Credit Value is {} and the Maximum Credit Count Transaction is {}".format((min(pd_df['1YR_CR_AMT'])),max(pd_df['1YR_CR_AMT'])))

plt.figure(figsize=(10,5))
sns.distplot(pd_df['1YR_CR_AMT'])

#We need to Bind this Column to get the best result from it 
pd_df['1YR_CR_AMT'] = round(pd_df['1YR_CR_AMT'],0)
cut_labels = [1, 2, 3, 4,5,6,7,8,9]
cut_bins = [-1,0, 25000, 100000, 500000, 1000000,10000000,50000000,100000000,(max(pd_df['1YR_CR_AMT']))]
pd_df['1YR_CR_AMT_BIN'] = pd.cut(pd_df['1YR_CR_AMT'], bins=cut_bins, labels = cut_labels)

#Visualise the Output 
plt.figure(figsize=(10,5))
sns.countplot('1YR_CR_AMT_BIN', data= pd_df)

# COMMAND ----------

# DBTITLE 1,F6M_DR_AMT
#print("Minimum Credit Value is {} and the Maximum Credit Count Transaction is {}".format((min(pd_df['F6M_DR_AMT'])),max(pd_df['F6M_DR_AMT'])))

plt.figure(figsize=(10,5))
sns.distplot(pd_df['F6M_DR_AMT'])

#We need to Bind this Column to get the best result from it 
pd_df['F6M_DR_AMT'] = round(pd_df['F6M_DR_AMT'],0)
cut_labels = [1, 2, 3, 4,5,6,7,8]
cut_bins = [-1,0, 25000, 100000, 500000, 1000000,10000000,20000000,(max(pd_df['F6M_DR_AMT']))]
pd_df['F6M_DR_AMT_BIN'] = pd.cut(pd_df['F6M_DR_AMT'], bins=cut_bins, labels = cut_labels)

#Visualise the Output 
plt.figure(figsize=(10,5))
sns.countplot('F6M_DR_AMT_BIN', data= pd_df)

# COMMAND ----------

# DBTITLE 1,L6M_DR_AMT
#print("Minimum Credit Value is {} and the Maximum Credit Count Transaction is {}".format((min(pd_df['F6M_DR_AMT'])),max(pd_df['F6M_DR_AMT'])))

plt.figure(figsize=(10,5))
sns.distplot(pd_df['L6M_DR_AMT'])

#We need to Bind this Column to get the best result from it 
pd_df['F6M_DR_AMT'] = round(pd_df['L6M_DR_AMT'],0)
cut_labels = [1, 2, 3, 4,5,6,7,8]
cut_bins = [-1,0, 25000, 100000, 500000, 1000000,10000000,20000000,(max(pd_df['L6M_DR_AMT']))]
pd_df['L6M_DR_AMT_BIN'] = pd.cut(pd_df['L6M_DR_AMT'], bins=cut_bins, labels = cut_labels)

#Visualise the Output 
plt.figure(figsize=(10,5))
sns.countplot('L6M_DR_AMT_BIN', data= pd_df)

# COMMAND ----------

# DBTITLE 1,1YR_DR_AMT
plt.figure(figsize=(10,5))
sns.distplot(pd_df['1YR_DR_AMT'])

#We need to Bind this Column to get the best result from it 
pd_df['1YR_DR_AMT'] = round(pd_df['1YR_DR_AMT'],0)
cut_labels = [1, 2, 3, 4,5,6,7,8]
cut_bins = [-1,0, 25000, 100000, 500000, 1000000,10000000,50000000,(max(pd_df['1YR_DR_AMT']))]
pd_df['1YR_DR_AMT_BIN'] = pd.cut(pd_df['1YR_DR_AMT'], bins=cut_bins, labels = cut_labels)

#Visualise the Output 
plt.figure(figsize=(10,5))
sns.countplot('1YR_DR_AMT_BIN', data= pd_df)

# COMMAND ----------

# DBTITLE 1,GENDER
pd_df.GENDER.value_counts()

plt.figure(figsize=(8,5))
sns.countplot('GENDER', data= pd_df)

# COMMAND ----------

# DBTITLE 1,Tenure
pd_df.Tenure.value_counts()
plt.figure(figsize=(10,5))
sns.distplot(pd_df['Tenure'])

# COMMAND ----------

# DBTITLE 1,Product_Cat	
pd_df.Product_Cat.value_counts()

plt.figure(figsize=(8,5))
sns.countplot('Product_Cat', data= pd_df)

# COMMAND ----------

# DBTITLE 1,HASDEBIT
pd_df.HASDEBIT.value_counts()

plt.figure(figsize=(8,5))
sns.countplot('HASDEBIT', data= pd_df)

# COMMAND ----------

# DBTITLE 1,MARITAL_STATUS
series = pd.value_counts(pd_df.MARITAL_STATUS)
mask = (series/series.sum() * 100).lt(1)
pd_df['new_marital_status'] = np.where(pd_df['MARITAL_STATUS'].isin(series[mask].index),'OTHERS',pd_df['MARITAL_STATUS'])
pd_df.drop('MARITAL_STATUS', axis = 1, inplace = True)

plt.figure(figsize=(8,5))
sns.countplot('new_marital_status', data= pd_df)

# COMMAND ----------

# DBTITLE 1,OCCUPATION
pd_df.OCCUPATION.value_counts()
series = pd.value_counts(pd_df.OCCUPATION)
mask = (series/series.sum() * 100).lt(.5)
pd_df['new_occupation'] = np.where(pd_df['OCCUPATION'].isin(series[mask].index),'OTHERS',pd_df['OCCUPATION'])
pd_df.drop('OCCUPATION', axis = 1, inplace = True)


plt.figure(figsize=(12,5))
sns.countplot('new_occupation', data= pd_df)

# COMMAND ----------

# DBTITLE 1,AGE
plt.figure(figsize=(10,5))
sns.distplot(pd_df['AGE'])

# COMMAND ----------

# DBTITLE 1,STATE
pd_df.STATE.value_counts()
#Correct Not Application to Missing 
pd_df['STATE'] = pd_df.STATE.replace('Not Applicable','Missing')
pd_df.STATE.value_counts()

series = pd.value_counts(pd_df.STATE)
mask = (series/series.sum() * 100).lt(1.25)
pd_df['new_state'] = np.where(pd_df['STATE'].isin(series[mask].index),'OTHERS',pd_df['STATE'])
pd_df.drop(['STATE'], axis = 1, inplace = True)

plt.figure(figsize=(20,5))
sns.countplot('new_state', data= pd_df)

# COMMAND ----------

# DBTITLE 1,F6M_AVG_BAL
plt.figure(figsize=(10,5))
sns.distplot(pd_df['F6M_AVG_BAL'])

# COMMAND ----------

# DBTITLE 1,L6M_AVG_BAL
plt.figure(figsize=(10,5))
sns.distplot(pd_df['L6M_AVG_BAL'])

# COMMAND ----------

# DBTITLE 1,AVG_BAL_1YR
plt.figure(figsize=(10,5))
sns.distplot(pd_df['AVG_BAL_1YR'])

# COMMAND ----------

# DBTITLE 1,F6M_CR_TRAN_COUNT
plt.figure(figsize=(10,5))
sns.distplot(pd_df['F6M_CR_TRAN_COUNT'])

pd_df['F6M_CR_TRAN_COUNT_BIN'] = pd.cut(pd_df['F6M_CR_TRAN_COUNT'],
                            bins=[-1,0, 12, 60, 120,  (max(pd_df['F6M_CR_TRAN_COUNT']))],
                            labels=[1,2,3,4,5])

#Visualise the Output 
plt.figure(figsize=(10,5))
sns.countplot('F6M_CR_TRAN_COUNT_BIN', data= pd_df)

# COMMAND ----------

# DBTITLE 1,L6M_CR_TRAN_COUNT
plt.figure(figsize=(10,5))
sns.distplot(pd_df['L6M_CR_TRAN_COUNT'])

pd_df['L6M_CR_TRAN_COUNT_BIN'] = pd.cut(pd_df['L6M_CR_TRAN_COUNT'],
                            bins=[-1,0, 12, 60, 120,  (max(pd_df['L6M_CR_TRAN_COUNT']))],
                            labels=[1,2,3,4,5])

#Visualise the Output 
plt.figure(figsize=(10,5))
sns.countplot('L6M_CR_TRAN_COUNT_BIN', data= pd_df)

# COMMAND ----------

# DBTITLE 1,F6M_DR_TRAN_COUNT
plt.figure(figsize=(10,5))
sns.distplot(pd_df['F6M_DR_TRAN_COUNT'])

pd_df['F6M_DR_TRAN_COUNT_BIN'] = pd.cut(pd_df['F6M_DR_TRAN_COUNT'],
                            bins=[-1,0, 12, 60, 120,  (max(pd_df['F6M_DR_TRAN_COUNT']))],
                            labels=[1,2,3,4,5])

#Visualise the Output 
plt.figure(figsize=(10,5))
sns.countplot('F6M_DR_TRAN_COUNT_BIN', data= pd_df)

# COMMAND ----------

# DBTITLE 1,L6M_DR_TRAN_COUNT
plt.figure(figsize=(10,5))
sns.distplot(pd_df['L6M_DR_TRAN_COUNT'])

pd_df['L6M_DR_TRAN_COUNT_BIN'] = pd.cut(pd_df['L6M_DR_TRAN_COUNT'],
                            bins=[-1,0, 12, 60, 120,  (max(pd_df['L6M_DR_TRAN_COUNT']))],
                            labels=[1,2,3,4,5])

#Visualise the Output 
plt.figure(figsize=(10,5))
sns.countplot('L6M_DR_TRAN_COUNT_BIN', data= pd_df)

# COMMAND ----------

# DBTITLE 1,F6M_CR_AMT
plt.figure(figsize=(10,5))
sns.distplot(pd_df['F6M_CR_AMT'])

#We need to Bind this Column to get the best result from it 
pd_df['F6M_CR_AMT'] = round(pd_df['F6M_CR_AMT'],0)
cut_labels = [1, 2, 3, 4,5,6,7,8]
cut_bins = [-1,0, 25000, 100000, 500000, 1000000,10000000,20000000,(max(pd_df['F6M_CR_AMT']))]
pd_df['F6M_CR_AMT_BIN'] = pd.cut(pd_df['F6M_CR_AMT'], bins=cut_bins, labels = cut_labels)

#Visualise the Output 
plt.figure(figsize=(10,5))
sns.countplot('F6M_CR_AMT_BIN', data= pd_df)

# COMMAND ----------

# DBTITLE 1,L6M_CR_AMT
plt.figure(figsize=(10,5))
sns.distplot(pd_df['L6M_CR_AMT'])

#We need to Bind this Column to get the best result from it 
pd_df['L6M_CR_AMT'] = round(pd_df['L6M_CR_AMT'],0)
cut_labels = [1, 2, 3, 4,5,6,7,8]
cut_bins = [-1,0, 25000, 100000, 500000, 1000000,10000000,20000000,(max(pd_df['L6M_CR_AMT']))]
pd_df['L6M_CR_AMT_BIN'] = pd.cut(pd_df['L6M_CR_AMT'], bins=cut_bins, labels = cut_labels)

#Visualise the Output 
plt.figure(figsize=(10,5))
sns.countplot('L6M_CR_AMT_BIN', data= pd_df)

# COMMAND ----------

pd_df.head()

# COMMAND ----------

# DBTITLE 1,Others
#Drop Not Revelevant Columns
pd_df.drop(['1YR_CR_TRAN_COUNT', '1YR_DR_TRAN_COUNT', '1YR_CR_AMT' , 'F6M_DR_AMT' , 'L6M_DR_AMT', '1YR_DR_AMT' , 'F6M_AVG_BAL', 'L6M_AVG_BAL' , 'AVG_BAL_1YR' , 'F6M_CR_TRAN_COUNT', 'L6M_CR_TRAN_COUNT', 'F6M_DR_TRAN_COUNT', 'L6M_DR_TRAN_COUNT', 'F6M_CR_AMT', 'L6M_CR_AMT', 'Day_Since_Last_Tran'], axis = 1 , inplace = True)
# Fill numerical missing columns with 0
for col in pd_df.select_dtypes('float64').columns:
    pd_df[col].fillna(0, inplace=True)


# COMMAND ----------

pd_df.head()

# COMMAND ----------

# DBTITLE 1,Data Preparation 
pd_df.info()

# COMMAND ----------

a_cols = ['1YR_CR_TRAN_COUNT_BIN', '1YR_DR_TRAN_COUNT_BIN', '1YR_CR_AMT_BIN', 'F6M_DR_AMT_BIN', 'L6M_DR_AMT_BIN', '1YR_DR_AMT_BIN', 'F6M_CR_TRAN_COUNT_BIN', 'L6M_CR_TRAN_COUNT_BIN', 'F6M_DR_TRAN_COUNT_BIN', 'L6M_DR_TRAN_COUNT_BIN', 'F6M_CR_AMT_BIN', 'L6M_CR_AMT_BIN']

# COMMAND ----------

#correct columns to right datatype
for a in a_cols:
  pd_df[a] = pd_df[a].astype('int')

# COMMAND ----------

#Encode Categorical Variables

le = LabelEncoder()

leFeatures = ['new_marital_status', 'new_state', 'new_occupation', 'GENDER', 'Product_Cat', 'HASDEBIT']

for col in leFeatures:
  pd_df[col] = le.fit_transform(pd_df[col])

# COMMAND ----------

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

# load the model from memory
import pickle
model_path = '/dbfs/xgb_def_prob.pkl'
xgb = pickle.load(open(model_path, 'rb'))

# COMMAND ----------

df_features = pd_df.drop(['CUSTOMER_ID', 'COUNTRY'], axis=1)

# COMMAND ----------

# Apply Feature Selection to Train and Test Sets
#df_features = selection.transform(df_features)

# COMMAND ----------

pred_result = xgb.predict_proba(df_features)
pd_df['probability_of_repayment'] = pred_result[:, 0].reshape(-1, 1)

# COMMAND ----------

pd_df = pd_df[['COUNTRY', 'CUSTOMER_ID' , 'probability_of_repayment']]

# COMMAND ----------

pd_df.head(100)

# COMMAND ----------

#convert the dataset back to Pyspark and Export it back to the database
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
sqlCtx = SQLContext(sc)
final_df = sqlCtx.createDataFrame(final_df)

# COMMAND ----------

#Export Model Output to Database
final_df.write \
.format("com.databricks.spark.sqldw")\
.option("url", "jdbc:sqlserver://ubariskdbsvr.database.windows.net:1433;database=ubariskmldb")\
.option("user", user)\
.option("password",password)\
.option("tempDir",temp_dir_url)\
.option("forward_spark_azure_storage_credentials", "true")\
.option("dbtable", "stg.nhc_probability_of_repayment")\
.mode("overwrite")\
.save()
