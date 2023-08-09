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
query = " SELECT   [COUNTRY] ,[CUSTOMER_ID] ,[first_six_months_average_balance] [F6M_AVG_BAL] ,[last_six_months_average_balance] [L6M_AVG_BAL] \
	  ,([one_year_cr_tran] + [one_year_dr_tran]) Frequency  ,[recency][Recency_in_days]  ,[one_year_average_balance] Monetary   \
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

df = df.toPandas()

# COMMAND ----------

df1 = df.copy()

# COMMAND ----------

df.info()

# COMMAND ----------

df.isnull().sum()

# COMMAND ----------

Min = df['Monetary'].min()

# COMMAND ----------

Max = df['Monetary'].max()

# COMMAND ----------

import numpy as np
df['Monetary_Band'] = np.select(
    [
        df['Monetary'].between(Min, 0, inclusive=True), 
        df['Monetary'].between(1, 4999, inclusive=True),
        df['Monetary'].between(5000, 49999, inclusive=True),
        df['Monetary'].between(50000, 100000, inclusive=True),
        df['Monetary'].between(100001, 499999, inclusive=True),
        df['Monetary'].between(500000, 999999, inclusive=True),
        df['Monetary'].between(1000000, 5000000, inclusive=True),
        df['Monetary'].between(5000001, Max, inclusive=True)
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

df['Recency_Band'] = np.select(
    [
        df['Recency_in_days'].between(0, 30, inclusive=True), 
        df['Recency_in_days'].between(91, 180, inclusive=True),
        df['Recency_in_days'].between(180, 365, inclusive=True),
        df['Recency_in_days'].between(31, 90, inclusive=True),
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

df.loc[df['Recency_Band'] == 'Between 0 and 30 Days' , 'R_Score'] = 5
df.loc[df['Recency_Band'] == 'Between 31 and 90 Days' , 'R_Score'] = 4
df.loc[df['Recency_Band'] == 'Between 91 and 180 Days' , 'R_Score'] = 3
df.loc[df['Recency_Band'] == 'Between 181 and 365 Days' , 'R_Score'] = 2
df.loc[df['Recency_Band'] == 'Between 366 and 730 Days' , 'R_Score'] = 1
df.loc[df['Recency_Band'] == 'Above 730 Days' , 'R_Score'] = 0

# COMMAND ----------

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

#Encode Categorical Variables
from sklearn.preprocessing import LabelEncoder
le = LabelEncoder()

features_df = ['F6M_AVG_BAL', 'L6M_AVG_BAL', 'Frequency', 'Recency_in_days', 'Monetary', 'M_Score', 'R_Score', 'F_Score', 'Monetary_Band',
              'Recency_Band', 'Frequency_Band']

for col in features_df:
  df[col] = le.fit_transform(df[col])

# COMMAND ----------

# Initialize a scaler, then apply it to the features
from sklearn.preprocessing import MinMaxScaler
scaler = MinMaxScaler() # default=(0, 1)
 
for col in df.select_dtypes(['float64']):
    df[col]= scaler.fit_transform(df[col].values.reshape(-1,1))

# COMMAND ----------

df.drop(['COUNTRY', 'CUSTOMER_ID'], axis = 1, inplace = True)

# COMMAND ----------

from sklearn.decomposition import PCA
pca = PCA()
pca.fit(df)
pca.explained_variance_ratio_

# COMMAND ----------

aa = pca.explained_variance_ratio_.shape[0]

# COMMAND ----------

import matplotlib.style as style
import matplotlib.pyplot as plt
%matplotlib inline
fig, ax = plt.subplots(figsize=(25,8))
 
plt.plot(range(0,aa), pca.explained_variance_ratio_.cumsum(), marker = 'o', linestyle = '--')
plt.title('Explained Variance by Components')
plt.xlabel('Number of Components')
plt.ylabel('Cumulative Explained Variance')
display(fig)

# COMMAND ----------

# determined that the number of components is 12
pca = PCA(n_components = 10)

# COMMAND ----------

pca.fit(df)
pca.components_

# COMMAND ----------

import pandas as pd
df_pca_comp = pd.DataFrame(data = pca.components_,
                           columns = df.columns.values,
                           index = ['C1', 'C2','C3','C4','C5','C6','C7','C8','C9','C10'])
df_pca_comp

# COMMAND ----------

# Heat Map for Principal Components against original features.
import seaborn as sns
fig, ax = plt.subplots(figsize=(16,7))
sns.heatmap(df_pca_comp,
            vmin = -1, 
            vmax = 1,
            cmap = 'RdBu',
            annot = True)
plt.yticks([0, 1, 2,3,4,5,6,7,8,9], 
           ['C1', 'C2','C3','C4','C5','C6','C7','C8','C9','C10'],
           rotation = 60,
           fontsize = 9)
 
display(fig)

# COMMAND ----------

scores_pca =  pca.transform(df)

# COMMAND ----------

from sklearn.cluster import KMeans
kmeans_pca = KMeans(n_clusters = 5, init = 'k-means++', random_state = 15)
kmeans_pca.fit_transform(scores_pca)

# COMMAND ----------

df['clusters'] = kmeans_pca.labels_

# COMMAND ----------

df['CUSTOMER_ID'] = df1.CUSTOMER_ID


# COMMAND ----------

df.head()

# COMMAND ----------

df.clusters.value_counts()


# COMMAND ----------

#convert the dataset back to Pyspark and Export it back to the database
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
sqlCtx = SparkSession.builder.getOrCreate()
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
.option("dbtable", "stg.kmeans_newcustomer_segments_corp")\
.mode("overwrite")\
.save()

# COMMAND ----------

