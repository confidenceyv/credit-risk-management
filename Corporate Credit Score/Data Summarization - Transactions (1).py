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
query = "SELECT  *, cast(getdate() as date) today_date  FROM  [biuser].[rpt_predict_data_final_corp]"

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

pd_df.head()

# COMMAND ----------

# DBTITLE 1,Credit Transactions
#One Year Credit Transactions
one_year_cr_tran = pd_df.iloc[:,5:17]
pd_df['one_year_cr_tran'] = one_year_cr_tran.sum(axis = 1)

#First Six Months Credit Transactions
first_six_months_cr_tran = pd_df.iloc[:,5:11]
pd_df['first_six_months_cr_tran'] = first_six_months_cr_tran.sum(axis = 1)

#Last Six Months Credit Transactions
Last_six_months_cr_tran = pd_df.iloc[:,11:17]
pd_df['last_six_months_cr_tran'] = Last_six_months_cr_tran.sum(axis = 1)

#Last Three Months Credit Transactions
Last_three_months_cr_tran = pd_df.iloc[:,14:17]
pd_df['last_three_months_cr_tran'] = Last_three_months_cr_tran.sum(axis = 1)


# COMMAND ----------

# DBTITLE 1,Debit Transactions
#One Year dredit Transactions
one_year_dr_tran = pd_df.iloc[:,17:29]
pd_df['one_year_dr_tran'] = one_year_dr_tran.sum(axis = 1)

#First Six Months dredit Transactions
first_six_months_dr_tran = pd_df.iloc[:,17:23]
pd_df['first_six_months_dr_tran'] = first_six_months_dr_tran.sum(axis = 1)

#Last Six Months dredit Transactions
Last_six_months_dr_tran = pd_df.iloc[:,23:29]
pd_df['last_six_months_dr_tran'] = Last_six_months_dr_tran.sum(axis = 1)

#Last Three Months dredit Transactions
Last_three_months_dr_tran = pd_df.iloc[:,26:29]
pd_df['last_three_months_dr_tran'] = Last_three_months_dr_tran.sum(axis = 1)


# COMMAND ----------

# DBTITLE 1,Credit Amount
#One Year Credit amtsactions
one_year_cr_amt = pd_df.iloc[:,29:41]
pd_df['one_year_cr_amt'] = one_year_cr_amt.sum(axis = 1)

#First Six Months Credit amtsactions
first_six_months_cr_amt = pd_df.iloc[:,29:35]
pd_df['first_six_months_cr_amt'] = first_six_months_cr_amt.sum(axis = 1)

#Last Six Months Credit amtsactions
Last_six_months_cr_amt = pd_df.iloc[:,35:41]
pd_df['last_six_months_cr_amt'] = Last_six_months_cr_amt.sum(axis = 1)

#Last Three Months Credit amtsactions
Last_three_months_cr_amt = pd_df.iloc[:,38:41]
pd_df['last_three_months_cr_amt'] = Last_three_months_cr_amt.sum(axis = 1)

# COMMAND ----------

# DBTITLE 1,Debit Amount
#One Year dredit amtsactions
one_year_dr_amt = pd_df.iloc[:,41:53]
pd_df['one_year_dr_amt'] = one_year_dr_amt.sum(axis = 1)

#First Six Months dredit amtsactions
first_six_months_dr_amt = pd_df.iloc[:,41:47]
pd_df['first_six_months_dr_amt'] = first_six_months_dr_amt.sum(axis = 1)

#Last Six Months dredit amtsactions
Last_six_months_dr_amt = pd_df.iloc[:,47:53]
pd_df['last_six_months_dr_amt'] = Last_six_months_dr_amt.sum(axis = 1)

#Last Three Months dredit amtsactions
Last_three_months_dr_amt = pd_df.iloc[:,50:53]
pd_df['last_three_months_dr_amt'] = Last_three_months_dr_amt.sum(axis = 1)

# COMMAND ----------

# DBTITLE 1,Inflow Retention 
#Inflow Retention for One Year
pd_df['one_year_inflow_retention'] = (pd_df.one_year_cr_amt - pd_df.one_year_dr_amt)

#Inflow Retention for First SIx Months Year
pd_df['first_six_months_inflow_retention']= (pd_df.first_six_months_cr_amt - pd_df.first_six_months_dr_amt)

#Inflow Retention for Last Six Months
pd_df['last_six_months_inflow_retention'] = (pd_df.last_six_months_cr_amt - pd_df.last_six_months_dr_amt)

#Inflow Retention for Last Three Months
pd_df['last_three_months_inflow_retention'] = (pd_df.last_three_months_cr_amt - pd_df.last_three_months_cr_amt	)

# COMMAND ----------

pd_df.head()

# COMMAND ----------

# DBTITLE 1,Convert MaxTran Date to Date Datatype 
import pandas as pd
pd_df['MAX_TRAN_DATE'] = pd.to_datetime(pd_df['MAX_TRAN_DATE'])
pd_df['today_date'] = pd.to_datetime(pd_df['today_date'])
a = pd_df['MAX_TRAN_DATE']
b = pd_df['today_date']
pd_df['Recency'] = (b-a).astype('<m8[D]')

# COMMAND ----------

pd_df.drop('today_date', axis = 1, inplace = True)

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
.option("dbtable", "stg.dataprep_summarized_transactions_corp")\
.mode("overwrite")\
.save()

# COMMAND ----------

