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
query = "select * from stg.LD_proc_train_set_cust_corp"

df = spark.read \
  .format("com.databricks.spark.sqldw") \
  .option("url", sql_dw_connection_string) \
  .option("tempdir", temp_dir_url) \
  .option("forward_spark_azure_storage_credentials", "true") \
  .option("query", query) \
  .load()

# COMMAND ----------

#display(df)

# COMMAND ----------

pd_df = df.toPandas()

# COMMAND ----------

import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler,OneHotEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix, classification_report, balanced_accuracy_score, roc_curve, roc_auc_score
import lightgbm as lgb
from xgboost import XGBClassifier
import numpy as np
import seaborn as sns
import matplotlib.style as style
import matplotlib.pylab as plt
%matplotlib inline
from sklearn.linear_model import Lasso, LogisticRegression
from sklearn.feature_selection import SelectFromModel
from BorutaShap import BorutaShap
from imblearn.over_sampling import SMOTE, SMOTENC
from sklearn.metrics import f1_score,accuracy_score

# COMMAND ----------

pd_df.head()

# COMMAND ----------

pd_df.dtypes

# COMMAND ----------

pd_df["Paid_Repayment_Amount"] = pd_df["Paid_Repayment_Amount"].astype(int).astype(float)
pd_df["Default_Repayment_Amount"] = pd_df["Default_Repayment_Amount"].astype(int).astype(float)
pd_df["Repayment_Amount"] = pd_df["Repayment_Amount"].astype(int).astype(float)

# COMMAND ----------

# DBTITLE 1,Split Data into Train and Test Splits
X = pd_df.drop(['Loan_Status','CUSTOMER_ID', 'COUNTRY', 'LOAN_ACCT', 'START_DATE', 'MATURITY_DATE'], axis=1)
#X = pd.get_dummies(X)
pd_df['Loan_Status'] = pd_df['Loan_Status'].replace (['Default','Paid'], [0, 1])
#print(df.dtypes)
y = pd_df['Loan_Status']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=123)

# COMMAND ----------

# DBTITLE 1,Base Model
class_weight = int(y_train.value_counts()[0]/y_train.value_counts()[1])
class_weight

# COMMAND ----------

a_xgb = XGBClassifier(
  scale_pos_weight=class_weight,
  seed=42)
a_xgb.fit(X_train,y_train)

print(classification_report(y_test,a_xgb.predict(X_test)))

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Feature Selection 


# COMMAND ----------

# DBTITLE 1,Lasso Regression
#Fit The Lasso Regression Model
selection = SelectFromModel(LogisticRegression(C=1, penalty='l1',solver='liblinear'))
selection.fit(X_train, y_train)
selection.get_support()

#Make a list of with the selected features
selected_features = X_train.columns[(selection.get_support())]
print('Total Features: {}'.format((X_train.shape[1])))
print('Selected Features: {}'.format(len(selected_features)))
print('Features with coefficients shrank to zero: {}'.format(
      np.sum(selection.estimator_.coef_ == 0)))

Removed_features = X_train.columns[(selection.estimator_.coef_ == 0).ravel().tolist()]
Removed_features = list(Removed_features)
Removed_features

# COMMAND ----------

# Apply Feature Selection to Train and Test Sets
X_train = selection.transform(X_train)
X_test = selection.transform(X_test)

# COMMAND ----------

# DBTITLE 1,BorutaShap
#model = RandomForestClassifier()
#Feature_Selector = BorutaShap(model = model, importance_measure='shap',
#                              classification=True)
#Feature_Selector.fit(X=X_train, y=y_train, n_trials=10, random_state=155)
#Feature_Selector.plot(which_features='all', 
#                      X_size=8, figsize=(25,8),
#                     y_scale='log')

# COMMAND ----------

#subset = Feature_Selector.Subset()
#sel_col = subset.columns
#sel_col

# COMMAND ----------

# DBTITLE 1,Handle Imbalanced Data


# COMMAND ----------

# DBTITLE 1,UnderSampling
# Get the length of the minority class
minority_class_length = len(pd_df[pd_df['Loan_Status'] == 1])
# Get the indices of the majority class and this will be use for the undersampling
majority_class_indices = pd_df[pd_df['Loan_Status'] == 0].index

# create a random indices of the majority class with the length of the minority class
random_majority_indices = np.random.choice(majority_class_indices,minority_class_length)
# get the indices of the minority class
minority_class_indices = pd_df[pd_df['Loan_Status'] == 1].index

#convert the two indices to list for easy concantation
minority_class_indices = list(minority_class_indices)
random_majority_indices = list(random_majority_indices)

under_sample_indices = minority_class_indices + random_majority_indices

un_df = un_df = pd_df.loc[under_sample_indices]

# COMMAND ----------

un_df.head()

# COMMAND ----------

un_X = un_df.drop(['Loan_Status','CUSTOMER_ID', 'COUNTRY', 'LOAN_ACCT','START_DATE', 'MATURITY_DATE'], axis=1)
#X = pd.get_dummies(X)
un_y = un_df['Loan_Status']
un_X_train, un_X_test, un_y_train, un_y_test = train_test_split(un_X, un_y, test_size=0.3, random_state=155)

# COMMAND ----------

# Apply Feature Selection to Train and Test Sets
un_X_train = selection.transform(un_X_train)
un_X_test = selection.transform(un_X_test)

# COMMAND ----------

# DBTITLE 1,Logistic Regression 
#Build Log reg on the undersampled Dataset
un_logreg_model = LogisticRegression(random_state=123, penalty='l2')
un_logreg_model.fit(un_X_train, un_y_train)
lr_y_pred = un_logreg_model.predict(un_X_test)
print(confusion_matrix(un_y_test,lr_y_pred))
print(classification_report(un_y_test,lr_y_pred))

# COMMAND ----------

# DBTITLE 1,Random Forest 
# Build random Forest on the undersampled Dataset
un_rf_model = RandomForestClassifier(max_depth=8, n_estimators=100)
un_rf_model.fit(un_X_train, un_y_train)
rf_y_pred = un_rf_model.predict(un_X_test)
print(confusion_matrix(un_y_test,rf_y_pred))
print(classification_report(un_y_test,rf_y_pred))

# COMMAND ----------

# DBTITLE 1,Light Gb
d_train = lgb.Dataset(un_X_train, label=un_y_train)
params = {}
params['learning_rate'] = 0.0001
params['boosting_type'] = 'gbdt'
params['objective'] = 'binary'
params['metric'] = 'binary_logloss'
# params['sub_feature'] = 0.5
params['num_leaves'] = 100
# params['min_data'] = 50
params['max_depth'] = 8
params['nthread'] = 8
params['metric'] = 'auc'
params['lambda_l1'] = 0.1
#params['scale_pos_weight'] = class_weight
params['feature_fraction'] = 0.65
params['max_bin'] = 100

#params['min_child_samples'] = 8, # specifies the minimum samples per leaf node.
#params['min_child_weight'] = class_weight, # minimal sum hessian in one leaf.

clf = lgb.train(params, d_train, 1000)
cf_y_pred = clf.predict(un_X_test)

for i in range(0,len(un_y_test)):
    if cf_y_pred[i]<0.5:
        cf_y_pred[i]=0
    else:  
        cf_y_pred[i]=1
        
print(confusion_matrix(un_y_test,cf_y_pred))
print(classification_report(un_y_test,cf_y_pred))

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,XGB
xgb = XGBClassifier(
   seed=42)
xgb.fit(un_X_train,un_y_train)
xg_y_pred = xgb.predict(un_X_test)
print(confusion_matrix(un_y_test,xg_y_pred))
print(classification_report(un_y_test,xg_y_pred))

# COMMAND ----------

print(classification_report(y_test,xgb.predict(X_test)))

# COMMAND ----------

# DBTITLE 1,Model Result


# COMMAND ----------

# DBTITLE 1,Confusion Matrix
def cm_analysis(y_true, y_pred, labels, mod, ymap=None, figsize=(10,8), cmap=plt.cm.Blues):
    if ymap is not None:
        y_pred = [ymap[yi] for yi in y_pred]
        y_true = [ymap[yi] for yi in y_true]
        labels = [ymap[yi] for yi in labels]
    accuracy= mod.score(un_X_test, un_y_test)
    #accuracy = accuracy_score(un_y_test,xg_y_pred)
    misclass = 1-accuracy
    cm = confusion_matrix(y_true, y_pred, labels=labels)
    cm_sum = np.sum(cm, axis=1, keepdims=True)
    cm_perc = cm / cm_sum.astype(float) * 100
    annot = np.empty_like(cm).astype(str)
    nrows, ncols = cm.shape
    for i in range(nrows):
        for j in range(ncols):
            c = cm[i, j]
            p = cm_perc[i, j]
            if i == j:
                s = cm_sum[i]
                annot[i, j] = '%.1f%%\n%d/%d' % (p, c, s)
            elif c == 0:
                annot[i, j] = ''
            else:
                annot[i, j] = '%.1f%%\n%d' % (p, c)
    cm = pd.DataFrame(cm, index=labels, columns=labels)
    cm.index.name = 'Actual'
    # cm.columns.name = 'Predicted'
    fig, ax = plt.subplots(figsize=figsize)
    sns.heatmap(cm, annot=annot, fmt='', ax=ax, cmap=cmap)
    plt.xlabel('Predicted \nAccuracy={:0.2f}; Misclassification={:0.2f}'.format(accuracy, misclass))
    plt.title('Model Confusion Matrix')
    plt.show()

# COMMAND ----------

cm_analysis(y_true= un_y_test,
            y_pred=xg_y_pred,
            labels= [0, 1],
            mod=xgb)

# COMMAND ----------

# DBTITLE 1,ROC AUC
fpr, tpr, _ = roc_curve(un_y_test,  xg_y_pred)
auc = roc_auc_score(un_y_test, xg_y_pred)
plt.plot(fpr,tpr,label="clf, auc="+str(auc))
plt.legend(loc=4)
plt.show()

# COMMAND ----------

# DBTITLE 1,Youden Index
clf_cm = confusion_matrix(un_y_test,xg_y_pred)
sensitivity = clf_cm[1,1]/ (clf_cm[1,1] +  clf_cm[1,0])
specificity = clf_cm[0,0]/ (clf_cm[0,0] +clf_cm[0,1])
Youden_Index = sensitivity + specificity -1
Youden_Index

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Predictions
# import pickle
# # save the model in the native sklearn format
# model_path = '/dbfs/xgb_def_repay.pkl'
# pickle.dump(xgb, open(model_path, 'wb'))

# COMMAND ----------

# DBTITLE 1,Get Data
#get predicted_data_from_the database
#querying data in SQL datawarehouse
query = "select * from stg.LD_proc_all_loans_cust_corp"

final_df = spark.read \
  .format("com.databricks.spark.sqldw") \
  .option("url", sql_dw_connection_string) \
  .option("tempdir", temp_dir_url) \
  .option("forward_spark_azure_storage_credentials", "true") \
  .option("query",query )\
  .load()

# COMMAND ----------

final_df = final_df.toPandas()

# COMMAND ----------

# DBTITLE 1,Get Features
df_features = final_df.drop(['CUSTOMER_ID', 'COUNTRY', 'LOAN_ACCT', 'START_DATE', 'MATURITY_DATE'], axis=1)
# for i in Removed_features:
#   if i in df_features.columns:
#     df_features = df_features.drop([i], axis=1)
    

# COMMAND ----------

# DBTITLE 1,Apply Feature Selection 
# Apply Feature Selection to Train and Test Sets
df_features = selection.transform(df_features)


# COMMAND ----------

# DBTITLE 1,Make Predictions
pred_result = xgb.predict_proba(df_features)
final_df['probability_of_repayment'] = pred_result[:, 0].reshape(-1, 1)

# COMMAND ----------

final_df = final_df[['COUNTRY', 'CUSTOMER_ID','LOAN_ACCT' , 'probability_of_repayment']]

# COMMAND ----------

# DBTITLE 1,Create Spark DataFrame
#convert the dataset back to Pyspark and Export it back to the database
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
sqlCtx = SQLContext(sc)
final_df = sqlCtx.createDataFrame(final_df)

# COMMAND ----------

# DBTITLE 1,Output Result
#Export Model Output to Database
final_df.write \
.format("com.databricks.spark.sqldw")\
.option("url", "jdbc:sqlserver://ubariskdbsvr.database.windows.net:1433;database=ubariskmldb")\
.option("user", user)\
.option("password",password)\
.option("tempDir",temp_dir_url)\
.option("forward_spark_azure_storage_credentials", "true")\
.option("dbtable", "stg.customer_loan_probability_of_repayment_corp")\
.mode("overwrite")\
.save()

# COMMAND ----------

