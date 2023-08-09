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
query = "SELECT * FROM stg.vw_Keams_segments_corp"

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

# Modules
import matplotlib.pyplot as plt
from matplotlib.image import imread
import pandas as pd
import seaborn as sns
#from sklearn.datasets.samples_generator import (make_blobs,
#                                                make_circles,
#                                                make_moons)
from sklearn.cluster import KMeans, SpectralClustering
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_samples, silhouette_score

%matplotlib inline
sns.set_context('notebook')
plt.style.use('fivethirtyeight')
from warnings import filterwarnings
filterwarnings('ignore')

# COMMAND ----------

kmeans_df = df[["F6M_AVG_BAL", "L6M_AVG_BAL", "Frequency", "Recency_in_days", "Monetary"]]

# COMMAND ----------

from mpl_toolkits.mplot3d import Axes3D
import numpy as np


x = kmeans_df['Frequency']
y = kmeans_df['Recency_in_days']
z = kmeans_df['Monetary']

fig = plt.figure(figsize=(6, 6))
ax = fig.add_subplot(111, projection='3d')
ax.scatter(x, y, z,
           linewidths=1, alpha=.7,
           edgecolor='k',
           s = 200,
           c=z)
plt.show()

# COMMAND ----------

#Q1 = np.percentile(df, 25, interpolation = 'midpoint') # The lower quartile Q1 is calculated.
#Q3 = np.percentile(df, 75, interpolation = 'midpoint') # The upper quartile Q3 is calculated.
#IQR = Q3-Q1 # The Interquartile range is calculated.
#Q3 + 1.5*IQR, Q1-1.5 * IQR # The outlier range is calculated.

# COMMAND ----------

#Standardized the data
from sklearn.preprocessing import StandardScaler
std_data = StandardScaler().fit_transform(kmeans_df)

# COMMAND ----------

# import numpy as np
# from numpy.linalg import norm


# class Kmeans:
#     '''Implementing Kmeans algorithm.'''

#     def __init__(self, n_clusters, max_iter=100, random_state=123):
#         self.n_clusters = n_clusters
#         self.max_iter = max_iter
#         self.random_state = random_state

#     def initializ_centroids(self, X):
#         np.random.RandomState(self.random_state)
#         random_idx = np.random.permutation(X.shape[0])
#         centroids = X[random_idx[:self.n_clusters]]
#         return centroids

#     def compute_centroids(self, X, labels):
#         centroids = np.zeros((self.n_clusters, X.shape[1]))
#         for k in range(self.n_clusters):
#             centroids[k, :] = np.mean(X[labels == k, :], axis=0)
#         return centroids

#     def compute_distance(self, X, centroids):
#         distance = np.zeros((X.shape[0], self.n_clusters))
#         for k in range(self.n_clusters):
#             row_norm = norm(X - centroids[k, :], axis=1)
#             distance[:, k] = np.square(row_norm)
#         return distance

#     def find_closest_cluster(self, distance):
#         return np.argmin(distance, axis=1)

#     def compute_sse(self, X, labels, centroids):
#         distance = np.zeros(X.shape[0])
#         for k in range(self.n_clusters):
#             distance[labels == k] = norm(X[labels == k] - centroids[k], axis=1)
#         return np.sum(np.square(distance))
    
#     def fit(self, X):
#         self.centroids = self.initializ_centroids(X)
#         for i in range(self.max_iter):
#             old_centroids = self.centroids
#             distance = self.compute_distance(X, old_centroids)
#             self.labels = self.find_closest_cluster(distance)
#             self.centroids = self.compute_centroids(X, self.labels)
#             if np.all(old_centroids == self.centroids):
#                 break
#         self.error = self.compute_sse(X, self.labels, self.centroids)
    
#     def predict(self, X):
#         distance = self.compute_distance(X, old_centroids)
#         return self.find_closest_cluster(distance)

# COMMAND ----------

# # Run local implementation of kmeans
# km = Kmeans(n_clusters=5, max_iter=100)
# km.fit(std_data)
# centroids = km.centroids

# COMMAND ----------

# # Plot the clustered data
# fig, ax = plt.subplots(figsize=(6, 6))
# plt.scatter(std_data[km.labels == 0, 0], std_data[km.labels == 0, 1],
#             c=None, label='cluster 1')
# plt.scatter(std_data[km.labels == 1, 0], std_data[km.labels == 1, 1],
#             c=None, label='cluster 2')
# plt.scatter(std_data[km.labels == 0, 1], std_data[km.labels == 0, 1],
#             c=None, label='cluster 3')
# plt.scatter(std_data[km.labels == 1, 1], std_data[km.labels == 1, 0],
#             c=None, label='cluster 4')
# #plt.scatter(std_data[km.labels == 1, 0], std_data[km.labels == 0, 1],
#    #         c=None, label='cluster 5')
# #plt.scatter(centroids[:, 0], centroids[:, 1], marker='*', s=300,
#            # c='r', label='centroid')
# #plt.legend()
# #plt.xlim([-2, 2])
# #plt.ylim([-2, 2])
# #ax.set_aspect('equal')

# COMMAND ----------

# # Run the Kmeans algorithm and get the index of data points clusters
# sse = []
# list_k = list(range(1, 10))

# for k in list_k:
#     km = KMeans(n_clusters=k)
#     km.fit(std_data)
#     sse.append(km.inertia_)

# # Plot sse against k
# plt.figure(figsize=(6, 6))
# plt.plot(list_k, sse, '-o')
# plt.xlabel(r'Number of clusters *k*')
# plt.ylabel('Sum of squared distance');

# COMMAND ----------



# COMMAND ----------

# Run the Kmeans algorithm
km = KMeans(n_clusters=5)
labels = km.fit_predict(std_data)
centroids = km.cluster_centers_

# COMMAND ----------

df['clusters'] = pd.Series(labels, index=df.index)

# COMMAND ----------

df.head()

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
.option("dbtable", "stg.kmeans_customer_segments_corp")\
.mode("overwrite")\
.save()

# COMMAND ----------

