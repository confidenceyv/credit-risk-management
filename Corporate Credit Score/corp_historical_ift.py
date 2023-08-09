# Databricks notebook source
spark.conf.set(
"fs.azure.account.key.ubariskdl.dfs.core.windows.net",
"WIltzoJWmkC9x1pmXadJggmodCMFHU5A8JgS8eoInZkaoNZ9acraHrkALVOQgjtYCHgcHiQbTPhyO1DK1slrew=="
)

# COMMAND ----------

dbutils.fs.ls("abfss://risk-datalake@ubariskdl.dfs.core.windows.net/landing/visionro")

# COMMAND ----------

spark.read.load("abfss://risk-datalake@ubariskdl.dfs.core.windows.net/landing/visionro/azure_datalake_visionro_ift_historical_rpt.txt")

# COMMAND ----------

