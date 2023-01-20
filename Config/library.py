# Databricks notebook source
# DBTITLE 1,Method for checking if the path is mounted in databricks
def file_exists(path):
  try:
    dbutils.fs.ls(path)
    return True
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      return False
    else:
      raise

# COMMAND ----------

# DBTITLE 1,Dictionary of paths from datalake in point mount
paths_datalake={
    'staging_area':'/mnt/datalake/staging_area/',
    'raw_bucket':'/mnt/datalake/raw_bucket/',
    'silver_bucket':'/mnt/datalake/silver_bucket/',
    'gold_bucket':'/mnt/datalake/gold_bucket/',
    'libraries_bucket':'/mnt/datalake/libaries/'
}


# COMMAND ----------

# DBTITLE 1,Dictionary of paths from datalake in S3
paths_datalake_s3={
    'staging_area':'s3a://demodmsgbstaging/',
    'raw_bucket':'s3a://demodmsgbraw/',
    'silver_bucket':'s3a://demodmsgbsilver/',
    'gold_bucket':'s3a://demodmsgbgold/',
    'libraries_bucket':'s3a://demodmsgblibraries/'
}

# COMMAND ----------

dbutils.fs.ls('/mnt/datalake/')

# COMMAND ----------

# MAGIC %sh pwd

# COMMAND ----------

import sys
print("\n".join(sys.path))

# COMMAND ----------

import tables_config

# COMMAND ----------


