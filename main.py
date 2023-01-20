# Databricks notebook source
# MAGIC %md
# MAGIC ### Autoreload config files

# COMMAND ----------

# MAGIC %reload_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import config files

# COMMAND ----------

from config.config_file import *
from config.tables_config import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Function for create databases in dbfs storage

# COMMAND ----------

def create_database_catalog():
    spark.sql(f"create database if not exists {databases['raw']}");
    spark.sql(f"create database if not exists {databases['silver']}");
    spark.sql(f"create database if not exists {databases['gold']}");

# COMMAND ----------

# MAGIC %md
# MAGIC ### Function main 

# COMMAND ----------

def main():
    try:
        create_database_catalog()
        
    except:
        subject=f"Error log date {date_process['today']}"
        msg="Error creating databases in databricks please review"
        send_notification(subject=subject,msg=msg)
        

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execute main process

# COMMAND ----------

main()
