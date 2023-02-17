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
from pydeequ import *

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
        # Send start notification 
        subject=f"Start Process Date {date_process['today']}"    
        msg=f"""The process for day {date_process['today']} has started"""
        send_notification(subject=subject,msg=msg)

        # create databases into databricks system
        create_database_catalog()
        dbutils.notebook.run("notebooks/Test/POKRakezIngestRawFullLoadsparkSQLTest",0)
        dbutils.notebook.run("notebooks/Test/POKRakezIngestGoldFullLoadsparkSQLTest",0)


        # Send finish notification 

        subject=f"Finish Process Date {date_process['today']}"    
        msg=f"""The process for day {date_process['today']} has finished"""
        send_notification(subject=subject,msg=msg)

        
    except Exception as e:
        subject=f"Error log date {date_process['today']}"        
        msg=f"""Error creating databases in databricks please review ERROR is: {e}"""
        send_notification(subject=subject,msg=msg)
        

# COMMAND ----------

main()
