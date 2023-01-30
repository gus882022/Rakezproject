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
from config.

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
        
        
        # Send finish notification 
        
        subject=f"Finish Process Date {date_process['today']}"    
        msg=f"""The process for day {date_process['today']} has finished"""
        send_notification(subject=subject,msg=msg)
        
        
    except Exception as e:
        subject=f"Error log date {date_process['today']}"        
        msg=f"""Error creating databases in databricks please review ERROR is: {e}"""
        send_notification(subject=subject,msg=msg)
        

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execute main process

# COMMAND ----------

main()

# COMMAND ----------

for line in dbutils.fs.ls('s3://demodmsgbraw/candidate/'):
    id_execution=line
    print(line[1])

# COMMAND ----------

df.write.format("delta").mode("overwrite").option("path","s3://demodmsgbsilver/customers/").saveAsTable(f"{databases['silver']}.customers")

# COMMAND ----------

df.createOrReplaceTempView('customers')

# COMMAND ----------

query=f"""
        create table if not exists {databases['silver']}.customers
        using delta
        location 's3://demodmsgbsilver/customers/'
        as
        select *
        from customers
"""
spark.sql(query)

# COMMAND ----------

# MAGIC %sql
# MAGIC use  silver_data_rakez;
# MAGIC 
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from candidate

# COMMAND ----------

send_notification(msg="Hola",subject="Hola")

# COMMAND ----------

dbutils.fs.cp('s3a://demodmsgbraw/candidate/','s3a://demodmsgbraw/processed/candidate',True)

# COMMAND ----------


