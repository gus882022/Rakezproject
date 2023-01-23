# Databricks notebook source
# MAGIC %md
# MAGIC ### Autoreload config files

# COMMAND ----------

# MAGIC %reload_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql import SparkSession
import time
import json
from config.config_file import *
from config.tables_config import *
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from datetime import date
from pyspark.sql.functions import col


# COMMAND ----------

# DBTITLE 1,Declaration of Variables
table_schema = tables_schema_config
path_staging_datalake = paths_datalake['staging_area']
path_raw_datalake = paths_datalake['raw_bucket']
path_silver_datalake = paths_datalake['silver_bucket']
path_gold_datalake = paths_datalake['gold_bucket']
path_raw_datalake_s3 = paths_datalake_s3['raw_bucket']
path_silver_datalake_s3 = paths_datalake_s3['silver_bucket']
path_gold_datalake_s3 = paths_datalake_s3['gold_bucket']
database_raw = databases['raw']

print(database_raw)
print(path_staging_datalake)
print(path_raw_datalake)
print(path_silver_datalake)
print(path_gold_datalake)


# COMMAND ----------

schemas_config=table_schema['schema']
list_logs_tables={}
for schema in schemas_config.keys():
    list_tables=schemas_config[schema]
    list_path_tables_crawler=[]
    for table in list_tables.keys():
            table_name = table
            fields = schemas_config[schema][table_name]['fields']
            path_table = [x for x in schemas_config[schema][table_name]['path_table'].values()][0]
            schema_fields=[]
            for field in fields:
                schema_fields.append(StructField(field,fields[field],True))
            schema_table= StructType(schema_fields)
            
            ## check if exists data
            
            if file_exists(f'{path_staging_datalake}{schema}/{table_name}/'):
                df = spark.read.parquet(f'{path_staging_datalake}{schema}/{table_name}/',schema=schema_table)
                path_delta_table=f"{path_raw_datalake}{schema}/{table_name}/"
                path_delta_table_s3=f"{path_raw_datalake_s3}{schema}/{table_name}/"
                spark.sql(f"use {database_raw}")
                df_tables= spark.sql("show tables")
                regs_loaded=df.count()
                today = date.today()
                df = df.select('*',current_date().alias('ts_load'))
                ## check if the table exists in the database
                
                if df_tables.filter(F.col('tableName')==f'{schema}_{table_name}').count() >0:
                    try:
                        save_delta_table(df,path_delta_table,database_raw,f"{schema}_{table_name}")
                        regs_written= spark.sql(f"select count(*) from {database_raw}.{schema}_{table_name} where cast(ts_load as varchar(50))={str(today)}")
                        value={'Result': 'Table Sucessfull Loaded','regs_loaded': regs_loaded,'regs_written':regs_written}
                        list_logs_tables[f"{schema}_{table_name}"] = 'Table Sucessfull Loaded'
                    except:
                        list_logs_tables[f"{schema}_{table_name}"] = "Error loading table"
                        df_version_table = spark.sql(f"Describe History {database_raw}.{schema}_{table_name}")
                        version_table = df_version_table.agg({"version": "max"}).collect()[0]["max(version)"] ## get the last version of the table
                        df = spark.sql(f"Select * from {database_raw}.{schema}_{table_name} version as of {version_table}")
                        save_delta_table(df,path_delta_table,database_raw,f"{schema}_{table_name}")     
                else:
                    try:
                        save_delta_table(df,path_delta_table,database_raw,f"{schema}_{table_name}")
                        list_logs_tables[f"{schema}_{table_name}"] = 'Table Sucessfull Loaded'
                    except:
                        list_logs_tables[f"{schema}_{table_name}"] = "Error loading table"
            else:        
                list_logs_tables[f"{schema}_{table_name}"] = "Doesn't exist data"
                
subject=f"Log process {date_process['today']}"
send_notification(subject=subject,msg=str(list_logs_tables))
                    


# COMMAND ----------


