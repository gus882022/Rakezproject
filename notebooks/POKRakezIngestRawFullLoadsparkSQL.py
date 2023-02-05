# Databricks notebook source
# MAGIC %md
# MAGIC ### Autoreload config files

# COMMAND ----------

# MAGIC %reload_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Libraries and config files

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql import SparkSession
import time
import json
from config.config_file import *
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.functions import col


# COMMAND ----------

# MAGIC %md
# MAGIC ### Declaration of variables

# COMMAND ----------

path_raw_datalake = paths_datalake['raw_bucket']
path_silver_datalake = paths_datalake['silver_bucket']
date = date_process['today']
database = databases['silver']
log_file = []

# COMMAND ----------

# MAGIC %md
# MAGIC ### Function to store data in silver Zone in Delta Format

# COMMAND ----------

# spark sql example code
def save_delta_table() -> DataFrame:
    # read dictionary for getting tables configuration
    tables_schemas = tables_schema_config['Tables']
    # loop for read configurations
    for table in tables_schemas:
        try:
       
            # store table_schemas dictionary in a variable
            table_config = tables_schemas[table]

            # define how the table is partitioned
            partition_by = ",".join(table_config['partition'])

            # varible to identify the path from raw zone
            path_table_raw = f"{path_raw_datalake}/{table_config['path']}"

            # varible to identify the path from silver zone
            path_table_silver = f"{path_silver_datalake}/{table_config['path']}"

            # varible to identify if the path contains data
            exists_data = file_exists(f"{path_table_raw}")
            
            if exists_data:
                fields = table_config['fields']
                schema_fields=[]
                
                # identifies what is the schema for the table and concat the fields 
                
                for field in fields:
                    cast_field = f"cast({field} as {fields[field]}) as {field}"
                    schema_fields.append(cast_field)
                    #schema_fields.append(StructField(field,fields[field],True))
                
                schema_table=f"select {','.join(schema_fields)} from {table}"
                
                # app flow generates an id execution for each process to get data
                
                for executions in dbutils.fs.ls(path_table_raw):
                    id_execution=executions[1]
                    df = spark.read.option("inferSchema",True).parquet(f"{path_table_raw}{id_execution}")
                    df.createOrReplaceTempView(table)
                    # identifies if exists data in S3
                    
                    if file_exists(f"{path_table_silver}"):
                        dbutils.fs.rm(f"{path_table_silver}",True)
                    # drop the table if exists into database
                    
                    spark.sql(f"drop table if exists {database}.{table}")
                    # identifies when a table has partitions
                    
                    if partition_by != "":
                        query=f"""
                                    create table if not exists {database}.{table}
                                    using delta
                                    location '{path_table_silver}'
                                    partitioned by ({partition_by})
                                    as
                                    {schema_table}"""
                    else:
                        query=f"""
                                    create table if not exists {database}.{table}
                                    using delta
                                    location '{path_table_silver}'
                                    as
                                    {schema_table}"""
                    spark.sql(query)
                    count_read_regs = df.count()
                    query=f"select count(*) as regs from {database}.{table}"
                    count_write_regs = spark.sql(query).collect()[0]["regs"]
                    
                    # save in the log list 
                    
                    log_file.append([table,"Table Sucessfull Loaded",count_write_regs,count_read_regs])
            else:
                log_file.append([table,"Don't exist data for the table",0,0])
        except Exception as e:
            log_file.append([table,"Error loading table please review the code ",0,0])
            print(e)
    # Convert log list into a Dataframe
    logColumns = ["TableName","Status","Rows_Loaded","Rows_Readed"]
    logdf = spark.createDataFrame(data=log_file, schema = logColumns)
    
    # Return log dataframe
    return logdf
    
        

# COMMAND ----------

# MAGIC %md
# MAGIC ### Function main

# COMMAND ----------

def main_raw_silver_full() -> None:
    # store the log dataframe
    logdf = save_delta_table()
    
    # convert pyspark dataframe to pandas df to send it as table on email
    log_df=convert_df_pandas(logdf)
    
    # variable for design email in HTML format
    html_content = f"""\
    <html>
      <head></head>
      <body>
        <p> Log for day {date} </p>
        {log_df.to_html()}
      </body>
    </html>
    """
    
    # send notification to the email
    send_notification(subject=f"RAW TO SILVER ZONE FULL LOAD -> LOG FOR DAY {date} ",msg=html_content)

# COMMAND ----------

main_raw_silver_full()
