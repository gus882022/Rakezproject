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
from pyspark.sql import *


# COMMAND ----------

# MAGIC %md
# MAGIC ### Declaration of variables

# COMMAND ----------

path_gold_datalake = paths_datalake['gold_bucket']
date = date_process['today']
database_target = databases['gold']
database_source = databases['silver']
log_file = []

# COMMAND ----------

# MAGIC %md
# MAGIC ### Function to store data in Gold Zone in Delta Format

# COMMAND ----------

# spark sql example code
def save_delta_table() -> DataFrame:
    # read dictionary for getting tables configuration
    tables_schemas = tables_aggregated['Tables']
    # loop for read configurations
    for table in tables_schemas:
        try:
            
            count_inserted_regs = 0
            
            count_updated_regs = 0
            
            count_deleted_regs = 0
            
            condition_list = []
            
            rows_affected = []
            
            # store table_schemas dictionary in a variable
            table_config = tables_schemas[table]

            # define how the table is partitioned
            partition = ",".join(table_config['partition'])

            # variable to identify the path from silver zone
            path_table_gold = f"{path_gold_datalake}/{table_config['path']}"

            # variable to identify the load type of the table ( Cdc- incremental, full load)
            type = table_config['type']
            
            # table primary key
            primary_key = table_config['primary_key']

            # variable to identify the function to create the table
            function = table_config['function_cdc']
            
            for x in primary_key:
                    condition_list.append(f"source.{x}=target.{x}")
            
            condition_join = " and ".join(condition_list)


            rows_affected = function(db_source=database_source,db_target=database_target,table_target=table,condition_table=condition_join,operation_table=type)
            
            count_inserted_regs = rows_affected[0]
            count_updated_regs = rows_affected[1]
            count_deleted_regs = rows_affected[2]
            
            # save in the log list 
            log_file.append([table,"Table Sucessfull Loaded",count_inserted_regs,count_updated_regs,count_deleted_regs])
            
        except Exception as e:
            log_file.append([table,"Error loading table please review the code ",0,0,0])
            print(e)
        
    # Convert log list into a Dataframe
    logColumns = ["TableName","Status","num_inserted_rows","num_updated_rows","num_deleted_rows"]
    logdf = spark.createDataFrame(data=log_file, schema = logColumns)
    
    # Return log dataframe
    return logdf
    
        

# COMMAND ----------

# MAGIC %md
# MAGIC ### Function main

# COMMAND ----------

def main_raw_gold_full()-> None:
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
    send_notification(subject=f"SILVER TO GOLD ZONE FULL LOAD -> LOG FOR DAY {date} ",msg=html_content)

# COMMAND ----------

main_raw_gold_full()
