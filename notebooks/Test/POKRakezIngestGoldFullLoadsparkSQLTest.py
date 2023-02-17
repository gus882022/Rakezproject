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
import unittest


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
            # store table_schemas dictionary in a variable
            table_config = tables_schemas[table]

            # define how the table is partitioned
            partition = ",".join(table_config['partition'])

            # variable to identify the path from silver zone
            path_table_gold = f"{path_gold_datalake}/{table_config['path']}"

            # identifies if exists data in S3

            if file_exists(f"{path_table_gold}"):
                dbutils.fs.rm(f"{path_table_gold}",True)

            # variable to identify the function to create the table
            function = table_config['function']

            count_read_regs = function(db_source=database_source,db_target=database_target,table_name=table,path=path_table_gold,partition_by=partition)

            # save in the log list 
            query=f"select count(*) as regs from {database_target}.{table}"

            count_write_regs = spark.sql(query).collect()[0]["regs"]         

            log_file.append([table,"Table Sucessfull Loaded",count_write_regs,count_read_regs])
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
# MAGIC ### Class main test

# COMMAND ----------

class test_main_gold(unittest.TestCase):
    
    def test_main_raw_gold_full(self)-> None:
        # store the log dataframe
        self.logdf = save_delta_table()
        
        result_data = self.logdf.select(col('Status')).collect()
        result_readed_rows = self.logdf.select(col('Rows_Readed')).collect()
        result_writed_rows = self.logdf.select(col('Rows_Loaded')).collect()
        
        result_data = [row["Status"] for row in result_data]
        result_readed_rows = [row["Rows_Readed"] for row in result_readed_rows]
        result_writed_rows = [row["Rows_Loaded"] for row in result_writed_rows]
        
        self.assertTrue(x for x in result_data if x == "Table Sucessfull Loaded")
        
        self.assertEqual(result_readed_rows,result_writed_rows)
        # convert pyspark dataframe to pandas df to send it as table on email
        self.log_df=convert_df_pandas(self.logdf)

        # variable for design email in HTML format
        html_content = f"""\
        <html>
          <head></head>
          <body>
            <p> Log for day {date} </p>
            {self.log_df.to_html()}
          </body>
        </html>
        """

        # send notification to the email
        send_notification(subject=f"SILVER TO GOLD ZONE FULL LOAD -> LOG FOR DAY {date} ",msg=html_content)

# COMMAND ----------

suite = unittest.TestSuite()
suite.addTest(test_main_gold('test_main_raw_gold_full')) 
unittest.TextTestRunner().run(suite)

# COMMAND ----------


