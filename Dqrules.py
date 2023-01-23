# Databricks notebook source
# MAGIC %md
# MAGIC ### Generate autoreload from config files

# COMMAND ----------

# MAGIC %reload_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import libraries

# COMMAND ----------

from config.config_file import *
from config.tables_config import *
from pydeequ import *
from pydeequ.profiles import *
from pydeequ.analyzers import *
from pydeequ.suggestions import *
from pydeequ.checks import *
from pydeequ.verification import *
from pyspark.sql.functions import row_number,lit
from pyspark.sql.window import Window
from datetime import date
from pyspark.sql.functions import *
from functools import reduce
from pyspark.sql import DataFrame
import re

import pandas as pd
from pandas import ExcelFile

# COMMAND ----------

# MAGIC %md
# MAGIC ### Variables

# COMMAND ----------

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

# MAGIC %md
# MAGIC ### Function for apply rules over the table when you have a config file .py

# COMMAND ----------

def read_rules_from_config_file():
    tables_config=tables_schema_config['schema']
    tables=tables_config['sales']
    for table in tables.keys():
        if table=='stores':
            rules_tables=tables[table]['rules_table']
            for field in rules_tables:
                rules_columns=rules_tables[field]
                for rule in rules_columns:
                    assertion=rules_columns[rule]
                    if rule != assertion:
                        check.addConstraint(getattr(check, rule)(field,assertion))
                    else:
                        check.addConstraint(getattr(check, rule)(field))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Function to read a config file in Excel Format storage in S3 Bucket

# COMMAND ----------

def read_rules_from_excel_file(table_name,df):
    path_excel_file=path_file_rules['path']
    sheet_name=path_file_rules['sheet_name']
    pdf = pd.read_excel(path_excel_file,sheet_name =sheet_name)
    schema_file_excel= StructType([
                       StructField("Table_Name", StringType(), True),
                       StructField("Attribute_Name", StringType(), True),
                       StructField("Rule_Definition", StringType(), True),
                       StructField("Domain", StringType(), True),
                       StructField("DQ_Dimension", StringType(), True),    
                       StructField("key", IntegerType(), True),        
                       StructField("rule_function", StringType(), True),            
                       StructField("condition", StringType(), True),
                       StructField("value", FloatType(), True),
                       StructField("pattern", StringType(), True),
                    ])
    df_rules = spark.createDataFrame(pdf,schema_file_excel)
    df_rules = df_rules.select('*').where(col("Table_Name")==table_name)
    count_rules= df_rules.count()
    if count_rules>0:
        rules_table=df_rules.collect()
        for rule in rules_table:
            rule_condition = rule['condition']
            rule_function= rule['rule_function']
            rule_field = rule['Attribute_Name']
            value_rule = rule['value']
            pattern_value= rule['pattern']
            if rule_condition == None:
                check.addConstraint(getattr(check,rule_function )(rule_field))
            elif rule_condition[0]=="[":
                rule_condition_1=rule_condition.replace("[","")
                rule_condition_2=rule_condition_1.replace("]","")
                rule_condition=list(rule_condition_2.split(','))
                check.addConstraint(getattr(check, rule_function)(rule_field,rule_condition))
            elif rule_condition.startswith('lambda'):
                check.addConstraint(getattr(check, rule_function)(rule_field,lambda x: x == value_rule))
            elif pattern_value != None:
                check.addConstraint(getattr(check, rule_function)(rule_condition,rule_field,lambda x: x == value_rule))
            elif rule_condition == 'Boolean':
                check.addConstraint(getattr(check, rule_function)(rule_field,ConstrainableDataTypes.Boolean))
            else:
                check.addConstraint(getattr(check, rule_function)(rule_field,rule_condition))
                
        checkResult = VerificationSuite(spark) \
        .onData(df) \
        .addCheck(check) \
        .run()
        checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)

        w = Window().orderBy(lit('constraint'))
        checkResult_df = checkResult_df.withColumn("row_num", row_number().over(w))
        checkResult_df = checkResult_df.select('*',lit(table_name).alias('table_name'))
        return(checkResult_df)

# COMMAND ----------


schemas_config=table_schema['schema']
schema = StructType([
  StructField('check', StringType(), True),
  StructField('check_level', StringType(), True),
  StructField('check_status', StringType(), True),
  StructField('constraint', StringType(), True),
  StructField('constraint_status', StringType(), True),
  StructField('constraint_message', StringType(), True),
  StructField('row_num', IntegerType(), True),
  StructField('table_name', StringType(), True)
  ])
emp_RDD = spark.sparkContext.emptyRDD()
complete_df = spark.createDataFrame(emp_RDD,schema)
for schema in schemas_config.keys():
    list_tables=schemas_config[schema]
    list_path_tables_crawler=[]
    for table in list_tables.keys():
        table_name = table
        print(table_name)
        path_delta_table=f"{path_raw_datalake}{schema}/{table_name}/"
        df=spark.read.format("delta").load(path_delta_table)
        check = Check(spark, CheckLevel.Warning, "Account  Checks")
        checkResult_df=read_rules_from_excel_file(table_name,df)
        if checkResult_df != None:
            complete_df=complete_df.union(checkResult_df)

display(complete_df)
  

# COMMAND ----------

today = date.today()
d1 = today.strftime("%Y-%m-%d")
dbutils.fs.mkdirs(f"dbfs:{path_raw_datalake}result_quality_rules/{d1}/")
path_rules_results=f"/dbfs{path_raw_datalake}result_quality_rules/{d1}/{d1}.csv"


checkResult_df_1 = complete_df.toPandas()
regex=r"[-+]?[0-9]*\.?[0-9]+(?:[eE][-+]?[0-9]+)?"
checkResult_df_1['Pass %']= (pd.DataFrame(checkResult_df_1['constraint_message'].apply(lambda x: [float(i) for i in re.findall(regex, x)]).to_list())*100).fillna(100)
checkResult_df_1['Fail %']= ((1-pd.DataFrame(checkResult_df_1['constraint_message'].apply(lambda x: [float(i) for i in re.findall(regex, x)]).to_list()))*100).fillna(0)
rows = df.count()
print(f"DataFrame Rows count : {rows}")
#cols = len(df.columns)
#print(f"DataFrame Columns count : {cols}")
#TotalCount = (rows * cols)
#print(f"Total Count : {TotalCount}")
checkResult_df_1['Total Rows'] = rows
checkResult_df_1['Failure Count'] = (checkResult_df_1['Fail %']*(rows/100))
checkResult_df_1['Pass Count'] = (checkResult_df_1['Pass %']*(rows/100))

df_rules = spark.createDataFrame(checkResult_df_1)

df_check=df_rules.select(col("*"),when(df_rules['Fail %'] >= 50,"High Impact")
                              .when(df_rules['Fail %'] <= 50,"Low Impact")
                              .when(df_rules['Fail %'] == 50,"No Impact").alias('Impact'))
    
checkResult_df_1=df_check.toPandas()

today = date.today()
d1 = today.strftime("%d/%m/%Y")
print("Today's date:", d1)
checkResult_df_1['Date'] = d1

display(checkResult_df_1)

checkResult_df_1.to_csv(path_rules_results,index=False,sep='|',header=True)

#complete_df.write.mode("overwrite").csv(path_rules_results,sep="|",header=True)

# COMMAND ----------


