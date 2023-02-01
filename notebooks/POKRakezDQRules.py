# Databricks notebook source
# MAGIC %md
# MAGIC ### Autoreload config files

# COMMAND ----------

# MAGIC %reload_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Libraries

# COMMAND ----------

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
from config.config_file import *

import pandas as pd
from pandas import ExcelFile

# COMMAND ----------

# MAGIC %md
# MAGIC ### Declaration of variables

# COMMAND ----------

path_raw_datalake = paths_datalake['raw_bucket']
path_silver_datalake = paths_datalake['silver_bucket']
date = date_process['today']
database = databases['silver']

# COMMAND ----------

# MAGIC %md
# MAGIC ### Function to read DQ rules into excel file

# COMMAND ----------

def read_rules_from_excel_file(table_name,df,check):
    path_excel_file= path_file_rules['path']
    sheet_name=path_file_rules['sheet_name']
    pdf = pd.read_excel(path_excel_file,sheet_name =sheet_name)  #/dbfs/FileStore/tables/Business_rules.xlsx
    schema_file_excel= StructType([
                       StructField("Table Name", StringType(), True),
                       StructField("Attribute Name", StringType(), True),
                       StructField("Rule Definition", StringType(), True),
                       StructField("Domain", StringType(), True),
                       StructField("DQ Dimension", StringType(), True),    
                       StructField("Key", IntegerType(), True),        
                       StructField("Rule Function", StringType(), True),            
                       StructField("Condition", StringType(), True),
                       StructField("Value", IntegerType(), True),
                       StructField("Pattern", StringType(), True),
                    ])
    df_rules = spark.createDataFrame(pdf,schema_file_excel)
    df_rules = df_rules.select('*').where(col("Table Name")==table_name)
    count_rules= df_rules.count()
    if count_rules>0:
        rules_table=df_rules.collect()
        for rule in rules_table:
            rule_condition =rule['Condition']
            rule_function= rule['Rule Function']
            rule_field = str(rule['Attribute Name']).strip()
            value_rule = rule['Value']
            pattern_value = rule['Pattern']
#            print(rule_field,rule_function)
            if rule_condition == None:
                check.addConstraint(getattr(check,rule_function )(rule_field))
            elif rule_condition[0]=="[":
                rule_condition_1=rule_condition.replace("[","")
                rule_condition_2=rule_condition_1.replace("]","")
                rule_condition=list(rule_condition_2.split(','))
                check.addConstraint(getattr(check, rule_function)(rule_field,rule_condition))
            elif rule_function == 'hasMaxLength':
                check.addConstraint(getattr(check, rule_function)(rule_field,eval(rule_condition)))
            elif pattern_value != None:
                check.addConstraint(getattr(check, rule_function)(rule_condition,rule_field,eval(f"lambda x: x == {value_rule}")))
            elif rule_condition == 'Boolean':
                check.addConstraint(getattr(check, rule_function)(rule_field,ConstrainableDataTypes.Boolean))
            else:
                check.addConstraint(getattr(check, rule_function)(rule_field,rule_condition))
        
        checkResult = VerificationSuite(spark) \
        .onData(df) \
        .addCheck(check) \
        .run()
        checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
        checkResult_df = checkResult_df.withColumn("row_num", row_number().over(Window.orderBy(monotonically_increasing_id())))
#        w = Window().orderBy(lit('constraint'))
#        checkResult_df = checkResult_df.withColumn("row_num", row_number().over(w))
#        print("Row Number")
#        display(checkResult_df)
        checkResult_df = checkResult_df.select('*',lit(table_name).alias('table_name'))
        total_rows = df.count()
        checkResult_df = checkResult_df.select('*',lit(total_rows).alias('total_rows'))
        checkResult_df = checkResult_df.join(df_rules,checkResult_df.row_num == df_rules.Key,"inner")
#        print("Final DF")
#        display(checkResult_df)
        return(checkResult_df)    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Function to add new columns to dataframe results from DQ rules

# COMMAND ----------

def add_new_columns(complete_df_dq):
    today = date.today()
    d1 = today.strftime("%Y-%m-%d")
    # path_rules_results=f"s3://rakez-silver-eu-ido-842624552760-prod/{d1}/{d1}.csv"
    checkResult_df_1 = complete_df_dq.toPandas()
    regex=r"[-+]?[0-9]*\.?[0-9]+(?:[eE][-+]?[0-9]+)?"
    checkResult_df_1['Pass %']= (pd.DataFrame(checkResult_df_1['Constraint Message'].apply(lambda x: [float(i) for i in re.findall(regex, x)]).to_list())*100).fillna(100)
    checkResult_df_1['Fail %']= ((1-pd.DataFrame(checkResult_df_1['Constraint Message'].apply(lambda x: [float(i) for i in re.findall(regex, x)]).to_list()))*100).fillna(0)
    checkResult_df_1['Failure Count'] = checkResult_df_1['Fail %']*(checkResult_df_1['Total Rows']/100)
    checkResult_df_1['Pass Count'] = checkResult_df_1['Pass %']*(checkResult_df_1['Total Rows']/100)
    df_rules = spark.createDataFrame(checkResult_df_1)
    
    df_check=df_rules.select(col("Table Name")
                             ,col("Attribute Name")
                             ,col("Rule Definition")
                             ,col("DQ Dimension")
                             ,col("Constraint Status")
                             ,col("Domain")
                             ,col("Total Rows")
                             ,round(col("Pass Count"),2).alias("Pass Count")
                             ,round(col("Failure Count"),2).alias("Failure Count")
                             ,round(col("Pass %"),2).alias("Pass %")
                             ,round(col("Fail %"),2).alias("Fail %")
                             ,when(df_rules['Fail %'] > 50,"High Impact")
                                  .when((df_rules['Fail %'] <= 50) & (df_rules['Fail %']> 0),"Low Impact")
                                  .when(df_rules['Pass %'] == 100,"No Impact").alias('Impact')
                             ,col("Constraint Message"))
    checkResult_df_1=df_check.toPandas()

    today = date.today()
    d1 = today.strftime("%d/%m/%Y")
    print("Today's date:", d1)
    checkResult_df_1['Date'] = d1
    #display(checkResult_df_1)
    
    html_content = f"""\
                            <html>
                              <head></head>
                              <body>
                                <p> Log for day {date} </p>
                                {checkResult_df_1.to_html()}
                              </body>
                            </html>"""
    
    # send notification to the email
    send_notification(subject=f"RAW TO SILVER DQ RULES EXECUTED -> LOG FOR DAY {date} ",msg=html_content)
    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Functions main to DQ Rules

# COMMAND ----------

def main_dq_rules():
    schema = StructType([
    StructField('Table Name', StringType(), True),
    StructField('Attribute Name', StringType(), True),
    StructField('Rule Definition', StringType(), True),
    StructField('Domain', StringType(), True),
    StructField('DQ Dimension', StringType(), True),
    StructField('Constraint Status', StringType(), True),
    StructField('Constraint Message', StringType(), True),
    StructField('Row Num', StringType(), True),
    StructField('Total Rows', IntegerType(), True)
    ])
    emp_RDD = spark.sparkContext.emptyRDD()
    complete_df = spark.createDataFrame(emp_RDD,schema)
    table_schema = tables_schema_config 
    tables_list = table_schema['Tables']
    for table_name in tables_list:
        query=f"select * from {database}.{table_name}"
        df= spark.sql(query)
        check=None
        check = Check(spark, CheckLevel.Warning, f"{table_name}  Checks")
        checkResult_df=read_rules_from_excel_file(table_name,df,check)
        if checkResult_df != None:
            complete_df=complete_df.union(checkResult_df.select(['Table Name','Attribute Name','Rule Definition','Domain','DQ Dimension','constraint_status','constraint_message','row_num','total_rows'])) 
    add_new_columns(complete_df)

# COMMAND ----------

main_dq_rules()

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from silver_data_rakez.campaign

# COMMAND ----------


