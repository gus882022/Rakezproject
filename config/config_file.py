from pyspark.sql.types import *
from pyspark.sql import SparkSession
import time
import json
import boto3
import subprocess
import sys
from datetime import date
from pyspark.dbutils import DBUtils
import pandas
from pyspark.sql.functions import col
import datetime

##############################################################################################FUNCTIONS FULL LOAD########################################################################################################


# Method for validating if a file exists
def file_exists(path)-> BooleanType:
  try:
    dbutils.fs.ls(path)
    return True
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      return False
    else:
      raise

# Convert Dataframe pyspark to pandas
def convert_df_pandas(df):
    df_pandas=df.toPandas()
    return df_pandas

# UDF for send notifications
def send_notification(subject,msg):
    region = aws_parameters['region']
    charset = "UTF-8"
    ses_client = boto3.client("ses", region_name=aws_parameters['region'])
    html_content = msg
    response = ses_client.send_email(
        Destination={
            "ToAddresses": [
                "gus882003@gmail.com",
            ],
        },
        Message={
            "Body": {
                "Html": {
                    "Charset": charset,
                    "Data": html_content,
                }
            },
            "Subject": {
                "Charset": charset,
                "Data": subject,
            },
        },
        Source="gus882003@gmail.com",
    )
    
# UDF for calculate candidates by campaigns
def candidate_by_campaign(db_source,db_target,table_name,path,partition_by)->IntegerType:
    
    sql = f"drop table if exists {db_target}.{table_name}"
    spark.sql(sql)
    if partition_by != "":
        sql= f""" create table {db_target}.{table_name}
                using delta
                location '{path}'   
                partitioned by ({partition_by})
                as 
                select t1.Id as LeadId, 
                       t3.Id as CampaignId,
                       t1.Name as Candidate, 
                       t1.Company, 
                       t2.Status, 
                       t3.Name as Campaign, 
                       t3.StartDate  as StartDateCampaign, 
                       t3.EndDate as EndDateCampaign
                from {db_source}.candidate t1
                  inner join {db_source}.candidate_campaign t2 
                              on t1.Id=t2.LeadId
                  inner join {db_source}.campaign t3 
                              on t2.CampaignId=t3.Id"""
    else:
        sql= f""" create table {db_target}.{table_name}
                using delta
                location '{path}'   
                as  
                select t1.Id as LeadId,
                       t3.Id as CampaignId,
                       t1.Name as Candidate, 
                       t1.Company, 
                       t2.Status, 
                       t3.Name as Campaign, 
                       t3.StartDate  as StartDateCampaign, 
                       t3.EndDate as EndDateCampaign
                from {db_source}.candidate t1
                  inner join {db_source}.candidate_campaign t2 
                              on t1.Id=t2.LeadId
                  inner join {db_source}.campaign t3 
                              on t2.CampaignId=t3.Id"""
    spark.sql(sql)
    sql =f"alter table {db_target}.{table_name} set tblproperties (delta.enableChangeDataFeed = true)"
    spark.sql(sql)
    sql= f""" select count(*) as regs
                from {db_source}.candidate t1
                  inner join {db_source}.candidate_campaign t2 
                              on t1.Id=t2.LeadId
                  inner join {db_source}.campaign t3 
                              on t2.CampaignId=t3.Id"""
    count_read_regs = spark.sql(sql).collect()[0]["regs"]
    return count_read_regs

# UDF for calculate status by campaigns
def status_by_campaign(db_source,db_target,table_name,path,partition_by)->IntegerType:
    
    sql = f"drop table if exists {db_target}.{table_name}"
    spark.sql(sql)
    if partition_by != "":
        sql= f""" create table {db_target}.{table_name}
                using delta
                location '{path}'   
                partitioned by ({partition_by})
                as 
                select status,count(*) cnt_status_campaign
                from {db_source}.campaign
                group by status"""
    else:
        sql= f""" create table {db_target}.{table_name}
                using delta
                location '{path}'   
                as 
                select status,count(*) cnt_status_campaign
                from {db_source}.campaign
                group by status"""
        
    spark.sql(sql)
    sql =f"alter table {db_target}.{table_name} set tblproperties (delta.enableChangeDataFeed = true)"
    spark.sql(sql)
    sql= f""" select count(*) as regs
                from (select status,count(*) cnt_status_campaign
                      from {db_source}.campaign
                       group by status) t1"""
    count_read_regs = spark.sql(sql).collect()[0]["regs"]
    return count_read_regs


# UDF for creating table into the database
def create_table(db,table_name,path_table,partition,schema) -> IntegerType:
    # drop the table if exists into database
    spark.sql(f"drop table if exists {db}.{table_name}")
    # identifies when a table has partitions

    if partition != "":
        query=f"""
                    create table if not exists {db}.{table_name}
                    using delta
                    location '{path_table}'
                    partitioned by ({partition})
                    as
                    {schema}"""
    else:
        query=f"""
                    create table if not exists {db}.{table_name}
                    using delta
                    location '{path_table}'
                    as
                    {schema}"""
    spark.sql(query)
    query =f"alter table {db}.{table_name} set tblproperties (delta.enableChangeDataFeed = true)"
    spark.sql(query)
    query=f"select count(*) as regs from {db}.{table_name}"
    count_write_regs = spark.sql(query).collect()[0]["regs"]
    return count_write_regs


#########################################################################################FUNCTIONS INCREMENTAL LOAD ############################################################################################

def merge_operation_data(db,table_name,condition,operation) -> ArrayType:
    
    rows_inserted = 0
    rows_updated = 0
    rows_deleted = 0
    rows = []
        
    if operation =='FULL':
        
        query=f"""merge into {db}.{table_name} target 
                  using {table_name} source on {condition}
                  when matched then delete """
    
        regs_affected=spark.sql(query)
    
        rows_deleted=regs_affected.select(col('num_deleted_rows')).collect()[0]["num_deleted_rows"]
    
    else:
        
        query=f"""merge into {db}.{table_name} target 
                      using {table_name} source on {condition}
                      when matched and source.IsDeleted = True
                      then delete"""
     
        regs_affected=spark.sql(query)

        rows_deleted=regs_affected.select(col('num_deleted_rows')).collect()[0]["num_deleted_rows"]

        
    query=f"""merge into {db}.{table_name} target 
                  using {table_name} source on {condition}
                  when matched then update set *
                  when not matched and source.IsDeleted = False then insert * """    

 
    regs_affected=spark.sql(query)

    rows_updated = regs_affected.select(col('num_updated_rows')).collect()[0]["num_updated_rows"]
    rows_inserted = regs_affected.select(col('num_inserted_rows')).collect()[0]["num_inserted_rows"]
    
    rows.append(rows_inserted)
    rows.append(rows_updated)
    rows.append(rows_deleted)
    
    
    return rows

def move_files_processed(path_execution,path_data,table_name) -> None:
    list_files=dbutils.fs.ls(path_execution)
    today = datetime.datetime.now()
    year = today.strftime("%Y")
    month = today.strftime("%m")
    day = today.strftime("%d") 
    hour = today.strftime("%H") 
    minutes = today.strftime("%M") 
    for file in list_files:
        dbutils.fs.cp(file[0],f"{path_data}/{table_name}/Processed/{year}/{month}/{day}/{hour}/{minutes}/{file[1]}",True)
    print(path_execution)
    
# UDF for calculate CDC status by campaigns
def status_by_campaign_cdc(db_source,db_target,table_target,condition_table,operation_table)->ArrayType:
    
    sql= f"""   create or replace temporary view {table_target}
                as
                select status,count(*) cnt_status_campaign,False as IsDeleted
                from {db_source}.campaign
                group by status,True"""
    
    spark.sql(sql)
    
    rows = merge_operation_data(db=db_target,table_name=table_target,condition=condition_table,operation=operation_table)
    
    return rows
    
# UDF for calculate CDC status by campaigns
def candidate_by_campaign_cdc(db_source,db_target,table_target,condition_table,operation_table)->ArrayType:
    
    sql= f"""   create or replace temporary view {table_target}
                as
                select t1.Id as LeadId, 
                       t3.Id as CampaignId,
                       t1.Name as Candidate, 
                       t1.Company, 
                       t2.Status, 
                       t3.Name as Campaign, 
                       t3.StartDate  as StartDateCampaign, 
                       t3.EndDate as EndDateCampaign,
                       False as IsDeleted                       
                from {db_source}.candidate t1
                  inner join {db_source}.candidate_campaign t2 
                              on t1.Id=t2.LeadId
                  inner join {db_source}.campaign t3 
                              on t2.CampaignId=t3.Id"""
    
    spark.sql(sql)
    
    rows = merge_operation_data(db=db_target,table_name=table_target,condition=condition_table,operation=operation_table)
    
    return rows
        
    
    

##############################################################################################PARAMETERS########################################################################################################
# table_log_file
log_file = []

# create spark session
spark = SparkSession.builder.appName('config').getOrCreate()
dbutils = DBUtils(spark)

# date data process 
date_process={
                'today': date.today()
             }
# define what are the databases to storage the data
databases={
            'raw' : 'raw_data_rakez',
            'silver' : 'silver_data_rakez',
            'gold' : 'gold_data_rakez'
          }
# Dictionary of paths from datalake in point mount
paths_datalake={
                    'raw_bucket':'s3a://demodmsgbraw',
                    'silver_bucket':'s3a://demodmsgbsilver',
                    'gold_bucket':'s3a://demodmsgbgold',
                    'raw_bucket_cdc':'s3a://demodmsgbraw/cdc',
                    'name_raw_bucket':'demodmsgbraw'
               }


# Dictionary of parameters to AWS
aws_parameters={
                    'topic_sns':'arn:aws:sns:us-west-1:393747608406:snsnotificationsms',
                    'region': 'us-west-1'
               }

# Path to read DQ rules file
path_file_rules={
                    'path': 's3://demodmsgbraw/libraries/Account_business_rules_input.xlsx',
                    'sheet_name':'example'
                }

# Dictionary for table schema
tables_schema_config = {
                          'Tables': {
                                        'candidate': {
                                                        'fields':{'Id':"String",
                                                                  'Name': "String",
                                                                  'Company': "String",
                                                                  'State': "String",
                                                                  'Email': "String",
                                                                  'Status': "String",
                                                                  'CreatedDate': "Timestamp"
                                                                 },
                                                        'path':'candidate/',
                                                        'path_cdc':'candidate-cdc/',
                                                        'partition':[''],
                                                        'primary_key':['Id'],
                                                        'type':'CDC'
                                                   },
                                        'account': {
                                                        'fields':{'Id':"String",
                                                                  'Name': "String",
                                                                  'Site': "String",
                                                                  'Phone': "String",
                                                                  'Type': "String",
                                                                  'BillingState': "String"
                                                                 },
                                                        'path':'account/',
                                                        'path_cdc':'account-cdc/',                                            
                                                        'partition':[''],
                                                        'primary_key':['Id'],
                                                        'type':'FULL'                                            
                                                        
                                                   },
                                       'campaign': {
                                                        'fields':{'Id':"String",
                                                                  'Name': "String",
                                                                  'Type': "String",
                                                                  'StartDate': "Date",
                                                                  'EndDate': "Date",
                                                                  'Status': "String",
                                                                  'ActualCost': "String",
                                                                  'BudgetedCost': "String"
                                                                 },
                                                        'path':'Campaign/',
                                                        'partition':[''],
                                                        'path_cdc':'Campaign-cdc/',                                           
                                                        'primary_key':['Id'],
                                                        'type':'CDC'                                           
                                                   },
                                       'candidate_campaign': {
                                                        'fields':{'LeadId':"String",
                                                                  'CampaignId': "String",
                                                                  'Status': "String",
                                                                  'CreatedDate': "Date"
                                                                 },
                                                        'path':'candidate_campaign/',
                                                        'path_cdc':'candidate_campaign-cdc/',                                                                                      
                                                        'partition':[''],
                                                        'primary_key':['LeadId','CampaignId'],
                                                        'type':'CDC'                                           
                                                   }
                                    }
                        }

# Dictionary for table with aggregrations
tables_aggregated = {
                        'Tables':{
                                        'candidate_by_campaign': {
                                                                    'function': candidate_by_campaign,
                                                                    'function_cdc': candidate_by_campaign_cdc,
                                                                    'path':'candidate_by_campaign/',
                                                                    'partition':[''],
                                                                    'primary_key':['LeadId','CampaignId'],
                                                                    'type':'CDC'                                            
                                                                 },
                                        'status_by_campaign':    {
                                                                    'function': status_by_campaign,
                                                                    'function_cdc': status_by_campaign_cdc,
                                                                    'path':'status_by_campaign/',
                                                                    'partition':[''],
                                                                    'primary_key':['status'],
                                                                    'type':'CDC'                                                                      
                                                                 }
                                  }
                    }



