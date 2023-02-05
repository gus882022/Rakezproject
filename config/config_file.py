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

##############################################################################################FUNCTIONS########################################################################################################


# Method for validating if a file exists
def file_exists(path)-> BooleanType:
  try:
    dbutils = DBUtils(spark)
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
                "gustavo.barrera@evalueserve.com",
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
        Source="gustavo.barrera@evalueserve.com",
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
    
    sql= f""" select count(*) as regs
                from (select status,count(*) cnt_status_campaign
                      from {db_source}.campaign
                       group by status) t1"""
    count_read_regs = spark.sql(sql).collect()[0]["regs"]
    return count_read_regs

    
##############################################################################################PARAMETERS########################################################################################################
# table_log_file
log_file = []

# create spark session
spark = SparkSession.builder.appName('config').getOrCreate()

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
                    'gold_bucket':'s3a://demodmsgbgold'
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
                                                        'partition':[''],
                                                        'primary_key':['Id']
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
                                                        'partition':[''],
                                                        'primary_key':['Id']
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
                                                        'primary_key':['Id']
                                                   },
                                       'candidate_campaign': {
                                                        'fields':{'LeadId':"String",
                                                                  'CampaignId': "String",
                                                                  'Status': "String",
                                                                  'CreatedDate': "Date"
                                                                 },
                                                        'path':'candidate_campaign/',
                                                        'partition':[''],
                                                        'primary_key':['LeadId','CampaignId']
                                                   }
                                    }
                        }

# Dictionary for table with aggregrations
tables_aggregated = {
                        'Tables':{
                                        'candidate_by_campaign': {
                                                                    'function': candidate_by_campaign,
                                                                    'path':'candidate_by_campaign/',
                                                                    'partition':[''],
                                                                    'primary_key':['LeadId','CampaignId']
                                                                 },
                                        'status_by_campaign':    {
                                                                    'function': status_by_campaign,
                                                                    'path':'status_by_campaign/',
                                                                    'partition':[''],
                                                                    'primary_key':['status']
                                                                 }
                                  }
                    }



