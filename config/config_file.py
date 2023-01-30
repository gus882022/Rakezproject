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
                    'path': '/dbfs/mnt/datalake/libraries_bucket/Account_business_rules_input.xlsx',
                    'sheet_name':'example'
                }

# Dictionary for table schema
tables_schema_config = {
                          'Tables': {
                                      'candidate': {
                                                        'fields':{'Id':StringType(),
                                                                  'Name': StringType(),
                                                                  'Company': StringType(),
                                                                  'State': StringType(),
                                                                  'Email': StringType(),
                                                                  'Status': StringType(),
                                                                  'CreatedDate':TimestampType()
                                                                 },
                                                        'path':'candidate/',
                                                        'partition':[''],
                                                        'primary_key':['Id','Name']
                                                   }
                                    }
                        }

##############################################################################################FUNCTIONS########################################################################################################


# Method for validating if a file exists
def file_exists(path):
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

