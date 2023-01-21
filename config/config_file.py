# Databricks notebook source
# MAGIC %md
# MAGIC ## Import Libraries

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql import SparkSession
import time
import json
import boto3
import subprocess
import sys
from datetime import date

# COMMAND ----------
date_process={
    'today': date.today()
}

# DBTITLE 1,Dictionary of paths from datalake in point mount
paths_datalake={
    'staging_area':'/mnt/datalake/staging_area/',
    'raw_bucket':'/mnt/datalake/raw_bucket/',
    'silver_bucket':'/mnt/datalake/silver_bucket/',
    'gold_bucket':'/mnt/datalake/gold_bucket/',
    'libraries_bucket':'/mnt/datalake/libaries/'
}


# COMMAND ----------

# DBTITLE 1,Dictionary of paths from datalake in S3
paths_datalake_s3={
    'staging_area':'s3a://demodmsgbstaging/',
    'raw_bucket':'s3a://demodmsgbraw/',
    'silver_bucket':'s3a://demodmsgbsilver/',
    'gold_bucket':'s3a://demodmsgbgold/',
    'libraries_bucket':'s3a://demodmsgblibraries/'
}

aws_parameters={
    'topic_sns':'arn:aws:sns:us-west-1:393747608406:snsnotificationsms',
    'region': 'us-west-1'
}

# COMMAND ----------

def send_notification(subject,msg):
    region = aws_parameters['region']
    sns = boto3.client('sns',region)
    topic_sns = aws_parameters['topic_sns']
    response=sns.publish(
           TopicArn=topic_sns,
           Subject=subject,
           Message=msg
        )


