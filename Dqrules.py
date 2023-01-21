# Databricks notebook source
# MAGIC %reload_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from config.config_file import *
from config.tables_config import *
from pydeequ import *
from pydeequ.profiles import *
from pydeequ.analyzers import *
from pydeequ.suggestions import *
from pydeequ.checks import *
from pydeequ.verification import *

# COMMAND ----------

df=spark.sql("select * from raw_data_rakez.sales_stores")

# COMMAND ----------

from pydeequ.checks import *
from pydeequ.verification import *
from pyspark.sql.functions import row_number,lit
from pyspark.sql.window import Window
from datetime import date


check = Check(spark, CheckLevel.Warning, "Account  Checks")

tables_config=tables_schema_config['schema']
tables=tables_config['sales']
for table in tables.keys():
    if table=='stores':
        rules_tables=tables[table]['rules_table']
        for field in rules_tables:
            print(field)
            for rule in rules_tables[field]:
                check.addConstraint(getattr(check, rule)(field))
            
            
    

    







# COMMAND ----------

checkResult = VerificationSuite(spark) \
    .onData(df) \
    .addCheck(check) \
    .run()

print(type(checkResult))
print(checkResult)
checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)

w = Window().orderBy(lit('constraint'))
checkResult_df = checkResult_df.withColumn("row_num", row_number().over(w))
checkResult_df.display()

# COMMAND ----------


