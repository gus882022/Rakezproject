# Databricks notebook source
import pandas as pd

# COMMAND ----------

df = pd.read_csv('/Workspace/Repos/gus882003@gmail.com/Rakezproject/RDNA_BRKG_CAWEB_A_20220816.txt',header=None)

# COMMAND ----------

display(df)

# COMMAND ----------

df.to_excel('s3://demodmsgbraw/RDNA_BRKG_CAWEB_A_20220816.xlsx','sheet1')

# COMMAND ----------


