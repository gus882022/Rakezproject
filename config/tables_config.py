# Databricks notebook source
# DBTITLE 1,Import methods from other notebooks
# MAGIC %run "./library"

# COMMAND ----------

# DBTITLE 1,Import sql types function
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from delta import *
import boto3

# DBTITLE 1,Define parameters from data
databases={
    'raw' : 'raw_data_rakez',
    'silver' : 'silver_data_rakez',
    'gold' : 'gold_data_rakez'
}

# COMMAND ----------

# COMMAND ----------

# DBTITLE 1,Tables Schema

tables_schema_config = {
                        'schema':{
                            'sales':{
                                      'stores': 
                                             {
                                                 'fields':
                                                             {
                                                                'store_id'  :IntegerType(),
                                                                'store_name':StringType(),
                                                                'phone'     :StringType(),
                                                                'email'     :StringType(),
                                                                'street'    :StringType(),
                                                                'city'      :StringType(),
                                                                'state'     :StringType(),
                                                                'zip_code'  :StringType()
                                                             },
                                                 'path_table':
                                                             {
                                                                'path_sales_stores' :'/sales/stores/'
                                                             },
                                                 'rules_table':
                                                             {
                                                                 'store_id' : {
                                                                                 'isComplete':True,
                                                                                 'isNonNegative':True
                                                                              },
                                                                 'store_name' : {
                                                                                 'isComplete':True,
                                                                                 'isNonNegative':True
                                                                              }
                                                             }
                                            },
                                      'staffs': 
                                             {
                                                 'fields':
                                                             {
                                                                'staff_id'  :IntegerType(),
                                                                'first_name':StringType(),
                                                                'last_name' :StringType(),
                                                                'email'     :StringType(),
                                                                'phone'     :StringType(),
                                                                'active'    :IntegerType(),
                                                                'store_id'     :IntegerType(),
                                                                'manager_id'  :IntegerType()
                                                             },
                                                 'path_table':
                                                             {
                                                                'path_sales_staffs' :'/sales/staffs/'
                                                             }
                                            },
                                       'order_items': 
                                             {
                                                 'fields':
                                                             {
                                                                'order_id'  :IntegerType(),
                                                                'item_id':IntegerType(),
                                                                'product_id' :IntegerType(),
                                                                'quantity'     :IntegerType(),
                                                                'list_price'     :DecimalType(10,2),
                                                                'discount'    :DecimalType(4,2)
                                                             },
                                                 'path_table':
                                                             {
                                                                'path_sales_order_items' :'/sales/order_items/'
                                                             }
                                            },
                                       'orders': 
                                             {
                                                 'fields':
                                                             {
                                                                'order_id'  :IntegerType(),
                                                                'customer_id':IntegerType(),
                                                                'order_status' :IntegerType(),
                                                                'order_date'     :DateType(),
                                                                'required_date'     :DateType(),
                                                                'shipped_date'    :DateType(),
                                                                'store_id': IntegerType(),
                                                                'staff_id': IntegerType()
                                                                 
                                                             },
                                                 'path_table':
                                                             {
                                                                'path_sales_orders' :'/sales/orders/'
                                                             }
                                            },
                                       'customers': 
                                             {
                                                 'fields':
                                                             {
                                                                'customer_id'  :IntegerType(),
                                                                'first_name':StringType(),
                                                                'last_name' :StringType()
                                                             },
                                                 'path_table':
                                                             {
                                                                'path_sales_customers' :'/sales/customers/'
                                                             }
                                            }
                                    },
                            'production':{
                                      'brands': 
                                             {
                                                 'fields':
                                                             {
                                                                'brand_id'  :IntegerType(),
                                                                'brand_name':StringType()
                                                             },
                                                 'path_table':
                                                             {
                                                                'path_sales_stores' :'/production/brands/'
                                                             }
                                            }
                                        }
                                }
                       }

# COMMAND ----------

aggregate_queries = {
    'billing_for_each_customer':{'query': """ SELECT t3.first_name,t3.last_name,t3.email,sum(t2.quantity*t2.list_price) billing,sum(discount)discount
                                    FROM orders t1
                                            INNER JOIN order_items t2 ON t1.order_id=t2.order_id
                                            INNER JOIN customers t3 ON t1.customer_id=t3.customer_id
                                    GROUP BY t3.first_name,t3.last_name,t3.email
                                    ORDER BY t3.first_name,t3.last_name
                                    """,
                                 'tables_query':['sales.orders','sales.order_items','sales.customers'],
                                 'path':'users/billing_for_each_customer/',
                                 'schema':'users'
                                }
}

# COMMAND ----------

# DBTITLE 1,Method for saving table as delta table
def save_delta_table(df,path_delta,database,table_name):
    df.write.format("delta").mode("overwrite").option("path",path_delta).saveAsTable(f"{database}.{table_name}")
