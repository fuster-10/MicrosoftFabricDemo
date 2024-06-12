#  Demo_Fabric_Consumption
#
# Capa Consumption - Procesamiento de datos
# 


from pyspark.sql.types import *
from pyspark.sql.functions import concat, lit, datediff, current_date, when, col, substring, sum, length, countDistinct

import sys
import os
#import Constant
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


if __name__ == '__main__':

    #Spark session builder
    spark = (SparkSession
          .builder
          .appName("example") 
          .config("spark.some.config.option", "some-value")
          .getOrCreate())
    
    spark_context = spark.sparkContext
    spark_context.setLogLevel("DEBUG")

    df_fact_summary = spark.read.format("parquet").load("Files/CURATED/FactInternetSales.parquet")
    
    df_fact_summary = df_fact_summary.withColumn(
        'OrderYear', substring(df_fact_summary['OrderDateKey'],0,4).cast('integer')
    ).withColumn(
        'OrderMonth', substring(df_fact_summary['OrderDateKey'],5,2).cast('integer')
    )

    df_fact_summary = df_fact_summary.groupBy('ProductKey', 'CustomerKey', 'SalesTerritoryKey', 'OrderYear', 'OrderMonth') \
        .agg(sum('TotalSales').alias('TotalSalesAmount'), sum('OrderQuantity').alias('TotalItems'), countDistinct('SalesOrderNumber').alias('TotalOrders'))

    df_fact_summary = df_fact_summary.withColumn(
        'DateKey', when(length(df_fact_summary['OrderMonth'].cast('string'))==2,concat(df_fact_summary['OrderYear'].cast('string'),df_fact_summary['OrderMonth'].cast('string'),lit('01'))).otherwise(concat(df_fact_summary['OrderYear'].cast('string'),lit('0'),df_fact_summary['OrderMonth'].cast('string'),lit('01'))).cast('integer')
    )

    df_fact_summary.write.mode("overwrite").parquet('Files/CONSUMPTION/FactSalesSummary.parquet')
    
    #### FactInternetSales
    df_fact_internet_sales = spark.read.format("parquet").load("Files/CURATED/FactInternetSales.parquet")

    df_fact_internet_sales.write.mode("overwrite").parquet('Files/CONSUMPTION/FactInternetSales.parquet')
    
    
    #### DimCustomer
    df_dim_customer = spark.read.format("parquet").load("Files/CURATED/DimCustomer.parquet")

    df_dim_customer.write.mode("overwrite").parquet('Files/CONSUMPTION/DimCustomer.parquet')
    
    #### DimDate
    df_dim_date = spark.read.format("parquet").load("Files/CURATED/DimDate.parquet")

    df_dim_date.write.mode("overwrite").parquet('Files/CONSUMPTION/DimDate.parquet')
    
    #### DimProduct
    df_dim_product = spark.read.format("parquet").load("Files/CURATED/DimProduct.parquet")

    df_dim_product.write.mode("overwrite").parquet('Files/CONSUMPTION/DimProduct.parquet')
    
    #### DimSalesTerritory
    df_dim_sales_territory = spark.read.format("parquet").load("Files/CURATED/DimSalesTerritory.parquet")

    df_dim_sales_territory.write.mode("overwrite").parquet('Files/CONSUMPTION/DimSalesTerritory.parquet')
