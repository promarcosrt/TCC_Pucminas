import sys
import boto3
import logging
import pandas as pd
from datetime import date, timedelta,datetime

import pyspark.sql.functions as F
from pyspark.sql.functions import udf
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.job import Job
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

# Setting Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

path_estemp = "s3://opendatasus.puc.structured/tempo"

def datas():
    # Criando uma lista com as datas
    start_date = datetime(2021, 1, 1)
    end_date = datetime(2023, 2, 1)
    delta = end_date - start_date
    dates = [start_date + timedelta(days=i) for i in range(delta.days + 1)]
    
    df = spark.createDataFrame(dates, TimestampType()).select(col("value").alias("data"))

    return df

def sem_epidemiologica(dt):
    # retorna o anosemana de semana epidemiologica iniciadas no domingo a sabado
    semana = dt.isocalendar()[1]
    if dt.weekday() == 6:
        semana += 1

    return semana

def dm_data(df,path_estemp):
    
    sem_epidemiologica_udf = udf(sem_epidemiologica, IntegerType())
    
    try:
        
        df.createOrReplaceTempView("df")
        
        print("Inicio dimensao data")
        
        df = spark.sql("""
        SELECT distinct
            data
            ,date_format(data,'dd') as dia
            ,date_format(data,'MM') as mes
            ,date_format(data,'yyyy') as ano
            ,trunc(data,'month') as anomes
        FROM df
        """)

        df = df.withColumn("semana",lpad(weekofyear(col("data")),2,'0'))
        df = df.withColumn("semana_epi",lpad(sem_epidemiologica_udf(col("data")),2,'0'))
        df = df.withColumn("anosemana_epi",concat(col("ano"),col("semana_epi")))
        
        df.write.mode("overwrite").format("parquet").save(path_estemp+"/dm_data")

        print("dimensao data atualizada!")
        
    except Exception as e:
        print(f"ERROR: {e}") 
    
def main():
    
    df = datas()    
    dm_data(df,path_estemp)
    
if __name__ == "__main__":
    main()
