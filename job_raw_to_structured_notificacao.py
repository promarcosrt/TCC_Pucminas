import sys
import boto3
import logging
import pandas as pd

import pyspark.sql.functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
from awsglue.job import Job

from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from datetime import date, timedelta,datetime
from pyspark.sql.window import Window
from dateutil.relativedelta import relativedelta

# Setting Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

path_not = "s3://opendatasus.puc.raw/not_srag/*.csv"
path_rac = "s3://opendatasus.puc.structured/vacinacao/dm_raca/"

def carga_notificacao(path):
    
    try:
        print("Inicio dados Brutos")
        df = spark.read.option("header","True").option("delimiter",";").csv(path)
        df = df.withColumn("cod_class_final",sha2(col("classificacaoFinal"),256))
        df = df.withColumn("des_class_final",upper(rtrim(ltrim(col("classificacaoFinal")))))
        df = df.withColumn("cod_evolucao",sha2(col("evolucaoCaso"),256))
        df = df.withColumn("des_evolucao",upper(rtrim(ltrim((col("evolucaoCaso"))))))
        df = df.withColumn("cod_condicoes",sha2(col("condicoes"),256))
        df = df.withColumn("des_condicoes",upper(rtrim(ltrim((col("condicoes"))))))
        df = df.withColumn("cod_idade",trim(col("idade")).cast("Integer"))
        df = df.withColumn("cod_municipio_estab",substring(trim(col("municipioNotificacaoIBGE")),1,6))        
        df = df.withColumn("cod_municipio_paciente",substring(trim(col("municipioIBGE")),1,6))
        df = df.filter(col("cod_municipio_paciente") != 'SE')
        df = df.withColumn("cod_municipio_paciente",col("cod_municipio_paciente").cast("Integer"))
        df = df.withColumn("estado",trim(col("estadoIBGE")))        
        df = df.withColumn("des_raca",upper(trim(col("racaCor"))))
        df = df.withColumn("cod_sexo", expr("case when sexo is null then 'N' else substring(upper(trim(sexo)),1,1) end"))
        df = df.withColumn("cod_vacinado",trim(col("codigoRecebeuVacina")).cast("Integer"))
        df = df.withColumn("array_doses",split(trim(col("codigoDosesVacina")),','))
        df = df.withColumn("cod_doses",array_max(col("array_doses"))).drop("array_doses")        
        df = df.withColumn("dat_encerramento",to_date(col("dataEncerramento"),"yyyy-MM-dd"))
        df = df.withColumn("dat_notificacao",to_date(col("dataNotificacao"),"yyyy-MM-dd"))        
        df = df.withColumn("data",col("dat_notificacao"))
        df = df.select("cod_class_final","des_class_final","cod_evolucao","des_evolucao","cod_condicoes","des_condicoes","cod_idade","cod_municipio_estab","cod_municipio_paciente","des_raca","cod_sexo","cod_vacinado","cod_doses","dat_encerramento","dat_notificacao","data")

        df = df.withColumn("cod_class_final",expr("case when cod_class_final is null then '0' else cod_class_final end"))
        df = df.withColumn("cod_evolucao",expr("case when cod_evolucao is null then '0' else cod_evolucao end"))
        df = df.withColumn("cod_condicoes",expr("case when cod_condicoes is null then '0' else cod_condicoes end"))
        df = df.withColumn("cod_idade",expr("case when cod_idade is null then 0 else cod_idade end"))
        df = df.withColumn("cod_municipio_estab",expr("case when cod_municipio_estab is null then 0 else cod_municipio_estab end"))
        df = df.withColumn("cod_municipio_paciente",expr("case when cod_municipio_paciente is null then 0 else cod_municipio_paciente end"))
        df = df.withColumn("cod_sexo",expr("case when cod_sexo is null then 'N' else cod_sexo end"))
        df = df.withColumn("cod_vacinado",expr("case when cod_vacinado is null then 0 else cod_vacinado end"))
        df = df.withColumn("cod_doses",expr("case when cod_doses is null then 0 else cod_doses end"))
        
        print("Dados com chaves")
        
        return df
        
    except Exception as e:
        print(f"ERROR: {e}")

def fato_notificacao(df):    
    
    try:
        df = df.filter(col("estado") == 'SE')
        
        df.createOrReplaceTempView("df")        

        df = spark.sql("""
        SELECT           
            a.cod_idade
            ,a.cod_municipio_paciente
            ,a.cod_sexo
            ,a.data            
            ,count(*) as qtd_notificacao
        FROM df a
        WHERE a.cod_sexo in ('M','F')
        and a.cod_idade between 10 and 110
        group by            
            a.cod_idade
            ,a.cod_municipio_paciente
            ,a.cod_sexo
            ,a.data         
        """)                  
        
        df = df.withColumn("qtd_notificacao",col("qtd_notificacao").cast("Float"))
        
        df = df.coalesce(100)
        df.write.mode("overwrite").format("parquet").save("s3://opendatasus.puc.structured/not_srag/fato_notificacao")        
        
        del df
        
        print("fato atualizada!")
        
    except Exception as e:
        print(f"ERROR: {e}")

def main():
    
    df = carga_notificacao(path_not)    
    fato_notificacao(df)
    
if __name__ == "__main__":
    main()
    
job.commit()