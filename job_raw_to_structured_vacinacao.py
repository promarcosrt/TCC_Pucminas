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
import os
import datetime
from dateutil.relativedelta import relativedelta

# Setting Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


path_vacina = "s3://opendatasus.puc.raw/vacinacao/"
path_prestr = "s3://opendatasus.puc.prestructured/vacinacao"


def listar_arquivos_s3(prefixo):
    bucket_name = "opendatasus.puc.raw"
    
    try:
        array_arquivos = []
        client = boto3.resource("s3")
        bucket = client.Bucket(bucket_name)
    
        for lista in bucket.objects.filter(Prefix=prefixo):
            if(lista.key[-3:] == 'csv'):
                array_arquivos.append(f"s3://{bucket_name}/{lista.key}")
            
        return array_arquivos
    except Exception as e:
        print(f"ERROR: {e}")

def carga_vacinas(prefixo,path_prestr):
    
    lista = listar_arquivos_s3(prefixo)
    print(lista)
    
    for path in lista:
        pstr = path.split("-")
        
        parte = str(pstr[1])
        print(f"| parte: {parte} e path: {path} |")
        
        try:
            print("Inicio dados Brutos")
            
            df = spark.read.option("header","True").option("delimiter",";").csv(path)
            df = df.withColumn("cod_sistema_origem",substring(upper(trim(col("sistema_origem"))),1,3))
            df = df.withColumn("cod_dose",sha2(trim(col("vacina_descricao_dose")),256))
            df = df.withColumn("cod_estabelecimento",trim(col("estabelecimento_valor")))
            df = df.withColumn("cod_grupo",trim(col("vacina_grupoAtendimento_codigo")))
            df = df.withColumn("cod_categoria",trim(col("vacina_categoria_codigo")))
            df = df.withColumn("cod_vacina",trim(col("vacina_codigo")))
            df = df.withColumn("cod_paciente",trim(col("paciente_id")))
            df = df.withColumn("cod_raca",trim(col("paciente_racaCor_codigo")))
            df = df.withColumn("cod_idade",trim(col("paciente_idade")).cast("Integer"))
            df = df.withColumn("cod_sexo",substring(trim(col("paciente_enumSexoBiologico")),1,1))
            df = df.withColumn("cod_municipio_paciente",regexp_replace(trim(col("paciente_endereco_coIbgeMunicipio")),'None','0'))
            df = df.withColumn("cod_municipio_estab",regexp_replace(trim(col("estabelecimento_municipio_codigo")),'None','0'))
            df = df.withColumn("data_aplicacao",to_date(trim(col("vacina_dataAplicacao")),"yyyy-MM-dd"))
            df = df.withColumn("data",to_date(trim(col("vacina_dataAplicacao")),"yyyy-MM-dd"))        
            df = df.withColumn("cod_sexo", expr("case when cod_sexo is null then 'N' when cod_sexo = 'I' then 'N' else cod_sexo end"))
            df = df.withColumn("cod_municipio_estab",expr("cast(case when cod_municipio_estab is null then 0 else cod_municipio_estab end as Integer)"))
            df = df.withColumn("cod_municipio_paciente",expr("cast(case when cod_municipio_paciente is null then 0 else cod_municipio_paciente end as Integer)"))        
            ## tratamento de tipos ##
            df = df.withColumn("cod_raca",trim(col("cod_raca")).cast("Integer"))
            df = df.withColumn("cod_vacina",trim(col("cod_vacina")).cast("Integer"))
            df = df.withColumn("cod_categoria",trim(col("cod_categoria")).cast("Integer"))
            df = df.withColumn("cod_grupo",trim(col("cod_grupo")).cast("Integer"))
            df = df.withColumn("cod_estabelecimento",trim(col("cod_estabelecimento")).cast("Integer"))
            ## tratamento de nulos ##
            df = df.withColumn("cod_sistema_origem",expr("case when cod_sistema_origem is null then '0' else cod_sistema_origem end"))
            df = df.withColumn("cod_dose",expr("case when cod_dose is null then '0' else cod_dose end"))
            df = df.withColumn("cod_estabelecimento",expr("case when cod_estabelecimento is null then 0 else cod_estabelecimento end"))
            df = df.withColumn("cod_grupo",expr("case when cod_grupo is null then 0 else cod_grupo end"))
            df = df.withColumn("cod_categoria",expr("case when cod_categoria is null then 0 else cod_categoria end"))
            df = df.withColumn("cod_vacina",expr("case when cod_vacina is null then 0 else cod_vacina end"))
            df = df.withColumn("cod_paciente",expr("case when cod_paciente is null then 0 else cod_paciente end"))
            df = df.withColumn("cod_raca",expr("case when cod_raca is null then 99 else cod_raca end"))
            df = df.withColumn("cod_idade",expr("case when cod_idade is null then 999 else cod_idade end"))
            df = df.withColumn("cod_municipio_estab",expr("case when cod_municipio_estab is null then 0 else cod_municipio_estab end"))
            df = df.withColumn("cod_municipio_paciente",expr("case when cod_municipio_paciente is null then 0 else cod_municipio_paciente end"))
            df = df.withColumn("cod_sexo",expr("case when cod_sexo is null then 'N' else cod_sexo end"))
            
            registros = df.count()
            
            print(f"{registros} Dados com chaves")
            
            df = df.coalesce(100)
            
            path_parte = f"{path_prestr}/{parte}"
            
            df.write.mode("overwrite").format("parquet").save(path_parte)
            
        except Exception as e:
            print(f"ERROR: {e}")
        
def main():
    
    carga_vacinas("vacinacao/",path_prestr)

if __name__ == "__main__":
    main()
    