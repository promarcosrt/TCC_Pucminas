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

path_srag = "s3://opendatasus.puc.prestructured/srag/*"
path_obto = "s3://opendatasus.puc.structured/obitos/"

def carga_srag(path):
    
    try:
        print("Inicio dados Brutos")
        print(path)
        df = spark.read.parquet(path)
        
        df = df.withColumn("cod_class_final",trim(col("CLASSI_FIN")))        
        df = df.withColumn("cod_evolucao",trim(col("EVOLUCAO")))
        df = df.withColumn("cod_risco",expr(""" case when trim(FATOR_RISC) = 'S' then 1
                                                     when trim(FATOR_RISC) = 'N' then 2
                                                     when trim(FATOR_RISC) is null then 9
                                                else trim(FATOR_RISC) end"""))
        df = df.withColumn("cod_idade",trim(col("NU_IDADE_N")))
        df = df.withColumn("cod_municipio_estab",trim(col("CO_MUN_NOT")))
        df = df.withColumn("cod_municipio_paciente",trim(col("CO_MUN_RES")))
        df = df.withColumn("cod_uf_paciente",trim(col("SG_UF")))
        df = df.withColumn("cod_raca",regexp_replace(trim(col("CS_RACA")),'4','6').cast("Integer"))
        df = df.withColumn("cod_raca", expr("""case when cod_raca = 6 then 3
                                                    when cod_raca = 3 then 4
                                                    when cod_raca = 9 then 99
                                                    when cod_raca is null then 99 
                                                else cod_raca end"""))        
        df = df.withColumn("cod_sexo", expr("""case when (trim(CS_SEXO) = 'I') then 'N' 
                                                    when (trim(CS_SEXO) is null) then 'N'                                                                                                        
                                                else trim(CS_SEXO) end"""))
        df = df.withColumn("cod_internacao",trim(col("HOSPITAL")).cast("Integer"))
        df = df.withColumn("cod_uti",trim(col("UTI")).cast("Integer"))
        df = df.withColumn("cod_vacinado",trim(col("VACINA_COV")).cast("Integer"))
        df = df.withColumn("cod_doses", expr("""case when (trim(DOSE_1_COV) is not null) then 1 
                                                    when (trim(DOSE_2_COV) is not null) then 2
                                                    when (trim(DT_DOSEUNI) is not null) then 1 
                                                    when (trim(DT_1_DOSE) is not null) then 1
                                                    when (trim(DT_2_DOSE) is not null) then 2                                                  
                                                else 0 end"""))      
        df = df.withColumn("dat_encerramento",to_date(col("DT_DIGITA"),"dd/MM/yyyy"))
        df = df.withColumn("dat_notificacao",to_date(col("DT_NOTIFIC"),"dd/MM/yyyy"))               
        df = df.withColumn("data",col("dat_notificacao"))

        df = df.select("cod_class_final","cod_evolucao","cod_risco","cod_idade","cod_municipio_estab","cod_municipio_paciente","cod_uf_paciente","cod_raca","cod_sexo","cod_internacao","cod_uti","cod_vacinado","cod_doses","dat_encerramento","dat_notificacao","data")

        df = df.withColumn("cod_class_final",expr("case when cod_class_final is null then 0 else cod_class_final end"))
        df = df.withColumn("cod_evolucao",expr("case when cod_evolucao is null then 9 else cod_evolucao end"))
        df = df.withColumn("cod_risco",expr("case when cod_risco is null then 9 else cod_risco end"))
        df = df.withColumn("cod_idade",expr("case when cod_idade is null then 0 else cod_idade end"))
        df = df.withColumn("cod_idade",col("cod_idade").cast("Integer"))
        df = df.withColumn("cod_internacao",expr("case when cod_internacao is null then 9 else cod_internacao end"))
        df = df.withColumn("cod_uti",expr("case when cod_uti is null then 9 else cod_uti end"))
        df = df.withColumn("cod_municipio_estab",expr("case when cod_municipio_estab is null then 0 else cod_municipio_estab end"))
        df = df.withColumn("cod_municipio_estab",col("cod_municipio_estab").cast("Integer"))
        df = df.withColumn("cod_municipio_paciente",expr("case when cod_municipio_paciente is null then 0 else cod_municipio_paciente end"))
        df = df.withColumn("cod_municipio_paciente",col("cod_municipio_paciente").cast("Integer"))
        df = df.withColumn("cod_vacinado",expr("case when cod_vacinado is null then 9 else cod_vacinado end"))
        df = df.withColumn("cod_doses",expr("case when cod_doses is null then 0 else cod_doses end"))
        
        print("Dados com chaves")
        
        return df
        
    except Exception as e:
        print(f"ERROR: {e}")

def fato_obitos(df,path):
    
    try:
        
        df = df.filter("cod_evolucao = 2")
        
        df.createOrReplaceTempView("df")
        
        df = spark.sql("""
        SELECT       
            a.cod_idade            
            ,a.cod_municipio_paciente
            ,a.cod_sexo
            ,a.data            
            ,count(*) as qtd_obito
        FROM df a
        WHERE a.cod_idade between 10 and 110
        and a.cod_sexo in ('F','M')
        and a.cod_uf_paciente = 'SE'
        group by          
            a.cod_idade            
            ,a.cod_municipio_paciente
            ,a.cod_sexo
            ,a.data         
        """)
        
        df = df.withColumn("qtd_obito",col("qtd_obito").cast("Float"))

        #df_fato.write.mode("overwrite").partitionBy("cod_sexo","cod_sistema_origem","cod_doses","cod_municipio_estab").\
        #format("parquet").save("s3://opendatasus.puc.structured/vacinacao/fato_vacinas")
        df = df.coalesce(100)
        df.write.mode("overwrite").format("parquet").save(path +"/fato_obitos")        


        print("fato atualizada!")
        
    except Exception as e:
        print(f"ERROR: {e}")

def main():
    
    df = carga_srag(path_srag)    
    fato_obitos(df,path_obto)
    
if __name__ == "__main__":
    main()
    
job.commit()