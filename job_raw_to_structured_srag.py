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
args = getResolvedOptions(sys.argv, ['JOB_NAME','ANO'])
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

path_srag = "s3://opendatasus.puc.raw/srag/"
path_pre = "s3://opendatasus.puc.prestructured/srag/"

def all_files_os(ano,path,path_pre):

    # Ler cada arquivo como um DataFrame separado
    # 2020 ###################################################################
    if (ano == 2020):
        print(path+"INFLUD20-06-03-2023.csv")
        df1 = spark.read.option("header","True").option("delimiter",";").csv(path+"INFLUD20-06-03-2023.csv")
        df1 = df1.filter("trim(SG_UF) = 'SE' and trim(CLASSI_FIN) = '5'")  
    
        colunas = ['ESTRANG','VACINA_COV','DOSE_1_COV','DOSE_2_COV','DOSE_REF','FAB_COV_1','FAB_COV_2','FAB_COVREF','LAB_PR_COV','LOTE_1_COV','LOTE_2_COV','LOTE_REF','FNT_IN_COV','DOSE_2REF','FAB_COVRF2','LOTE_REF2','TRAT_COV','TIPO_TRAT','OUT_TRAT','DT_TRT_COV'] 
        
        for col in colunas:
            df1 = df1.withColumn(col,lit('0'))
            
        df1 = df1.select("CLASSI_FIN","EVOLUCAO","FATOR_RISC","NU_IDADE_N","SG_UF_NOT","CO_MUN_NOT","SG_UF","CO_MUN_RES","CS_RACA","CS_SEXO","HOSPITAL","UTI","VACINA_COV","DOSE_1_COV","DOSE_2_COV","DT_DOSEUNI","DT_1_DOSE","DT_2_DOSE","DT_DIGITA","DT_NOTIFIC")
        
        df1 = df1.coalesce(100)
        df1.write.mode("overwrite").format("parquet").save(path_pre +"2020/")
        del df1
    # 2021 ###################################################################
    elif (ano == 2021):
        print(path+"INFLUD21-06-03-2023.csv")
        df2 = spark.read.option("header","True").option("delimiter",";").csv(path+"INFLUD21-06-03-2023.csv")    
        df2 = df2.filter("trim(SG_UF) = 'SE' and trim(CLASSI_FIN) = '5'")
        colunas2 = ['DOSE_2REF','FAB_COVRF2','LOTE_REF2','TRAT_COV','TIPO_TRAT','OUT_TRAT','DT_TRT_COV']
        for col in colunas2:
            df2 = df2.withColumn(col,lit('0'))
        df2 = df2.select("CLASSI_FIN","EVOLUCAO","FATOR_RISC","NU_IDADE_N","SG_UF_NOT","CO_MUN_NOT","SG_UF","CO_MUN_RES","CS_RACA","CS_SEXO","HOSPITAL","UTI","VACINA_COV","DOSE_1_COV","DOSE_2_COV","DT_DOSEUNI","DT_1_DOSE","DT_2_DOSE","DT_DIGITA","DT_NOTIFIC")
        
        df2 = df2.coalesce(100)
        df2.write.mode("overwrite").format("parquet").save(path_pre +"2021/")
        del df2
    # 2022 ###################################################################
    elif (ano == 2022):
        print(path+"INFLUD22-06-03-2023.csv")
        df3 = spark.read.option("header","True").option("delimiter",";").csv(path+"INFLUD22-06-03-2023.csv")
        df3 = df3.filter("trim(SG_UF) = 'SE' and trim(CLASSI_FIN) = '5'")
        df3 = df3.select("CLASSI_FIN","EVOLUCAO","FATOR_RISC","NU_IDADE_N","SG_UF_NOT","CO_MUN_NOT","SG_UF","CO_MUN_RES","CS_RACA","CS_SEXO","HOSPITAL","UTI","VACINA_COV","DOSE_1_COV","DOSE_2_COV","DT_DOSEUNI","DT_1_DOSE","DT_2_DOSE","DT_DIGITA","DT_NOTIFIC")
    
        df3 = df3.coalesce(100)
        df3.write.mode("overwrite").format("parquet").save(path_pre +"2022/")
        del df3
    # 2023 ###################################################################     
    elif (ano == 2023):
        print(path+"INFLUD23-06-03-2023.csv")
        df4 = spark.read.option("header","True").option("delimiter",";").csv(path+"INFLUD23-06-03-2023.csv")    
        df4 = df4.filter("trim(SG_UF) = 'SE' and trim(CLASSI_FIN) = '5'")
        df4 = df4.select("CLASSI_FIN","EVOLUCAO","FATOR_RISC","NU_IDADE_N","SG_UF_NOT","CO_MUN_NOT","SG_UF","CO_MUN_RES","CS_RACA","CS_SEXO","HOSPITAL","UTI","VACINA_COV","DOSE_1_COV","DOSE_2_COV","DT_DOSEUNI","DT_1_DOSE","DT_2_DOSE","DT_DIGITA","DT_NOTIFIC")
    
        df4 = df4.coalesce(100)
        df4.write.mode("overwrite").format("parquet").save(path_pre +"2023/")
        del df4
    else:
        print("arquivo de origem não especificado!")
    #########################################################################   

def main():
    
    ano = int(args["ANO"])
    
    print(ano)
    
    if (ano == 0):
        for ano in range(2020,2024):
            all_files_os(ano,path_srag,path_pre)
    else:
        all_files_os(ano,path_srag,path_pre)
        
if __name__ == "__main__":
    main()
    
job.commit()