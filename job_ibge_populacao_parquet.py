import sys
#mport boto3
import logging
import pandas as pd

import pyspark.sql.functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
#from awsglue.job import Job

#from awsglue.context import GlueContext
#from pyspark.context import SparkContext
#from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from datetime import date, timedelta,datetime
from pyspark.sql.window import Window
import os

spark =  SparkSession.builder.getOrCreate()


#spark = SparkContext.getOrCreate()
#glueContext = GlueContext(sc)
#spark = glueContext.spark_session
#spark = sc.spark_session

filepath = "s3://opendatasus.puc.raw/populacao/tabela3175.csv"
path_dt = "s3://opendatasus.puc.structured/tempo/dm_data/"
path_estrut = "s3://opendatasus.puc.structured/"

def carga_pop(path):
    
    try:
        print(f"Inicio dados Brutos {path}")
        
        df_csv = pd.read_csv(path, delimiter = ';', header = 3)
        
        meuSchema = StructType([ StructField('Situacao_domicilio', StringType(), True)\
                                ,StructField('Sexo', StringType(), True)\
                                ,StructField('Cor_raca', StringType(), True)\
                                ,StructField('Cod', StringType(), True)\
                                ,StructField('Municipio', StringType(), True)\
                                ,StructField('Idade', StringType(), True)\
                                ,StructField('Qtd', StringType(), True)])
        df_tmp = spark.createDataFrame(df_csv,schema=meuSchema)
        del df_csv
        df_tmp = df_tmp.filter(~trim(col("Qtd")).isin(['-','',' ']))
        df_tmp = df_tmp.filter(trim(col("Idade")).like('%ano%'))
        df_tmp = df_tmp.withColumn("Idade",regexp_replace(regexp_replace(col("Idade")," anos","")," ano",""))
        
        
        df_tmp = df_tmp.filter(~trim(col("Qtd")).isin(['-','',' ']))
        #df_tmp = df_tmp.filter(trim(col("Idade")).like('%ano%'))
        df_tmp = df_tmp.withColumn("cod_idade",trim(regexp_replace(regexp_replace(col("Idade")," anos","")," ano","")))
        df_tmp = df_tmp.withColumn("cod_idade",expr("cast(case when cod_idade like '% m%' then 0 else cod_idade end as Integer)"))
        df_tmp = df_tmp.withColumn("desc_idade",rtrim(ltrim(col("Idade"))))
        df_tmp = df_tmp.withColumn("desc_idade",expr("case when cod_idade = 0 then 'Meses Idade' else desc_idade end"))     
        df_tmp = df_tmp.withColumn("situacao_domicilio",trim(col("Situacao_domicilio")))
        df_tmp = df_tmp.withColumn("desc_sexo",upper(trim(col("Sexo"))))
        df_tmp = df_tmp.withColumn("cor_raca",trim(col("Cor_raca")))
        df_tmp = df_tmp.withColumn("cod_municipio_paciente",substring(trim(col("Cod")),1,6).cast("Integer"))
        df_tmp = df_tmp.withColumn("qtd",col("Qtd").cast("Integer"))
        
        
        print("Dados com chaves")
        
        return df_tmp
        
    except Exception as e:
        print(f"ERROR: {e}")
        
def fato_populacao(df,path_dt,path_estrut):
    
    try:
        print("Inicio fato populacao")
        
        # não utilizado
        #columns = ["cod_raca","cor_raca"]
        #data = [(4,'Amarela'),(2,'Preta'),(3,'Parda'),(1,'Branca'),(99,'Sem declaração'),(5,'Indígena')]
        #df_raca = spark.createDataFrame(data,columns)        
        #df_raca.createOrReplaceTempView("df_raca")

        #df_dt = spark.read.parquet(path_dt)
        #df_dt = df_dt.select("anomes").distinct()
        #df_dt.createOrReplaceTempView("df_dt")

        df = df.withColumn("cod_sexo",expr("case when desc_sexo = 'HOMENS' then 'M' when desc_sexo = 'MULHERES' then 'F' else 'N' end"))
        df.createOrReplaceTempView("df")
        
        df_fato = spark.sql("""
            select
                a.cod_sexo                
                ,a.cod_municipio_paciente
                ,(a.cod_idade + 9) as cod_idade
                ,sum(a.qtd) as qtd_populacao
            from df a            
            group by                
                a.cod_sexo                
                ,a.cod_municipio_paciente
                ,a.cod_idade            
        """) 

        df_fato = df_fato.filter("cod_idade between 10 and 110")        
        df_fato = df_fato.filter("cod_sexo in ('F','M')")

        df_fato = df_fato.coalesce(60)
        df_fato.write.mode("overwrite").format("parquet").save(f"{path_estrut}populacao/fato_populacao")

        print("fim fato")
        
    except Exception as e:
        print(f"ERROR: {e}")
        

def idades():
    
    colunas = ["cod_idade","desc_idade"]
    idade = list(range(10,111))
    # Criando uma lista com as descrições das idades
    descricao = [f"{i} Ano" if i <= 1 else f"{i} Anos" for i in idade]
    
    data = list(zip(idade, descricao))
    df = spark.createDataFrame(data,colunas)
    return df       
        
def dm_faixa_etaria(df,path_estrut):
    
    try:
        print("Inicio dimensao faixa etaria")
        df_fx = df.select("cod_idade","desc_idade").distinct()
        df_fx.createOrReplaceTempView("df_fx")
        
        df_faixa = spark.sql("""
            select
                a.cod_idade
                ,a.desc_idade
                ,case when a.cod_idade < 10 then 'Menos de 10 anos' else
                    case when a.cod_idade >= 10 and a.cod_idade <= 19 then 'De 10 a 19 anos' else
                        case when a.cod_idade >= 20 and a.cod_idade <= 39 then 'De 20 a 39 anos' else
                            case when a.cod_idade >= 40 and a.cod_idade <= 59 then 'De 40 a 59 anos' else
                                case when a.cod_idade >= 60 and a.cod_idade <= 89 then 'De 60 a 89 anos' else 'Mais de 90 anos' end
                            end
                        end
                    end
                end as faixa_etaria
            from df_fx a
        """)
        #df_faixa.show()
        
        df_faixa.write.mode("overwrite").format("parquet").save(f"{path_estrut}vacinacao/dm_faixa_etaria")  
        
        print("fim faixas")
        
    except Exception as e:
        print(f"ERROR: {e}")        

def main():
    
    try:    
        
        df_dados = carga_pop(filepath)        
        fato_populacao(df_dados,path_dt,path_estrut)

        df_idade = idades()
        dm_faixa_etaria(df_idade,path_estrut)
        
    except Exception as e:
        print(f"ERROR: {e}")
        
if __name__ == "__main__":
    main()
        
