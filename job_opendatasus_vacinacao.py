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
path_prestr = "s3://opendatasus.puc.prestructured/vacinacao/*"
path_esvaci = "s3://opendatasus.puc.structured/vacinacao"
path_estemp = "s3://opendatasus.puc.structured/tempo"
path_popula = "s3://opendatasus.puc.structured/populacao/fato_populacao/"
path_locali = "s3://opendatasus.puc.raw/auxiliares/BR_Localidades_2010_v1.csv"

def cordenadas_municipios(path_locali):
    
    try:
        df_local = spark.read.option("header","True").option("delimiter",";").option("encoding", "UTF-8").csv(path_locali)
        df_local = df_local.select("CD_GEOCODDS","NM_MUNICIPIO","NM_UF","LAT","LONG").distinct()
        df_local = df_local.withColumn("cod_municipio",substring(trim(col("CD_GEOCODDS")),1,6).cast("Integer")).drop("CD_GEOCODDS")
        df_local = df_local.withColumn("desc_municipio",trim(col("NM_MUNICIPIO")).cast("string")).drop("NM_MUNICIPIO")
        df_local = df_local.withColumn("desc_uf",trim(col("NM_UF")).cast("string")).drop("NM_UF")
        df_local = df_local.withColumn("latitude",regexp_replace(trim(col("LAT")),"[\,]",".").cast("Float")).drop("LAT")
        df_local = df_local.withColumn("longitude",regexp_replace(trim(col("LONG")),"[\,]",".").cast("Float")).drop("LONG")
        df_local = df_local.filter(col("desc_uf") == 'SERGIPE')

        df_local = df_local.groupBy("cod_municipio","desc_municipio","desc_uf").agg(max("latitude").alias("latitude"),max("longitude").alias("longitude"))

        return df_local

    except Exception as e:
        print(f"ERROR: {e}")

def carga_vacinas(path_prestr):
    
    try:
        
        df = spark.read.parquet(path_prestr)
        return df
    
        print("Lendo dados Pré Estruturados!")
        
    except Exception as e:
        print(f"ERROR: {e}")    
    
def fato_vacinas(df,path_esvaci,path_popula):
    
    #df_pop = spark.read.parquet(path_popula)

    try:
        
        df.createOrReplaceTempView("df")
        #df_pop.createOrReplaceTempView("df_pop")

        #########################################
        # Filtros aplicados para remover discrepancias
        # Restringir pacientes residentes no estado de Sergipe
        # Restringir Pacientes com registro de Sexo (Masculino, Feminino) Definido
        # Restringir Idade entre 10 e 110 anos publico alvo das analises
        # Datas igual ou superior a 19/01/2021 data da aplicação da primeira dose de vacina no estado
        # Remover dados de doses não informados
        
        print("Inicio fato")
        df_fato = spark.sql("""
        SELECT
            a.cod_sistema_origem as cod_sistema_origem
            ,a.cod_estabelecimento as cod_estabelecimento
            ,a.cod_grupo as cod_grupo
            ,a.cod_categoria as cod_categoria
            ,a.cod_vacina as cod_vacina
            ,a.cod_paciente as cod_paciente
            ,a.cod_dose as cod_dose            
            ,a.cod_raca as cod_raca
            ,a.cod_sexo as cod_sexo
            ,a.cod_idade as cod_idade
            ,a.cod_municipio_estab as cod_municipio_estab
            ,a.cod_municipio_paciente as cod_municipio_paciente            
            ,a.data_aplicacao as data
            ,count(distinct a.document_id) as Qtd_Vacinas_Aplicadas
        FROM df a
        where a.paciente_endereco_uf = 'SE'
        and a.cod_sexo in ('F','M')
        and a.cod_idade between 10 and 110
        and a.cod_dose <> '0'
        and a.data_aplicacao >= '2021-01-19'
        group by
            a.cod_sistema_origem
            ,a.cod_estabelecimento
            ,a.cod_grupo
            ,a.cod_categoria
            ,a.cod_vacina
            ,a.cod_paciente
            ,a.cod_raca
            ,a.cod_dose
            ,a.cod_idade
            ,a.cod_sexo
            ,a.cod_municipio_estab
            ,a.cod_municipio_paciente     
        """)
        
        df_fato = df_fato.coalesce(100)
        df_fato.write.mode("overwrite").format("parquet").save(path_esvaci+"/fato_vacinas")
                
        print("fato atualizada!")
        
    except Exception as e:
        print(f"ERROR: {e}")
    
def dm_sistemas(df,path_esvaci):

    try:
        
        df.createOrReplaceTempView("df")
        
        print("Inicio dimensao sistemas")
        
        df_sistemas = spark.sql("""
        SELECT distinct
            cod_sistema_origem
            ,case when cod_sistema_origem is null then 'SEM INFORMACAO' else upper(sistema_origem) end as sistema_origem
        FROM df
        where sistema_origem is not null
        """)
        
        df_sistemas.write.mode("overwrite").format("parquet").save(path_esvaci+"/dm_sistemas")
        
        print("dimensao sistemas atualizada!")
        
    except Exception as e:
        print(f"ERROR: {e}")
        
def dm_doses(df,path_esvaci):

    try:
        
        df.createOrReplaceTempView("df")
        
        print("Inicio dimensao doses")
        
        df_doses = spark.sql("""
        SELECT distinct
            cod_dose
            ,case when cod_dose is null then 'SEM INFORMACAO' else upper(vacina_descricao_dose) end as desc_dose
        FROM df
        where vacina_descricao_dose is not null
        """)
        
        df_doses.write.mode("overwrite").format("parquet").save(path_esvaci+"/dm_doses")
      
        print("dimensao doses atualizada!")
        
    except Exception as e:
        print(f"ERROR: {e}")

def dm_sexo(df,path_esvaci):

    try:
        
        df.createOrReplaceTempView("df")
        
        print("Inicio dimensao sexo")
        
        df = spark.sql("""
        SELECT distinct
            cod_sexo
            ,case when cod_sexo = 'F' then 'FEMININO' when cod_sexo ='M' then 'MASCULINO' else 'SEM INFORMACAO' end as des_sexo
        FROM df
        """)
        
        df = df.filter("cod_sexo in ('F','M')") # ajustado

        df.write.mode("overwrite").format("parquet").save("s3://opendatasus.puc.structured/vacinacao/dm_sexo")
        
        print("dimensao sexo atualizada!")
        
    except Exception as e:
        print(f"ERROR: {e}")        

def dm_paciente(df,path_esvaci):

    try:
        
        df.createOrReplaceTempView("df")
        
        print("Inicio dimensao paciente")
        
        df_paciente = spark.sql("""
        SELECT distinct
            cod_paciente
            ,to_date(paciente_dataNascimento,'yyyy-MM-dd') as data_nascimento
            ,coalesce(trim(paciente_endereco_cep),0) as cep
        FROM df
        where cod_paciente is not null
        """)
        
        df_paciente.write.mode("overwrite").format("parquet").save(path_esvaci+"/dm_paciente")

        print("dimensao paciente atualizada!")
        
    except Exception as e:
        print(f"ERROR: {e}")
        
def dm_estabelecimento(df,path_esvaci):

    try:
        
        df.createOrReplaceTempView("df")
        
        print("Inicio dimensao estabelecimento")
        
        df_estabelecimento = spark.sql("""
        SELECT distinct
            cod_estabelecimento
            ,coalesce(estabelecimento_razaoSocial,'SEM INFORMACAO') as razao_social
            ,coalesce(estalecimento_noFantasia,'SEM INFORMACAO') as nome_fantasia
        FROM df
        where cod_estabelecimento is not null
        """)
        
        df_estabelecimento.write.mode("overwrite").format("parquet").save(path_esvaci+"/dm_estabelecimento")
       
        print("dimensao estabelecimento atualizada!")
        
    except Exception as e:
        print(f"ERROR: {e}")
        
def dm_vacina(df,path_esvaci):

    try:
        
        df.createOrReplaceTempView("df")
        
        print("Inicio dimensao vacina")
        
        df_vacina = spark.sql("""
        SELECT distinct
            cod_vacina
            ,coalesce(vacina_nome,'SEM INFORMACAO') as nome_vacina
            ,coalesce(vacina_fabricante_nome,'SEM INFORMACAO') as nome_fabricante
        FROM df
        where cod_vacina is not null
        """)
        
        df_vacina.write.mode("overwrite").format("parquet").save(path_esvaci+"/dm_vacina")

        print("dimensao vacina atualizada!")
        
    except Exception as e:
        print(f"ERROR: {e}") 
        
def dm_grupo(df,path_esvaci):

    try:
        
        df.createOrReplaceTempView("df")
        
        print("Inicio dimensao grupo")
        
        df_grupo = spark.sql("""
        SELECT distinct
            cod_grupo
            ,coalesce(vacina_grupoAtendimento_nome,'SEM INFORMACAO') as desc_grupo
        FROM df
        where cod_grupo is not null
        """)
        
        df_grupo.write.mode("overwrite").format("parquet").save(path_esvaci+"/dm_grupo")
        
        print("dimensao grupo atualizada!")
        
    except Exception as e:
        print(f"ERROR: {e}")
        
def dm_categoria(df,path_esvaci):

    try:
        
        df.createOrReplaceTempView("df")
        
        print("Inicio dimensao categoria")
        
        df_categoria = spark.sql("""
        SELECT distinct
            cod_categoria
            ,coalesce(vacina_categoria_nome,'SEM INFORMACAO') as desc_categoria
        FROM df
        where cod_categoria is not null
        """)
        
        df_categoria.write.mode("overwrite").format("parquet").save(path_esvaci+"/dm_categoria")
      
        print("dimensao categoria atualizada!")
        
    except Exception as e:
        print(f"ERROR: {e}") 
        
def dm_raca(df,path_esvaci):

    try:
        
        df.createOrReplaceTempView("df")
        
        print("Inicio dimensao raca")
        
        df_raca = spark.sql("""
        SELECT distinct
            cod_raca
            ,coalesce(paciente_racaCor_valor,'SEM INFORMACAO') as desc_raca
        FROM df
        where cod_raca is not null
        """)
        
        df_raca.write.mode("overwrite").format("parquet").save(path_esvaci+"/dm_raca")
       
        print("dimensao raca atualizada!")
        
    except Exception as e:
        print(f"ERROR: {e}")

def dm_regiao_estab(df,df_cord,path_esvaci):

    try:
        df = df.withColumn("cod_municipio_estab",col("cod_municipio_estab").cast("Integer"))
        df.createOrReplaceTempView("df")
        df_cord.createOrReplaceTempView("df_cord")
        
        print("Inicio dimensao regiao estab")
        
        df_regiao_estab = spark.sql("""
        SELECT distinct
            a.cod_municipio_estab
            ,(case when a.cod_municipio_estab = 0 then '-' else c.desc_municipio end) as desc_municipio_estab
            ,(case when a.cod_municipio_estab = 0 then '-' else c.desc_uf end) as desc_uf_estab
            ,c.latitude
            ,c.longitude
        FROM df a
        JOIN df_cord c ON a.cod_municipio_estab = c.cod_municipio
        where cod_municipio_estab is not null
        and estabelecimento_uf in ('SE','-')
        """)
        del df_cord
        df_regiao_estab.write.mode("overwrite").format("parquet").save(path_esvaci+"/dm_regiao_estab")
        
        print("dimensao regiao estab atualizada!")
        
    except Exception as e:
        print(f"ERROR: {e}") 
        
def dm_regiao_paciente(df,df_cord,path_esvaci):

    try:
        df = df.withColumn("cod_municipio_paciente",col("cod_municipio_paciente").cast("Integer"))
        df.createOrReplaceTempView("df")
        df_cord.createOrReplaceTempView("df_cord")

        print("Inicio dimensao regiao paciente")
        
        df_regiao_paciente = spark.sql("""
        SELECT distinct
            a.cod_municipio_paciente
            ,(case when (a.cod_municipio_paciente = 0) then 'SEM INFORMACAO' else c.desc_municipio end) as desc_municipio
            ,(case when (a.cod_municipio_paciente = 0) then '-' else c.desc_uf end) as desc_uf
            ,(case when (a.cod_municipio_paciente = 0) then '-' else 'Brasil' end) as desc_pais
            ,c.latitude
            ,c.longitude
        FROM df a
        JOIN df_cord c ON a.cod_municipio_paciente = c.cod_municipio
        where cod_municipio_paciente is not null
        and paciente_endereco_uf in ('SE','-')
        """)
        del df_cord
        df_regiao_paciente.write.mode("overwrite").format("parquet").save(path_esvaci+"/dm_regiao_paciente")
              
        print("dimensao regiao paciente atualizada!")
        
    except Exception as e:
        print(f"ERROR: {e}")  
       

def main():
    
    df_dados = carga_vacinas(path_prestr)
    #
    fato_vacinas(df_dados,path_esvaci,path_popula)
    dm_sistemas(df_dados,path_esvaci)
    dm_doses(df_dados,path_esvaci)
    dm_sexo(df_dados,path_esvaci)
    dm_paciente(df_dados,path_esvaci)
    dm_estabelecimento(df_dados,path_esvaci)
    dm_vacina(df_dados,path_esvaci)
    dm_grupo(df_dados,path_esvaci)
    dm_categoria(df_dados,path_esvaci)
    dm_raca(df_dados,path_esvaci)
    #
    df_cord = cordenadas_municipios(path_locali)
    dm_regiao_estab(df_dados,df_cord,path_esvaci)
    dm_regiao_paciente(df_dados,df_cord,path_esvaci)
    
    del df_dados
    
if __name__ == "__main__":
    main()
    