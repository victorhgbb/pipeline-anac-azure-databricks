# Databricks notebook source
# MAGIC %md
# MAGIC ###### Documentação parquet
# MAGIC https://spark.apache.org/docs/latest/sql-data-sources-parquet.html 
# MAGIC
# MAGIC https://spark.apache.org/docs/latest/sql-data-sources-parquet.html

# COMMAND ----------

dfpq = spark.read.parquet("/FileStore/tables/Anac/V_OCORRENCIA_AMPLAParquet")
display(dfpq)

# COMMAND ----------

#verificando dados distintos de colunas com spark sql 
display(dfpq.select("Classificacao_da_Ocorrência").distinct())


# COMMAND ----------

dfpq.write \
    .partitionBy("Classificacao_da_Ocorrência") \
    .mode("overwrite") \
    .parquet("/FileStore/tables/Anac/parquet_particionado")

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/Anac"))

# COMMAND ----------

# MAGIC %md
# MAGIC ######Vendo Arquivos particionados

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/Anac/parquet_particionado/"))

# COMMAND ----------

#Descendo mais um nivel ver os logs de cada partição
display(dbutils.fs.ls("dbfs:/FileStore/tables/Anac/parquet_particionado/Classificacao_da_Ocorrência=Acidente/"))


# COMMAND ----------

# MAGIC %md
# MAGIC ######lendo arquivo particionado 

# COMMAND ----------

dfacidente = spark.read.parquet("/FileStore/tables/Anac/parquet_particionado/Classificacao_da_Ocorrência=Ocorrência de Solo/")
display(dfacidente)

# COMMAND ----------

# MAGIC %md
# MAGIC ######Ler todos os dados mesmo particionados (agrupados)

# COMMAND ----------

dfTudo = spark.read\
    .format("parquet")\
    .load("dbfs:/FileStore/tables/Anac/parquet_particionado/")
display(dfTudo)

# COMMAND ----------

# MAGIC %md
# MAGIC ######Salvando em mais de 1 partição 

# COMMAND ----------

#exemplo , uma equipe de cada estado analisa as "Classificacao da Ocorrência" e cada uma das ocorrencias é analizado por uma pessoa
#Separarar (particionar por Classificacao da Ocorrência e estado)


dfpq.write \
    .partitionBy("UF","Classificacao_da_Ocorrência") \
    .mode("overwrite") \
    .parquet("/FileStore/tables/Anac/parquet_Multiparticionado")

#Obs: pode demorar na escrita devido aos particionamentos , mas ganha tempo na leitura 


# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/Anac"))

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/Anac/parquet_Multiparticionado/"))

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/Anac/parquet_Multiparticionado/UF=MG/"))

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/Anac/parquet_Multiparticionado/UF=MG/Classificacao_da_Ocorrência=Acidente/"))

# COMMAND ----------

#simulando o responsavel de Mg onde a Classificacao da Ocorrência seja igual a Acidente trabalharia com o arquivo 
#dbfs:/FileStore/tables/Anac/parquet_Multiparticionado/UF=MG/Classificacao_da_Ocorrência=Acidente/
#e nao todo o arquivo original , ganhando performace na leitura 

# COMMAND ----------

# MAGIC %md
# MAGIC Dados Acidente do analista de MG

# COMMAND ----------


DfAcidentesMG = spark.read\
    .format("parquet")\
    .load("/FileStore/tables/Anac/parquet_Multiparticionado/UF=MG/Classificacao_da_Ocorrência=Acidente/")
display(DfAcidentesMG)


# COMMAND ----------


