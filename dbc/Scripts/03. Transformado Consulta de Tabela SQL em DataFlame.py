# Databricks notebook source
#Obs quando seu Cluster Desligar vai perder os dados do Banco de dados e tabelas criadas (Database Tables)
# Dados do DBFS não sao perdidos
# Run All no Notbook ( 2. Criando Tabelas Via Comando SQL) para subir os dados

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Criando Banco de dados 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS Teste;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Criando tabela de Clientes

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use Teste;
# MAGIC CREATE TABLE IF NOT EXISTS Clientes
# MAGIC USING csv
# MAGIC OPTIONS (
# MAGIC   'path' '/FileStore/tables/Bikes/customers.csv',  -- Caminho para o arquivo no DBFS
# MAGIC   'header' 'true',                               -- Se a primeira linha do arquivo contém cabeçalho
# MAGIC   'inferSchema' 'true'                           -- Inferir automaticamente os tipos de dados das colunas
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC use Teste;
# MAGIC select * from Clientes

# COMMAND ----------

# MAGIC %sql
# MAGIC use Teste;
# MAGIC --  Consulta de Contatos de Clientes convertendo em um DF
# MAGIC select 
# MAGIC customer_id
# MAGIC ,first_name
# MAGIC ,phone
# MAGIC ,email
# MAGIC from Clientes 

# COMMAND ----------

# MAGIC %sql
# MAGIC use Teste;
# MAGIC --  Consulta de Contatos de Clientes convertendo em um DF
# MAGIC select 
# MAGIC customer_id
# MAGIC ,first_name
# MAGIC ,phone
# MAGIC ,email
# MAGIC from Clientes
# MAGIC where first_name like "%sha%"
# MAGIC

# COMMAND ----------

DfSQL =  spark.sql('''
         
select 
customer_id as Id
,first_name as Nome
,phone as Telefone
,email as Email
from Clientes
where first_name like "%sha%"          
                
'''
)

# COMMAND ----------

DfSQL.show()

# COMMAND ----------

spark.sql("""
          
select * from clientes

"""        
          
          
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Salvando Df em diferentes formatos

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS Teste;
# MAGIC
# MAGIC
# MAGIC use Teste;
# MAGIC CREATE TABLE IF NOT EXISTS Clientes
# MAGIC USING csv
# MAGIC OPTIONS (
# MAGIC   'path' '/FileStore/tables/Bikes/customers.csv',  -- Caminho para o arquivo no DBFS
# MAGIC   'header' 'true',                               -- Se a primeira linha do arquivo contém cabeçalho
# MAGIC   'inferSchema' 'true'                           -- Inferir automaticamente os tipos de dados das colunas
# MAGIC );
# MAGIC

# COMMAND ----------

DfSQL =  spark.sql(

'''         
select 
customer_id as Id
,first_name as Nome
,phone as Telefone
,email as Email
from Clientes
where first_name like "%sha%"          
                
'''
)

# COMMAND ----------

display(DfSQL)

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/tables/'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Salvando no formato Parquet

# COMMAND ----------

#criar nova pasta
dbutils.fs.mkdirs('/FileStore/tables/DiferentesSaidas') 

# COMMAND ----------


#Obs Termos Tecnicos Para voce ir treinando 
"""
write= Escrever/ Gravar
-----  Modos de Escritas Mais usados-----
overwrite   = se existe ele sobescreve/subistitui
append      = Mantem o existente e adiciona o conteudo no final 
ignore      = Usado para salvar os dados apenas se o local de destino não existir

"""


# COMMAND ----------

DfSQL.write.mode("overwrite").parquet("/FileStore/tables/DiferentesSaidas/ContatoSha.parquet")


# COMMAND ----------

display(dbutils.fs.ls('/FileStore/tables/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/tables/DiferentesSaidas'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/tables/DiferentesSaidas/ContatoSha.parquet'))

# COMMAND ----------

# Salvar em CSV e JSON
DfSQL.write.option("delimiter", ",").mode("overwrite").csv("/FileStore/tables/DiferentesSaidas/ContatoSha.csv")
DfSQL.write.mode("overwrite").json("/FileStore/tables/DiferentesSaidas/ContatoSha.json")


# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/tables/DiferentesSaidas'))

# COMMAND ----------

#modo quebrado com \ enter
DfSQL.write\
     .option("delimiter", ",")\
     .mode("overwrite")\
     .csv("/FileStore/tables/DiferentesSaidas/ContatoSha.csv")


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Ler arquivos CSV , Json e Parquet

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/tables/DiferentesSaidas'))

# COMMAND ----------

# Caminho para o arquivo CSV
caminho_csv = "dbfs:/FileStore/tables/DiferentesSaidas/ContatoSha.csv"

# Ler o arquivo CSV em um DataFrame
df_csv = spark.read.csv(caminho_csv, header=False, inferSchema=True)
display(df_csv)



# COMMAND ----------

# Caminho para o arquivo JSON
caminho_json = "dbfs:/FileStore/tables/DiferentesSaidas/ContatoSha.json"

# Ler o arquivo JSON em um DataFrame
df_json = spark.read.json(caminho_json)
display(df_json)

# COMMAND ----------

# Caminho para o arquivo Parquet
caminho_parquet = "dbfs:/FileStore/tables/DiferentesSaidas/ContatoSha.parquet"

# Ler o arquivo Sem Salvar em Um DF
Df_Parquet =  spark.read.parquet(caminho_parquet)
display(Df_Parquet)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Saiba mais sobre tipos de arquivos Databricks
# MAGIC
# MAGIC https://docs.databricks.com/pt/external-data/index.html
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from clientes

# COMMAND ----------

novodf= spark.sql ('select * from clientes')
display(novodf)

# COMMAND ----------

#Gravar Particionado tem como?
novodf.write\
    .partitionBy("state")\
    .mode("overwrite")\
    .parquet('/FileStore/tables/DiferentesSaidas/Contato_Particionado.parquet')

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/tables/DiferentesSaidas/Contato_Particionado.parquet/state=CA/'))


# COMMAND ----------

#Ler Arquivo particionado 
state_CA = spark.read.parquet('dbfs:/FileStore/tables/DiferentesSaidas/Contato_Particionado.parquet/state=CA/')
display(state_CA)

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/tables/DiferentesSaidas/Contato_Particionado.parquet'))

# COMMAND ----------

#Ler Arquivo particionado (Geral) 
state_CA = spark.read.parquet('dbfs:/FileStore/tables/DiferentesSaidas/Contato_Particionado.parquet')
display(state_CA)
