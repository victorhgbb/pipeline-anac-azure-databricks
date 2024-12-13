# Databricks notebook source
# MAGIC %md
# MAGIC ######Sistema interno de arquivo
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ######Teste MarkDown

# COMMAND ----------

# MAGIC %md
# MAGIC Teste
# MAGIC
# MAGIC #Teste
# MAGIC
# MAGIC ##Teste
# MAGIC
# MAGIC ####Teste
# MAGIC
# MAGIC ######Teste

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/FileStore/

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/databricks-datasets/

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/COVID/

# COMMAND ----------

# MAGIC %fs ls  dbfs:/databricks-datasets/COVID/ESRI_hospital_beds/

# COMMAND ----------

#Vendos os dados  obs: depois fazer ajustes ou salvar em DF se quiser
display(
spark.read.csv("/databricks-datasets/COVID/ESRI_hospital_beds/Definitive_Healthcare__USA_Hospital_Beds_2020_03_24.csv")
)

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ######Criando nova pasta 

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC mkdirs /FileStore/tables/NovaPasta
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Vendo nova Pasta 

# COMMAND ----------

# MAGIC %fs
# MAGIC ls
# MAGIC /FileStore/tables

# COMMAND ----------

# MAGIC %md
# MAGIC ######Deletando pasta

# COMMAND ----------

# MAGIC %fs
# MAGIC rm /FileStore/tables/NovaPasta/

# COMMAND ----------

# MAGIC %md
# MAGIC ######Copiando Arquivo de uma pasta 

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/Bikes

# COMMAND ----------

# MAGIC %fs
# MAGIC cp dbfs:/FileStore/tables/Bikes/products.csv  dbfs:/FileStore/tables/Bikes2/products.csv 

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/Bikes2/

# COMMAND ----------

# MAGIC %md
# MAGIC ######Renomeando arquivo
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/FileStore/tables
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC mv dbfs:/FileStore/tables/Bikes2/products.csv dbfs:/FileStore/tables/Bikes2/NovoNometeste.csv

# COMMAND ----------

# MAGIC %md
# MAGIC ######Deletando Pasta Bike2

# COMMAND ----------

# MAGIC %fs rm -r /FileStore/tables/Bikes2/ 

# COMMAND ----------

dbutils.fs.rm( 'dbfs:/FileStore/tables/Bikes2/', recurse = True)
