# Databricks notebook source
# MAGIC %md
# MAGIC #####Documentação Databricks
# MAGIC https://docs.databricks.com/pt/external-data/parquet.html
# MAGIC
# MAGIC #####Documentação Spark
# MAGIC https://spark.apache.org/docs/latest/sql-data-sources-parquet.html
# MAGIC
# MAGIC Pesquisar no google Parquet Documentação (databricks ou Spark)
# MAGIC

# COMMAND ----------

#Curiosidades comparado com CSV e Json
"""
80% (Aprox) menos armazenamento ao comparar com CSV e Json
30x (Aprox) Mais Rápido
90% (Aprox) Economia na leitura agiliza o cluster isso resulta em leituras e gravações mais rápidas.
Pode ser particionado
Quanto menor tamando de armazenamento menos dinheiro a empresa paga em recursos Cloud
Pesquise no google , comparação arquivo parquet com CSV 
"""

#Será que é isso tudo mesmo ? Vamos  ver na pratica

# COMMAND ----------

# MAGIC %md
# MAGIC ######Transformando DataFlame em Parquet
# MAGIC

# COMMAND ----------

df = spark.read.json("dbfs:/FileStore/tables/Anac/V_OCORRENCIA_AMPLA.json")
display(df)

# COMMAND ----------

df.write \
  .format('parquet') \
  .mode("overwrite") \
  .save("/FileStore/tables/Anac/V_OCORRENCIA_AMPLAParquet")


# COMMAND ----------

#Código em unica linha 
#df.write.format('parquet').mode("overwrite").save("/FileStore/tables/Anac/V_OCORRENCIA_AMPLAParquet")

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/Anac"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Alterando nome do arquivo

# COMMAND ----------

#Arquivo antigo =   dbfs:/FileStore/tables/Anac/V_OCORRENCIA_AMPLA-zip/

#dbutils.fs.mv("/FileStore/tables/Anac/V_OCORRENCIA_AMPLA-zip", "/FileStore/tables/Anac/csv_zip" , recurse = True)

# dbutils.fs.mv + arquivo caminho completo do que qer mudaro nome + caminho como novo nome  + recurse=True para alterar tudo dentro da pasta que for nescessário


# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/Anac/V_OCORRENCIA_AMPLAParquet/"))
#por padrão ele ja vem compactado na estensão snappy

# COMMAND ----------

#csv =     9869805  Bytes  9MB
#json=     20436383 Bytes  20MB
#parquet = 3308863  Bytes  3MB

#Da para melhorar ainda mais?posso ser promovido por isso? pense em grande escala de economia 20 mil por mes ou 3 mil por mes qual melhor?

# COMMAND ----------

# MAGIC %md
# MAGIC ###### lendo arquivo Parquet

# COMMAND ----------

dfpq = spark.read.parquet("/FileStore/tables/Anac/V_OCORRENCIA_AMPLAParquet")
display(dfpq)

# COMMAND ----------

# MAGIC %md
# MAGIC Salvando parquet com Compactado

# COMMAND ----------

df.write \
  .format('parquet') \
  .mode("overwrite") \
  .option("compression", "gzip") \
  .save("/FileStore/tables/Anac/Parquet_zip")
# pegai o mesmo script para salvar apenas inseri  option de compression


# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/Anac"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/Anac/Parquet_zip/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Lendo Arquivo Parquet compactado
# MAGIC
# MAGIC

# COMMAND ----------

dfParquetZip = spark.read\
    .format("parquet")\
    .option("compression", "gzip")\
    .load("dbfs:/FileStore/tables/Anac/Parquet_zip")
display(dfParquetZip)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### tempo de Execução parquet x json

# COMMAND ----------

#Json
df_json = spark.read.json("dbfs:/FileStore/tables/Anac/V_OCORRENCIA_AMPLA.json")
display(df_json)

# COMMAND ----------

#parquet
dfpq = spark.read.parquet("/FileStore/tables/Anac/V_OCORRENCIA_AMPLAParquet")
display(dfpq)

