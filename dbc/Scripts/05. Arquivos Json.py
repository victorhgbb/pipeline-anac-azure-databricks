# Databricks notebook source
# pesquisar google dados gov dados publicos
# https://dados.gov.br/dados/conjuntos-dados
# Defesa e segurança Formato Json > Recursos
# https://dados.gov.br/dados/conjuntos-dados/ocorrncias-aeronuticas

# COMMAND ----------

# MAGIC %md
# MAGIC #####Sites
# MAGIC https://jsonviewer.stack.hu/
# MAGIC
# MAGIC https://codebeautify.org/jsonviewer
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #####Ler um arquivo json
# MAGIC https://spark.apache.org/docs/latest/sql-data-sources-json.html

# COMMAND ----------

df = spark.read.json("dbfs:/FileStore/tables/Anac/V_OCORRENCIA_AMPLA.json")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Renomear Colunas

# COMMAND ----------

#Renomear colunas caso necessário , obs eu subresqcrevi o df original , poderiar criar um novo passando outro nome no inio\
    #df_novo = df.withCo...................... e por ai vai 
        
df = df.withColumnRenamed("Aerodromo_de_Destino","Destino") \
            .withColumnRenamed("Aerodromo_de_Origem", "Origem")\
            .withColumnRenamed("Classificacao_da_Ocorrência","Classificação") 
display (df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Salvar arquivo json zipado

# COMMAND ----------

 # write = Escrever\Gravar |||   Read = Ler
df.write \
    .format("json") \
    .option("compression", "gzip") \
    .mode("overwrite") \
    .save("/FileStore/tables/Anac/json_zip")


# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/Anac"))
#json 20 MB
#Json compromido 3 MB
#pesquisar no google por conversor de bytes para mb mais basicamente 1 milhao de Byte = 1 Megabyte

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Lendo Json Compactado

# COMMAND ----------

 # write = Escrever\Gravar |||   Read = Ler
dfCompresao = spark.read \
        .option("compression", "gzip") \
        .json("/FileStore/tables/Anac/json_zip/") 

display(dfCompresao)

# COMMAND ----------

#Obs poderia passar o caminho do arquivo em uma variavel
Caminho="/FileStore/tables/Anac/json_zip/"
df = spark.read \
        .option("compression", "gzip") \
        .json(Caminho) 

display(df)
