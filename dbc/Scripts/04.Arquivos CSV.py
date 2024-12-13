# Databricks notebook source
# pesquisar google dados gov dados publicos
# https://dados.gov.br/dados/conjuntos-dados
# Defesa e segurança Formato csv > Recursos
# https://dados.gov.br/dados/conjuntos-dados/ocorrncias-aeronuticas


# COMMAND ----------

# MAGIC %md
# MAGIC #####Documentação CSV
# MAGIC ######Pesquisar csv documentation databricks\
# MAGIC https://docs.databricks.com/pt/external-data/csv.html

# COMMAND ----------

#Criar nova pasta
dbutils.fs.mkdirs("/FileStore/tables/Anac")


# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/Anac/"))

# COMMAND ----------

#modo anterior
# Caminho para o arquivo CSV
caminho_csv = "dbfs:/FileStore/tables/Anac/V_OCORRENCIA_AMPLA.csv"

# Ler o arquivo CSV em um DataFrame  modo antigo sem tratamento
df_csv = spark.read.csv(caminho_csv)
display(df_csv)

# COMMAND ----------

caminho_csv = "dbfs:/FileStore/tables/Anac/V_OCORRENCIA_AMPLA.csv"

#Passando Opções avançadas 
df = spark.read\
    .format("csv")\
    .option("skipRows", 1)\
    .option("header", True)\
    .option("sep", ";")\
    .option("inferSchema", True)\
    .load(caminho_csv)
display(df)


# COMMAND ----------

#Treino obs(estrutura vai server para outros formatos , vamos ver nas poximas aulas)
df = spark.read\                    # Modo de Leitura do spark
    .format("csv")\                 # Formato do Arquivo
    .option("skipRows", 1)\         # Quantas Linhas Pular
    .option("header", True)\        # 1° linha é o Cabeçalho
    .option("sep", ";")\            # Separador do CSV
    .option("inferSchema", True)\   # inserir os tipos de Dados automaticamente
    .load("/FileStore/tables/Anac/V_OCORRENCIA_AMPLA.csv")              # Comando para Carregar o Arquivo
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Mais Links
# MAGIC https://spark.apache.org/docs/latest/sql-data-sources-csv.html

# COMMAND ----------

# MAGIC %md
# MAGIC #####Fazendo Ganbiarra

# COMMAND ----------

#Csv ContatoShar da aula anterior (3. Transformado Consulta de Tabela SQL em DataFlame)
caminho_csv = "dbfs:/FileStore/tables/DiferentesSaidas/ContatoSha.csv"
df_csv = spark.read.csv(caminho_csv, header=False, inferSchema=True)
display(df_csv)

# COMMAND ----------

# Renomenado colunas
df_csv = df_csv.withColumnRenamed("_c0", "ID") \
               .withColumnRenamed("_c1", "Nome") \
               .withColumnRenamed("_c2", "Telefone") \
               .withColumnRenamed("_c3", "E-mail")

display(df_csv)


# COMMAND ----------

# Exemplo d como ficaria sem o \Enter (Imagina com umas 30 ,50 colunas)
df_csv = df_csv.withColumnRenamed("_c0", "ID").withColumnRenamed("_c1", "Nome").withColumnRenamed("_c2", "Telefone").withColumnRenamed("_c3", "E-mail").withColumnRenamed("_c1", "Nome").withColumnRenamed("_c2", "Telefone").withColumnRenamed("_c3", "E-mail").withColumnRenamed("_c1", "Nome").withColumnRenamed("_c2", "Telefone").withColumnRenamed("_c3", "E-mail").withColumnRenamed("_c1", "Nome").withColumnRenamed("_c2", "Telefone").withColumnRenamed("_c3", "E-mail").withColumnRenamed("_c1", "Nome").withColumnRenamed("_c2", "Telefone").withColumnRenamed("_c3", "E-mail")
display(df_csv)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Salvando Arquivo em Tamanho reduzido
# MAGIC https://spark.apache.org/docs/latest/sql-data-sources-csv.html
# MAGIC
# MAGIC Pesquisar no google conversor bytes para mb

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/Anac/V_OCORRENCIA_AMPLA.csv"))

# COMMAND ----------

#Salvando um DataFlame
caminho_csv = "/FileStore/tables/Anac/V_OCORRENCIA_AMPLA.csv"
df = spark.read\
    .format("csv")\
    .option("skipRows", 1)\
    .option("header", True)\
    .option("sep", ";")\
    .option("inferSchema", True)\
    .load(caminho_csv)
display(df)

# COMMAND ----------

#Salvando em formato comprimido (melhor para questoes de armazenamento em nuvem onde paga pelo tanto que armazenar)
#write significa Gravar\Escrever 
#overwrite = sobrescrever o arquivo
df.write \
    .format("csv") \
    .option("compression", "gzip") \
    .option("header", "true") \
    .option("sep", ";") \
    .mode("overwrite") \
    .save("/FileStore/tables/Anac/V_OCORRENCIA_AMPLA-zip")

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/Anac"))

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/Anac/V_OCORRENCIA_AMPLA-zip"))
display(dbutils.fs.ls("/FileStore/tables/Anac/V_OCORRENCIA_AMPLA.csv"))

# COMMAND ----------

#pesquisar no google por conversor de bytes para mb mais basicamente 1 milhao de Byte = 1 Megabyte

# COMMAND ----------

# MAGIC %md
# MAGIC #####Lendo Arquivo salvo  em tamanho reduzido

# COMMAND ----------

#read = ler fazer a leitura do arquivo
dfnovo = spark.read \
            .option("compression", "gzip") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("sep", ";") \
            .csv("/FileStore/tables/Anac/V_OCORRENCIA_AMPLA-zip")

display(dfnovo)
