# Databricks notebook source
# MAGIC %md
# MAGIC ##### DBFS e dbutils Documentação 
# MAGIC https://docs.databricks.com/pt/dbfs/index.html
# MAGIC
# MAGIC https://docs.databricks.com/pt/dev-tools/databricks-utils.html

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Arquitetura azure
# MAGIC
# MAGIC https://learn.microsoft.com/pt-br/azure/architecture/solution-ideas/articles/azure-databricks-modern-analytics-architecture
# MAGIC
# MAGIC https://learn.microsoft.com/pt-br/azure/architecture/solution-ideas/articles/ingest-etl-stream-with-adb

# COMMAND ----------

#carregar csv de clientes primeiro para demosntrar , depois carregar bikes no lugar errado dem oganização depois apagar e carregar certo e seguir com curso 

# COMMAND ----------

/dbfs/FileStore/tables/Clientes.csv

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------


#Traduzido


# COMMAND ----------

fsutils
cp(from: String, to: String, recurse: boolean = false): boolean -> Copia um arquivo ou diretório, possivelmente entre sistemas de arquivos
head(file: String, maxBytes: int = 65536): String -> Retorna até os primeiros bytes 'maxBytes' do arquivo fornecido como uma String codificada em UTF-8
ls(dir: String): Seq -> Lista o conteúdo de um diretório
mkdirs(dir: String): boolean -> Cria o diretório fornecido se ele não existir, criando também quaisquer diretórios pais necessários
mv(from: String, to: String, recurse: boolean = false): boolean -> Move um arquivo ou diretório, possivelmente entre sistemas de arquivos
put(arquivo: String, conteúdo: String, overwrite: boolean = false): boolean -> Grava a String fornecida em um arquivo, codificado em UTF-8
rm(dir: String, recurse: boolean = false): boolean -> Remove um arquivo ou diretório

# COMMAND ----------

dbutils.fs.ls('/')

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/tables'))

# COMMAND ----------

dbutils.fs.head('/FileStore/tables/Clientes.csv')

# COMMAND ----------

# inserir varios dados em pasta errada como ajustar 
display(dbutils.fs.ls('/FileStore/'))

# COMMAND ----------

# Criando uma lista e deletando arquivos
# Lista de caminhos de arquivo a serem excluídos
excluir = [
    "/FileStore/categories.csv",
    "/FileStore/customers.csv",
    "/FileStore/order_items.csv",
    "/FileStore/orders.csv",
    "/FileStore/products.csv",
    "/FileStore/staffs.csv",
    "/FileStore/stocks.csv",
    "/FileStore/stores.csv",
]

# for (passar por todos arquivos)  e excluir
for apagar_arquivos in excluir:
    dbutils.fs.rm(apagar_arquivos)

# COMMAND ----------


#Verificando tabelas Para criar novas pastas
display(dbutils.fs.ls('/FileStore/tables'))




# COMMAND ----------

#criando nova pasta 
dbutils.fs.mkdirs('/FileStore/tables/Bikes')


# COMMAND ----------

display(dbutils.fs.ls('/FileStore/tables/Bikes'))


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Visualizando os dados

# COMMAND ----------

dbfs:/FileStore/tables/Bikes/customers.csv
/dbfs/FileStore/tables/Bikes/customers.csv

# COMMAND ----------

# Ler o arquivo CSV e criar um DataFrame
spark.read.csv("/FileStore/tables/Bikes/customers.csv", header=True, inferSchema=True)


# COMMAND ----------

display(spark.read.csv("/FileStore/tables/Bikes/customers.csv", header=True, inferSchema=True))

# COMMAND ----------

display(spark.read.csv("dbfs:/FileStore/tables/Bikes/customers.csv", header=True, inferSchema=True))
#header = primeira linha é o cabeçalho?
#inferSchema = inserir automaticamente os tipos de dados(numero,texto,data etc....)

# COMMAND ----------

#salvando dados em um DF(DataFlame)
df = spark.read.csv("dbfs:/FileStore/tables/Bikes/customers.csv", header=True, inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Tipos de visualizações 

# COMMAND ----------

df

# COMMAND ----------

df.show()

# COMMAND ----------

display(df)

# COMMAND ----------

#Ver algumas colunas sem salvar em um DF
colunas = ["customer_id", "first_name", "email", "state"]
display(df.select(colunas))

# COMMAND ----------

df.select('customer_id','email').show()

# COMMAND ----------

display(df.select('customer_id','email'))   

# COMMAND ----------

#salvando em um novo DF ou subscrevendo
colunas = ["customer_id", "first_name", "email", "state"]
dfFiltrado = df.select(colunas)

# COMMAND ----------

dfFiltrado.show(5)

# COMMAND ----------

df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Sobrescrevendo DF

# COMMAND ----------

df =  df.select('first_name','email','city')

# COMMAND ----------

df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Localizando Base de Dados Para Treino
# MAGIC
# MAGIC https://www.kaggle.com

# COMMAND ----------

dbutils.fs.ls('/')

# COMMAND ----------

dbutils.fs.ls('/databricks-datasets')

# COMMAND ----------

display(dbutils.fs.ls('/databricks-datasets/wine-quality/'))

# COMMAND ----------

spark.read.csv('dbfs:/databricks-datasets/wine-quality/winequality-white.csv',header=True , inferSchema=True , sep=';').show(5)

# COMMAND ----------

display(dbutils.fs.ls('/databricks-datasets/COVID/coronavirusdataset/'))

# COMMAND ----------

Arquivo ="dbfs:/databricks-datasets/COVID/coronavirusdataset/SearchTrend.csv"
spark.read.csv(Arquivo,header=True , inferSchema=True ).show(5)

