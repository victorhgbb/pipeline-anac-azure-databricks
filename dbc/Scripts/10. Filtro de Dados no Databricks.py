# Databricks notebook source
# MAGIC %md
# MAGIC #####Documentação
# MAGIC
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.filter.html
# MAGIC

# COMMAND ----------

df = spark.read.json("dbfs:/FileStore/tables/Anac/V_OCORRENCIA_AMPLA.json")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Filtros de texto 
# MAGIC Sintaxe (Filter)

# COMMAND ----------

#sintaxe . filter 1° localiza a coluna e faz o filtro como é string o criterio tem que ser emtre aspas simples ou duplas 
df.filter(df.UF == "MG") # nao rodar 

# COMMAND ----------

#vizualizando os dados sem salvar em um DF novo ou sobrescrever o antigo 
display(df.filter(df.UF == "MG"))

# COMMAND ----------

#Salvando em um novo DF
df_MG = df.filter(df.UF == "MG")
display(df_MG)


# COMMAND ----------

display(df_MG)

# COMMAND ----------

#Sobrescrevendo Df
df = df.filter(df.UF == "MG")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Vários Critérios

# COMMAND ----------

# Para voce treinar 
"""

& = "E" lógico  ,  tem que cumprir todos os criterios para retornar o resultado 1° filtro e o 2° filtro o que tiver resultado igual nos 2 criterios ele vai trazer 
| = "OU" lógico , quando obedcer um criterio ou o outro ele vai trazer 
!= (significa diferente)

Nomes 
& = e comercial
| = "barra vertical" ou "pipe". 

"""

# COMMAND ----------

df = spark.read.json("dbfs:/FileStore/tables/Anac/V_OCORRENCIA_AMPLA.json")
display(df)

# COMMAND ----------

#Obs Passar cada Criterio emtre parenteses com o operador lógico no meio para separar os criterios 
display(  df.filter((df.UF == "MG") & (df.Classificacao_da_Ocorrência == "Incidente"))           )

# COMMAND ----------

display(  df.filter((df.Danos_a_Aeronave == "leve") | (df.Classificacao_da_Ocorrência == "Incidente"))   )

# COMMAND ----------

#Mesmo exemplo com o &
display(  df.filter((df.Danos_a_Aeronave == "Leve") & (df.Classificacao_da_Ocorrência == "Incidente")) )

# COMMAND ----------

#Salvando em um Df novo
dfnovo = df.filter((df.Danos_a_Aeronave != "Leve") & (df.Classificacao_da_Ocorrência == "Incidente"))
display(dfnovo)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Metodo where

# COMMAND ----------

df = spark.read.json("dbfs:/FileStore/tables/Anac/V_OCORRENCIA_AMPLA.json")
display(df)

# COMMAND ----------

#sintaxe (where = Onde)
df.where(df.UF == 'SP') # nao rodar


# COMMAND ----------

#visualizando os dados nao estou salvando em lugar nenhum 
display(df.where(df.UF == 'SP'))

# COMMAND ----------

display(  df.where((df.Danos_a_Aeronave == "Leve") & (df.Classificacao_da_Ocorrência == "Incidente")) )

# COMMAND ----------

#Salvando em um DF
DfFiltrado =   df.where((df.Danos_a_Aeronave == "Leve") & (df.Classificacao_da_Ocorrência == "Incidente"))
display(DfFiltrado)

# COMMAND ----------

# MAGIC %md
# MAGIC ######Método Comando SQL
# MAGIC Documentação https://spark.apache.org/docs/latest/api/sql/

# COMMAND ----------

df = spark.read.json("dbfs:/FileStore/tables/Anac/V_OCORRENCIA_AMPLA.json")
display(df)

# COMMAND ----------

#Criar uma tabela temporaria

df.createOrReplaceTempView("temp_teste")
mg = spark.sql('''  
                            
SELECT 
"Classificacao_da_Ocorrência" as Classificacao
,Descricao_do_Tipo            as Tipo
,Fase_da_Operacao                Fase
,Municipio 
,UF                           as Estado
FROM temp_teste
WHERE UF = "MG" 
and Fase_da_Operacao = "Decolagem"


''')

display(mg)


# COMMAND ----------

#Exemplo sem as 3 aspas simples ou Duplas
mg = spark.sql(' SELECT * FROM temp_teste WHERE UF = "MG" ')
display(mg)

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS temp_teste;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Functions do Spark
# MAGIC Documentação functions
# MAGIC https://spark.apache.org/docs/latest/sql-ref-functions.html
# MAGIC
# MAGIC Documentação functions string
# MAGIC https://spark.apache.org/docs/latest/sql-ref-functions-builtin.html#string-functions

# COMMAND ----------

from pyspark.sql.functions import *  # importanto mais funções * traz tudo pode trocar o * por uma especifica 
"""
O PySpark fornece uma ampla gama de funções dentro do módulo pyspark.sql.functions.
Alguns Exemplos

col: Retorna uma coluna do DataFrame com base no nome fornecido.
Exemplo: col("nome_da_coluna")

concat: Concatena várias colunas em uma única coluna.
Exemplo: concat(col("coluna1"), lit(" "), col("coluna2"))

substring: Retorna uma parte de uma coluna de texto com base em índices.
Exemplo: substring(col("texto"), 1, 3)

length: Retorna o comprimento de uma coluna de texto.
Exemplo: length(col("texto"))

lower: Converte o texto de uma coluna para minúsculas.
Exemplo: lower(col("texto"))

upper: Converte o texto de uma coluna para maiúsculas.
Exemplo: upper(col("texto"))

trim:Remove espaços em branco em branco do início e do final de uma coluna de texto.
Exemplo: trim(col("texto"))

replace:Substitui um padrão por outro em uma coluna de texto.
Exemplo: replace(col("texto"), "old", "new")

regexp_replace: Substitui um padrão de expressão regular por outro em uma coluna de texto.
Exemplo: regexp_replace(col("texto"), "pattern", "replacement")

split:Divide uma coluna de texto em um array com base em um delimitador.
Exemplo: split(col("texto"), " ")

substring_index:Retorna as primeiras n ocorrências de um delimitador em uma coluna de texto.
Exemplo: substring_index(col("texto"), " ", 2)

concat_ws:Concatena colunas usando um delimitador especificado.
Exemplo: concat_ws(" ", col("coluna1"), col("coluna2"))

when: Realiza uma operação condicional em uma coluna com base em uma condição.
Exemplo: when(col("idade") >= 18, "adulto").otherwise("menor")

coalesce: Retorna a primeira coluna não nula de um conjunto de colunas.
Exemplo: coalesce(col("coluna1"), col("coluna2"), col("coluna3"))

Es'sas são apenas algumas das muitas funções' disponíveis no módulo pyspark.sql.functions

"""

# COMMAND ----------

df = spark.read.json("dbfs:/FileStore/tables/Anac/V_OCORRENCIA_AMPLA.json")
display(df)

# COMMAND ----------

from pyspark.sql.functions import *
#importando + funções do Spark


# COMMAND ----------

  # Retorna uma coluna do DataFrame passando nome como referencia
  display(df.filter(col('UF') == 'SP'))  

# display(df.filter(df.UF == 'SP')) Jeito aulas anteriores

# COMMAND ----------

# MAGIC %md
# MAGIC ######Filtrando pelo indice da coluna

# COMMAND ----------

#Localizando Indice em Dados com varias colunas 
Coluna = "Operador"
Indice = df.columns.index(Coluna)
display(Indice)

# COMMAND ----------

# Lista vazia para inserir os dados
ListaIndices = []

# Loop 
for Coluna in df.columns:
    Indice = df.columns.index(Coluna)
    ListaIndices.append((Coluna, Indice))

# Salvar lista em DF
df_indices = spark.createDataFrame(ListaIndices, ["Coluna", "Indice"])

display(df_indices)


# COMMAND ----------

# Filtrando pelo indice 
df_filtrado = df.filter(col(df.columns[43]) == "SCF-PP") 
display(df_filtrado)


# COMMAND ----------

# Filtro Personalizado  com variaveis , vai da muito certo e organizado 
Coluna = 'UF'
Filtro = 'SP'
display(df.filter(df[Coluna] == Filtro))



# COMMAND ----------

Coluna = 'Fase_da_Operacao'
Filtro = 'Decolagem'
display(df.filter(df[Coluna] == Filtro))


# COMMAND ----------

Coluna1 = 'Fase_da_Operacao'
Filtro1 = 'Decolagem'
Coluna2 = 'UF'
Filtro2 = 'MG'

display(df.filter( (df[Coluna1] == Filtro1) & (df[Coluna2] == Filtro2)   )  )

#Obs entre a criação da variavel e o criterio no filtro == ou !=

# COMMAND ----------

# MAGIC %md
# MAGIC ######Corrigindo erro de maiúsculas e minúsculas

# COMMAND ----------

df = spark.read.json("dbfs:/FileStore/tables/Anac/V_OCORRENCIA_AMPLA.json")
display(df)

# COMMAND ----------

display(df.filter(df.Fase_da_Operacao == 'Decolagem'))

# COMMAND ----------

display(df.filter(df.Fase_da_Operacao == 'decolagem'))

# COMMAND ----------

#Correção para bases grandes
#1° verificar comportamento 
#2° criar um padrão para as colunas

df.select("Fase_da_Operacao").distinct().show()


# COMMAND ----------

from pyspark.sql.functions import upper,lower #poderia ser * tambem para traser tudo 

# COMMAND ----------

# Upper  = Transforma dados da coluna em maiúsculas
display(df.select(upper("Fase_da_Operacao")).distinct())

# COMMAND ----------

# lower  = Transforma dados da coluna em minúsculas 
display(df.select(lower("Fase_da_Operacao")).distinct())

# COMMAND ----------

#Filtro sem erro eu resolvi  fazer minúsculas
display(df.filter(lower(df.Fase_da_Operacao) == 'decolagem'))

# COMMAND ----------

#salvar em um novo DF
novo_df = df.filter(lower(df.Fase_da_Operacao) == 'decolagem')
display(novo_df)
"""
notem que no DF Permanece como Maiusculo , usei somente no filtro para nao haver erro
O Df permanece original depois vamos aprender a alterar direto na base 
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Resolvendo erros de Digitação com Espaços Excessivos

# COMMAND ----------

Dados = [("Belo Horizonte ", "MG", 2512070),
        (" Belo Horizonte ", "MG", 12106920),
        ("Rio de Janeiro", "RJ", 6718903),
        (" Belo Horizonte", "MG", 20001151),
        ("Brasília", "DF", 3015268)]

Colunas = ["Cidade", "UF", "População"]

df = spark.createDataFrame(Dados, Colunas)
display(df)

# COMMAND ----------

display(df.filter(df.Cidade == 'Belo Horizonte'))

# COMMAND ----------

from pyspark.sql.functions import rtrim,ltrim,trim 
#poderia ser * tambem 
"""
trim:  Remove espaços em branco do início e do final de uma coluna de texto.
rtrim: Remove espaços em branco da direita 
ltrim: Remove espaços em branco da esquerda

"""


# COMMAND ----------

display(df.filter(ltrim(df.Cidade) == 'Belo Horizonte'))
# neste ele eliminou o espaço da esquerda e trouxe o resultado

# COMMAND ----------

display(df.filter(rtrim(df.Cidade) == 'Belo Horizonte'))
# neste ele eliminou o espaço da direita e trouxe o resultado

# COMMAND ----------

display(df.filter(  rtrim(ltrim(df.Cidade))    == 'Belo Horizonte'))

# COMMAND ----------

display(df.filter(trim(df.Cidade) == 'Belo Horizonte'))
#ja elimina os 2 o melhor de usar é esse 

# COMMAND ----------

BH =  df.filter(trim(df.Cidade) == 'Belo Horizonte')
display(BH)

# COMMAND ----------

from pyspark.sql.functions import trim 
#Novo DF avacalhado para simular 
Dados = [("Belo Horizonte ", "MG", 2512070),
        (" Belo Horizonte ", "MG", 12106920),
        ("Rio de Janeiro", "RJ", 6718903),
        (" Belo Horizonte", "MG", 20001151),
        ("Brasília", "DF", 3015268)]

Colunas = ["Cidade", "UF", "População"]

df = spark.createDataFrame(Dados, Colunas)
display(df)


# COMMAND ----------

#colocando Df Certinho
#withColumn adiciona ou substitui uma coluna (mesmo nome substitui nome diferente adiciona)
df = df.withColumn("Cidade", trim(df.Cidade))
display(df)

# COMMAND ----------

#Filtro sem usar o ltrim rtrim trim zrim gtrim jtrim rsrsrs ja foi tratado antes 
display(df.filter(df.Cidade == 'Belo Horizonte'))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Substituindo Dados avançados 
# MAGIC

# COMMAND ----------

# Simulando dados para os 27 estados do Brasil
dados = [
    ("Acre", "AC", 894470),
    ("Alagoas", "AL", 3337357),
    ("Amapá", "AP", 861773),
    ("Amazonas", "AM", 4207714),
    ("Bahia", "BA", 14930634),
    ("Ceará", "CE", 9132078),
    ("Distrito Federal", "DF", 3055149),
    ("Espírito Santo", "ES", 4018650),
    ("Goiás", "GO", 7113540),
    ("Maranhão", "MA", 7114598),
    ("Mato Grosso", "MT", 3526220),
    ("Mato Grosso do Sul", "MS", 2809394),
    ("Minas Gerais", "MG", 21168791),
    ("Pará", "PA", 8690745),
    ("Paraíba", "PB", 4039277),
    ("Paraná", "PR", 11433957),
    ("Pernambuco", "PE", 9616621),
    ("Piauí", "PI", 3273227),
    ("Rio de Janeiro", "RJ", 17366189),
    ("Rio Grande do Norte", "RN", 3534165),
    ("Rio Grande do Sul", "RS", 11422973),
    ("Rondônia", "RO", 1796460),
    ("Roraima", "RR", 631181),
    ("Santa Catarina", "SC", 7252502),
    ("São Paulo", "SP", 46289333),
    ("Sergipe", "SE", 2318822),
    ("Tocantins", "TO", 1590248),
]

colunas = ["Estado", "UF", "População"]

# Criando o DataFrame
df_estados = spark.createDataFrame(dados,colunas)
display(df_estados)


# COMMAND ----------

from pyspark.sql.functions import regexp_replace

# COMMAND ----------


df_estados = df_estados.withColumn("EstadoSemAcento", regexp_replace(df_estados.Estado, "á", "a")) 
display(df_estados)

#poderia localidar a coluna tambm df_estados["Estado"] ou pelo indice

# COMMAND ----------

from pyspark.sql.functions import translate
# tem que ser na mesma ordem 
acentos     = "áàãâéèêíìóòõôúùûç"
sem_acentos = "aaaaeeeiioooouuuc"

# Aplicar a substituição usando a função translate
df_estados = df_estados.withColumn("EstadoSemAcento", translate(df_estados.Estado, acentos, sem_acentos))

# Exibir o DataFrame
display(df_estados)

# COMMAND ----------

#Usando translatee lower pra nao ter problema de forma alguma tudo sem acento e minusculo
from pyspark.sql.functions import translate,lower
acentos     = "áàãâéèêíìóòõôúùûç"
sem_acentos = "aaaaeeeiioooouuuc"

df_estados = df_estados.withColumn("EstadoSemAcento", translate(df_estados.Estado, acentos, sem_acentos))
display(df_estados)

# COMMAND ----------

display( df_estados.filter(df_estados.EstadoSemAcento == 'Maranhao' ))

# COMMAND ----------

#Sendo um Analista Senior e usando todas as funções possiveis pra nao te encherem kkkkkk
#sem acento , tudo minusculo e sem espaços escessivos
from pyspark.sql.functions import translate,lower,trim
acentos     = "áàãâéèêíìóòõôúùûç"
sem_acentos = "aaaaeeeiioooouuuc"

df_estados = df_estados.withColumn("EstadoSemAcento", trim(lower(translate(df_estados.Estado, acentos, sem_acentos))) )
display(df_estados)

# COMMAND ----------

display(df_estados.filter(df_estados.EstadoSemAcento == 'sao paulo'))

# COMMAND ----------

#subtituindo coluna e nao adicionando uma nova 
from pyspark.sql.functions import translate,lower,trim
acentos     = "áàãâéèêíìóòõôúùûç"
sem_acentos = "aaaaeeeiioooouuuc"

df_estados = df_estados.withColumn("Estado", trim(lower(translate(df_estados.Estado, acentos, sem_acentos))) )
display(df_estados)

# COMMAND ----------

# MAGIC %md
# MAGIC ######Função Like

# COMMAND ----------

df = spark.read.json("dbfs:/FileStore/tables/Anac/V_OCORRENCIA_AMPLA.json")
display(df)

# COMMAND ----------

#passar filtro por trecho especifico 
# % a grosso modo significa todo o resto , voce vai entender na prática
resultado = df.filter(df.Descricao_do_Tipo.like("%AVE"))
display(resultado)

#Resumo pra ficar facil 
"""
%Texto%  = traz todo o resto pra frente e pra tras do texto
Texto%   = inicia com o texto e nao importa o que tem pra frente
%Texto   = finaliza com o texto nao importa o que tem para tras

"""


# COMMAND ----------

# MAGIC %md
# MAGIC ######Filtro de Valores

# COMMAND ----------

orders_df       = spark.read.\
    format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("/FileStore/tables/Bikes/orders.csv")
display(orders_df)

# COMMAND ----------

display(orders_df.filter(orders_df.order_status == 1) )

# COMMAND ----------

#sanlvando em um df simulando que 1 seja pedidos pendentes 
PedidosPendentes = orders_df.filter(orders_df.order_status == 1) 
display(PedidosPendentes)

# COMMAND ----------

#pedidos de uma loja especifica 
Loja2 = orders_df.filter(orders_df.store_id == 2) 
display(Loja2)

# COMMAND ----------

#pedidos diferentes de uma loja espefica
fora3 = orders_df.filter(orders_df.store_id != 3) 
display(fora3)

# COMMAND ----------

#Filtro composto exemplo pedidos da Loja 3 com status =1
StatusLoja3 = orders_df.filter( (orders_df.store_id == 3) & (orders_df.order_status == 1)  )  
display(StatusLoja3)


# COMMAND ----------

order_items       = spark.read.\
    format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("/FileStore/tables/Bikes/order_items.csv")
display(order_items)

# COMMAND ----------

# Exemplos de faixa de preços
Barato = order_items.filter(order_items.list_price < 300 ) 
display(Barato)

# COMMAND ----------

Caro = order_items.filter(order_items.list_price > 2000 ) 
display(Caro)

# COMMAND ----------

medio = order_items.filter((order_items.list_price >= 700) & (order_items.list_price <= 1200))
display(medio)

# COMMAND ----------

from pyspark.sql.functions import col
medio = order_items.filter((col("list_price") >= 700) & (col("list_price") <= 1200))
display(medio)
#exemplo Col 


# COMMAND ----------

# com variaveis 
ValorMinimo= 50
ValorMaximo= 100

df_resultado = order_items.filter((order_items.list_price >= ValorMinimo) & (order_items.list_price <= ValorMaximo))
display(df_resultado)

# COMMAND ----------

df = spark.read.json("dbfs:/FileStore/tables/Anac/V_OCORRENCIA_AMPLA.json")
display(df)
