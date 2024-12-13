# Databricks notebook source
# MAGIC %md
# MAGIC ###### Tipos de Dados
# MAGIC Referencia de Documetação https://spark.apache.org/docs/latest/api/python/reference/index.html

# COMMAND ----------

df = spark.read.json("dbfs:/FileStore/tables/Anac/V_OCORRENCIA_AMPLA.json")
display(df)

# COMMAND ----------

orders_df       = spark.read.\
    format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("/FileStore/tables/Bikes/orders.csv")
display(orders_df)

# COMMAND ----------

orders_df.printSchema()

# COMMAND ----------

#(nullable = true), isso indica que alguns dos registros nessa coluna podem ter valores nulos

# COMMAND ----------

#Tipos De Dados
"""
string ou varchar: Texto.
integer ou int:    Numero inteiro.
long:              Numero inteiro longo.
double:            precisão dupla.
float:             precisão simples.
decimal:           decimal.
timestamp:         Data e hora.
date:              Somente a Data.
boolean:           booleana (True e False) 0=False e 1=True.

"""

# COMMAND ----------

#ver nomes das colunas 
print(df.columns)

# COMMAND ----------

for Loop in df.columns:
    print(Loop)



# COMMAND ----------

# MAGIC %md
# MAGIC ######Selecionando apenas algumas colunas do DF

# COMMAND ----------

df = spark.read.json("dbfs:/FileStore/tables/Anac/V_OCORRENCIA_AMPLA.json")
display(df)

# COMMAND ----------

print(df.columns)

# COMMAND ----------

display( df['Classificacao_da_Ocorrência','Danos_a_Aeronave','UF','Municipio','Ilesos_Passageiros']) 


# COMMAND ----------

#salvando em um novo Df
Filtrado =  df['Classificacao_da_Ocorrência','Danos_a_Aeronave','UF','Municipio','Ilesos_Passageiros']
display(Filtrado)

# COMMAND ----------

df.columns

# COMMAND ----------

#Criando uma variavel com as colunas (lista)
ColunasSelecionadas = [
'Categoria_da_Aeronave',
'Classificacao_da_Ocorrência',
'Danos_a_Aeronave',
'Data_da_Ocorrencia',
'Descricao_do_Tipo',
'Fase_da_Operacao'
]

display( df[ColunasSelecionadas])

# COMMAND ----------

#Salvando em um Df 
ColunasSelecionadas = [
'Categoria_da_Aeronave',
'Classificacao_da_Ocorrência',
'Danos_a_Aeronave',
'Data_da_Ocorrencia',
'Descricao_do_Tipo',
'Fase_da_Operacao'
]

Novo = df[ColunasSelecionadas]
display(Novo)

# COMMAND ----------

print(df.columns)

# COMMAND ----------

#Método Select
display(df.select('Danos_a_Aeronave','Data_da_Ocorrencia','Operador', 'UF') )

# COMMAND ----------

#Salvar em Df Novo Denovo rsrsrsrs (tirar fora o display )


NovoDenovo = df.select('Operacao','Tipo_de_Aerodromo', 'Tipo_de_Ocorrencia', 'UF')
display(NovoDenovo)

# COMMAND ----------

# MAGIC %md
# MAGIC ######Criando novas Colunas 

# COMMAND ----------

df = spark.read.json("dbfs:/FileStore/tables/Anac/V_OCORRENCIA_AMPLA.json")
display(df)

# COMMAND ----------

display(df['Classificacao_da_Ocorrência','UF','Municipio'])

# COMMAND ----------

teste = df['Classificacao_da_Ocorrência','UF','Municipio']

# COMMAND ----------

display(teste)

# COMMAND ----------

from pyspark.sql.functions import lit
display(teste.withColumn("TesteTexto",lit(10)    ) )

# COMMAND ----------

#Nova coluna com minicipio - UF obs p -  ou qualqer caractere que inserir pode dar erro por nao fazer part do concat 
#Lit = Trazer o valor literal para a coluna ex uma coluna com todas as linhas com o valor 10 ou com a  palava Brasil 

from pyspark.sql.functions import concat,lit
display(teste.withColumn("Municipio_UF", concat(teste.Municipio, lit(" - "),teste["UF"]  ) )  )

# COMMAND ----------

#Salvando em Df
from pyspark.sql.functions import concat,lit
Tratado = teste.withColumn("Municipio_UF", concat(teste.Municipio, lit(" - "),df["UF"]  ) )
display(Tratado)



# COMMAND ----------

#Inserindo mais de uma coluna no mesmo comando exemplo uma coluna com país , outra com Municipio em letra miniscula 
from pyspark.sql.functions import concat,lit,lower
Tratado2 = teste\
    .withColumn("Municipio_UF", concat(teste.Municipio, lit(" - "),df["UF"]  ) )\
    .withColumn("País",lit("Brasil")  )\
    .withColumn("Minisculo", lower(teste.Municipio  ) )

display(Tratado2)


# COMMAND ----------

# MAGIC %md
# MAGIC ######Criando Coluna Condicional
# MAGIC

# COMMAND ----------

df = spark.read.json("dbfs:/FileStore/tables/Anac/V_OCORRENCIA_AMPLA.json")
display(df)

# COMMAND ----------

print(df.columns)

# COMMAND ----------

Regiao = df[ 'Danos_a_Aeronave','UF']
display(Regiao)

# COMMAND ----------

#Estado \ Regiao 
"""
    'AC': 'Norte',
    'AL': 'Nordeste',
    'AP': 'Norte',
    'AM': 'Norte',
    'BA': 'Nordeste',
    'CE': 'Nordeste',
    'ES': 'Sudeste',
    'GO': 'Centro-Oeste',
    'MA': 'Nordeste',
    'MT': 'Centro-Oeste',
    'MS': 'Centro-Oeste',
    'MG': 'Sudeste',
    'PA': 'Norte',
    'PB': 'Nordeste',
    'PR': 'Sul',
    'PE': 'Nordeste',
    'PI': 'Nordeste',
    'RJ': 'Sudeste',
    'RN': 'Nordeste',
    'RS': 'Sul',
    'RO': 'Norte',
    'RR': 'Norte',
    'SC': 'Sul',
    'SP': 'Sudeste',
    'SE': 'Nordeste',
    'TO': 'Norte'
"""


# COMMAND ----------

from pyspark.sql.functions import when
Regiao = Regiao.withColumn("Regiao", when(df.UF == "MG", "Sudeste").otherwise("Outra Regiao"))
display(Regiao)
#When = quando 
#otherwise = outra Forma (senão)

# COMMAND ----------

#mais de uma condição  isin - esta em (Estra dentro de algo)  Muito usada para verificar se os valores de uma coluna ou expressão estão contidos em um conjunto de valores especificados
from pyspark.sql.functions import when
Regiao = Regiao\
    .withColumn("Regiao", when(df.UF.isin ("MG","SP","RJ","ES"), "Sudeste").otherwise("Outra Regiao"))
display(Regiao)

# COMMAND ----------

#Fazendo o mesmo Para regiao Sul
"""
    'PR': 'Sul',
    'RS': 'Sul',
    'SC': 'Sul',
"""
from pyspark.sql.functions import when
Regiao = Regiao\
    .withColumn("Regiao",\
        when(df.UF.isin ("MG","SP","RJ","ES"), "Sudeste")\
        .when(df.UF.isin ("PR","RS","SC"), "Sul" )
        .otherwise("Outra Regiao"))
display(Regiao)




# COMMAND ----------

#Treine com as Demais 

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Coluna Condicional de valor 

# COMMAND ----------

#Coluna Condicional de valor 
orders_df       = spark.read.\
    format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("/FileStore/tables/Bikes/orders.csv")
display(orders_df)

# COMMAND ----------

#Nova coluna com descrição Status de Ordem 
from pyspark.sql.functions import when

orders_df = orders_df.withColumn("DescStatus",when (orders_df.order_status == 1 , "Pendente").otherwise("Sem Status"))
display(orders_df)

# COMMAND ----------

# inserindo Varias condições (Simulando Status de produção)
"""
1= Pendente
2= Em produção
3= Em rota de entrega
4= Finalizado
"""
from pyspark.sql.functions import when

orders_df = orders_df.withColumn("DescStatus",\
    when (orders_df.order_status == 1 , "Pendente")\
    .when (orders_df.order_status == 2 , "Em produção")\
    .when (orders_df.order_status == 3 , "Em rota de entrega")\
    .otherwise("Finalizado"))

display(orders_df)



# COMMAND ----------

order_items       = spark.read.\
    format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("/FileStore/tables/Bikes/order_items.csv")
display(order_items)

# COMMAND ----------

# caso for um valor especifico traga uma informação 
from pyspark.sql.functions import when

order_items = order_items.withColumn("DescStatus",when (order_items.list_price == 599.99 , "valor procurado").otherwise("Não"))
display(order_items)

# COMMAND ----------

#Filtro do Valor procurado
display(order_items.filter(order_items.DescStatus =="valor procurado"))

# COMMAND ----------

#Sumilado  Com varios Valores
"""
0 a 500    = Barato
501 a 1500 = Médio
> 1500     = Caro
"""
from pyspark.sql.functions import when
Taxa = order_items.withColumn("TaxaCliente",\
    when((order_items.list_price >= 0) & (order_items.list_price <= 500), "Barato")\
    .when((order_items.list_price >= 501) & (order_items.list_price <= 1500), "Médio")\
    #.when(order_items.list_price > 1500, "Caro")\
    .otherwise("Caro"))

display(Taxa)


# COMMAND ----------

#Fazendo Filtro para validar 
display(Taxa.filter( Taxa.TaxaCliente == "Caro"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Comando Sql para Selecionar Colunas e Criar Novas 

# COMMAND ----------

df = spark.read.json("dbfs:/FileStore/tables/Anac/V_OCORRENCIA_AMPLA.json")
display(df)

# COMMAND ----------

# Inserindo em uma tabela temporaria para 
df.createOrReplaceTempView("Anac")


# COMMAND ----------

print(df.columns)

# COMMAND ----------

#Selecionando algumas colunas
Novodf = spark.sql(
"""
select
Danos_a_Aeronave
,Municipio
,UF
 from anac 

"""
)
display(Novodf)

# COMMAND ----------

#Coluna condicional  "MG","SP","RJ","ES"=  "Sudeste"
Novodf = spark.sql(
"""
select
Danos_a_Aeronave
,Municipio
,UF
,Case
   when UF in ('MG','SP','RJ','ES')  then 'Sudeste'
   else 'Sem Regiao'
   end as Regiao

from anac 

"""
)
display(Novodf)

# COMMAND ----------

#Coluna condicional  "MG","SP","RJ","ES"=  "Sudeste" e 'PR',RS,SC = Sul 

Novodf = spark.sql(
"""
select
Danos_a_Aeronave
,Municipio
,UF
,Case
   when UF in ('MG','SP','RJ','ES')  then 'Sudeste'
   when UF in ('PR','RS','SC')  then 'Sul'
   else 'Sem Regiao'
   end as Regiao

from anac 

"""
)
display(Novodf)

# COMMAND ----------

#Filtro para Validar no Df e voce faça para os demais para treinar (pode ser filtro na consulta tambem )
"""
Case= Caso
when= Quando
else= Senão
end= Fim 
"""
display(Novodf.filter(Novodf.Regiao ==  'Sudeste' ))



# COMMAND ----------

# MAGIC %md
# MAGIC ######Criando coluna condicional de Valor 

# COMMAND ----------

order_items       = spark.read.\
    format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("/FileStore/tables/Bikes/order_items.csv")
display(order_items)

# COMMAND ----------

#Inserindo em tabela temporaria para rodar SQL
order_items.createOrReplaceTempView("Vendas")

# COMMAND ----------

# Consulta Para identificar tipo de cliente a partir da quantidade de compra 
Testevendas = spark.sql(
"""
select *
,Case 
    when quantity = 1 then 'Mâo de Vaca'
    --when quantity = 2 then 'Top'
    else 'Top'
    end TipoCliente
from Vendas
"""

)
display(Testevendas)

# COMMAND ----------

#Classificação por faixa de Preço
"""
0 a 500    = Barato
501 a 1500 = Médio
> 1500     = Caro
"""

Testevendas = spark.sql(
"""
select *
, case 
    when list_price between   0 and 500 then 'Barato'
    when list_price between 501 and 1500 then 'Médio'
    when list_price > 1500 then 'Médio'
    else 'Sem Perfil' 
    end as PerfilProduto
 from Vendas
"""

)
display(Testevendas)
