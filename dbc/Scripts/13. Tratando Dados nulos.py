# Databricks notebook source
# MAGIC %md
# MAGIC ###### Apagando dados nulos 
# MAGIC https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrame.dropna.html

# COMMAND ----------

"""
DataFrame.dropna(how='any', thresh=None, subset=None)

dropna(): se usar assim sem critérios ele apaga todas a linhas se em alguma delas tiver dados nulos
obs: tenho 40 colunas e em qalquer 1 das colunas eu tiver dados nulos ele vai apagar toda a linha mesmo que as outras colunas nao tenha dado nulos

how:
how='any' significa que uma linha será removida se ela contiver pelo menos um valor nulo em qualquer uma de suas colunas.
how='all' significa que uma linha será removida somente se todos os seus valores forem nulos.

thresh:
a linha será mantida se ela tiver pelo menos x valores não nulos
Por exemplo, se thresh=2, a linha mantida se ela tiver pelo menos 2 valores não nulos. Se tiver menos de 2 valores não nulos, a linha será removida.


subset:
Se especificado, o método dropna() remove as linhas que contêm valores nulos apenas nas colunas listadas em subset.
Por exemplo, se subset=['coluna1', 'coluna2'], uma linha será removida se tiver valores nulos apenas em 'coluna1' e 'coluna2', enquanto outros valores nulos em diferentes colunas não afetarão a remoção.

"""

# COMMAND ----------

df = spark.read.json("dbfs:/FileStore/tables/Anac/V_OCORRENCIA_AMPLA.json")
display(df)

# COMMAND ----------

#salvando em um novo df
sem_nulo = df.dropna()
display(sem_nulo)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Usando parametros

# COMMAND ----------

display(df)

# COMMAND ----------

"""
how='all' significa que uma linha será removida  se tiver nulos em todas as linhas 
how='any' significa que uma linha será removida se ela contiver pelo menos um valor nulo em qualquer uma de suas colunas.
obs 'any' é a mesma coisa que df.dropna()
"""
display(df.dropna(how='all'))

# COMMAND ----------

"""
thresh:
a linha será mantida se ela tiver pelo menos 30 valores não nulos. Se tiver menos de 30 valores não nulos, a linha será removida.
"""
display(df.dropna(thresh=30))

# COMMAND ----------

"""
subset:
Por exemplo, se subset=['coluna1', 'coluna2'], se tiverdados nulos nessas colunas de referencia a linha será apagada se nessa coluna nao tiver nulo por mais que as outras tenham os dados nao serao removidos 
Vamos treinar mais nesses com exemplos 
"""

#exemplo vou realizar analise de Aerodromo_de_Origem o que for nulo pra min nao importa entao posso passar fogo nos que estao nulos por que vao avacalhar minha minha base

display(df.dropna(subset='Aerodromo_de_Origem'))


# COMMAND ----------

#2° exemplo vou realizar analise de Aerodromo_de_Origem  e Danos_a_Aeronave se no tem registro em qualquer um desses 2 o restante dos dados pra min nao importa bora passar fogo tambem 
#Obs como sao dados de 2 colunas ou mais tem que passar em uma lista entre []
display(df.dropna(subset=['Aerodromo_de_Origem','Danos_a_Aeronave']))

# COMMAND ----------

print(df.columns)

# COMMAND ----------

#Exemplo 3 passando fogo em nulos de varias colunas , vamos fazer chique com lista de colunas em variaveis 

colunas_ref=['Regiao','Aerodromo_de_Origem','Municipio','Numero_da_Ocorrencia','Danos_a_Aeronave']
display(df.dropna(subset=colunas_ref))

# COMMAND ----------

#Salvando Dados em um novo df

df_tratado= df.dropna(subset=['Aerodromo_de_Origem','Danos_a_Aeronave'])
display(df_tratado)

# COMMAND ----------

# MAGIC %md
# MAGIC ######Filtrando dados nulos 

# COMMAND ----------

df = spark.read.json("dbfs:/FileStore/tables/Anac/V_OCORRENCIA_AMPLA.json")
display(df)

# COMMAND ----------

#Filtrando dados nulos 
display( df.where(df.Aerodromo_de_Destino.isNull()) )

# COMMAND ----------

#Filtrando dados nao nulos 
display( df.where(df.Aerodromo_de_Destino.isNotNull()) )

# COMMAND ----------

#salvando em um novo df
destino_nao_nulo = df.where(df.Aerodromo_de_Destino.isNotNull()) 
display(destino_nao_nulo)

# COMMAND ----------

# contando nulo em coluna 
display( df.where(df.Aerodromo_de_Destino.isNull()).count() )

# COMMAND ----------

print(df.columns)

# COMMAND ----------

coluna = 'Operacao'
display( df.where(df[coluna].isNull()).count() )


# COMMAND ----------


lista_contagem_nulos = [(coluna, df.where(df[coluna].isNull()).count()) for coluna in df.columns]
display(lista_contagem_nulos)


# COMMAND ----------

# MAGIC %md
# MAGIC ######Substituindo dados nulos 

# COMMAND ----------

df = spark.read.json("dbfs:/FileStore/tables/Anac/V_OCORRENCIA_AMPLA.json")
display(df)

# COMMAND ----------

print(df.columns)

# COMMAND ----------

coluna = 'Aerodromo_de_Origem'
display( df.where(df[coluna].isNull()).count() )

# COMMAND ----------

"""
dropna = apagar nulos 
fillna = Substituir nulo 
"""

# COMMAND ----------

#Substituindo dados nulos de uma coluna especifica (se nao passar a coluna no  subset ele faz para todas as colunas )
display( df.fillna('Sem Origem', subset=['Aerodromo_de_Origem'])  )

# COMMAND ----------

#obs sem subset = substitui tudos nulos sem criterio 
display( df.fillna('Sem Origem')  )

# COMMAND ----------

teste.printSchema()

# COMMAND ----------

#Substituindo dados nulos de uma coluna especifica (atenção com formato de dados )
teste= df.fillna("0", subset=['Lesoes_Desconhecidas_Passageiros'])
display(teste)

# COMMAND ----------

#substitui por 0 em string primeiro e depois converter a coluna pra int (depois vamos fazer tudo junto)
teste = teste.withColumn('Lesoes_Desconhecidas_Passageiros', teste.Lesoes_Desconhecidas_Passageiros.cast("int"))
display(teste)

# COMMAND ----------

teste.printSchema()

# COMMAND ----------

# Fazendo Conversoes usando withColumn com fillna (fazer o mesmo com as outras colunas de lesoes)
novo = df\
    .withColumn("Lesoes_Desconhecidas_Terceiros", df.Lesoes_Desconhecidas_Terceiros.cast("int"))\
    .fillna(0, subset=["Lesoes_Desconhecidas_Terceiros"])
display(novo)


# COMMAND ----------

# MAGIC %md
# MAGIC ###### Tratando nulos SQL
# MAGIC

# COMMAND ----------

df = spark.read.json("dbfs:/FileStore/tables/Anac/V_OCORRENCIA_AMPLA.json")
df.createOrReplaceTempView("anac")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from anac

# COMMAND ----------

# MAGIC %sql
# MAGIC --Comando is null e is not null em filtro 
# MAGIC select * from anac
# MAGIC where Aerodromo_de_Origem is null
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ver descições da tabela 
# MAGIC DESCRIBE anac

# COMMAND ----------

# MAGIC %sql
# MAGIC --exibir nome das colunas  
# MAGIC SHOW COLUMNS FROM anac
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- tratando nulos com método coalesce e ifnull 
# MAGIC select 
# MAGIC coalesce(Aerodromo_de_Destino,"Sem Registro") as Aerodromo_de_Destino
# MAGIC ,ifnull(Aerodromo_de_Origem,"Sem Origem")   Aerodromo_de_Origem
# MAGIC ,ifnull(CLS, "Sem Registro") as CLSTeste
# MAGIC ,Categoria_da_Aeronave
# MAGIC ,Danos_a_Aeronave
# MAGIC ,Data_da_Ocorrencia
# MAGIC ,UF
# MAGIC from anac

# COMMAND ----------

#Salvando  consulta tratada em um Df novo

novodf= spark.sql(
"""
select 
coalesce(Aerodromo_de_Destino,"Sem Registro") as Aerodromo_de_Destino
,ifnull(Aerodromo_de_Origem,"Sem Origem")   Aerodromo_de_Origem
,ifnull(CLS, "Sem Registro") as CLSTeste
,Categoria_da_Aeronave
,Danos_a_Aeronave
,Data_da_Ocorrencia
,UF
from anac
"""

)

display(novodf)
