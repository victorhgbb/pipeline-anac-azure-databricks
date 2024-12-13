# Databricks notebook source
"""
Descrição do projeto Gold (base final para analistas de estados tratarem suas bases)

1°:Selecionar somentes colunas que o cliente/setor pediu 
2°:Criar uma nova coluna que faz a soma de todas as lesoes 
3°:Renomear colunas para ficar mais intuitivas para o usuario final 
4°:Excluir dados que estados tenham a classificação [Indeterminado, Sem Registro, Exterior]
5°:inserir coluna com nome de atualização para usuario ver quando os dados foram atualizados 
6°:Salvar na camada Gold particionada por UF > 'Estado'

"""

# COMMAND ----------

# MAGIC %fs ls /mnt/Anac/

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/Anac/Silver/

# COMMAND ----------

df = spark.read.parquet("dbfs:/mnt/Anac/Silver/anac_silver.parquet/")
display(df)


# COMMAND ----------

print(df.columns)

# COMMAND ----------

#1°:Selecionar somentes colunas que o cliente/setor pediu
colunas = ['Aerodromo_de_Destino', 'Aerodromo_de_Origem','Classificacao_da_Ocorrência', 'Danos_a_Aeronave', 'Data_da_Ocorrencia','Municipio','UF','Regiao','Tipo_de_Aerodromo', 'Tipo_de_Ocorrencia','Lesoes_Desconhecidas_Passageiros', 'Lesoes_Desconhecidas_Terceiros', 'Lesoes_Desconhecidas_Tripulantes', 'Lesoes_Fatais_Passageiros', 'Lesoes_Fatais_Terceiros', 'Lesoes_Fatais_Tripulantes', 'Lesoes_Graves_Passageiros', 'Lesoes_Graves_Terceiros', 'Lesoes_Graves_Tripulantes', 'Lesoes_Leves_Passageiros', 'Lesoes_Leves_Terceiros', 'Lesoes_Leves_Tripulantes']

df = df.select(colunas)
display(df)


# COMMAND ----------

#2°:Criar uma nova coluna que faz a soma de todas as lesoes 
colunas_a_somar = [
    "Lesoes_Desconhecidas_Passageiros",
    "Lesoes_Desconhecidas_Terceiros",
    "Lesoes_Desconhecidas_Tripulantes",
    "Lesoes_Fatais_Passageiros",
    "Lesoes_Fatais_Terceiros",
    "Lesoes_Fatais_Tripulantes",
    "Lesoes_Graves_Passageiros",
    "Lesoes_Graves_Terceiros",
    "Lesoes_Graves_Tripulantes",
    "Lesoes_Leves_Passageiros",
    "Lesoes_Leves_Terceiros",
    "Lesoes_Leves_Tripulantes"
]
df = df.withColumn("Total_Lesoes", sum(df[somartudo] for somartudo in colunas_a_somar))
display(df)

# COMMAND ----------

print(df.columns)

# COMMAND ----------

# 3°:renomear colunas para ficar mais intuitivas para o usuario final 
df = df\
    .withColumnRenamed('Aerodromo_de_Destino', 'Destino')\
    .withColumnRenamed('Aerodromo_de_Origem', 'Origem')\
    .withColumnRenamed('Classificacao_da_Ocorrência', 'Classificacao')\
    .withColumnRenamed('Danos_a_Aeronave', 'Danos')\
    .withColumnRenamed('Data_da_Ocorrencia', 'Data')\
    .withColumnRenamed('UF', 'Estado')\
    .withColumnRenamed('Aerodromo_de_Destino', 'Destino')\
    .withColumnRenamed('Aerodromo_de_Destino', 'Destino')
display(df)


# COMMAND ----------

#~ Simbolo de negacao 
Filtro = ["Indeterminado", "Sem Registro", "Exterior"]
display( df.filter( ~df['Estado'].isin(Filtro)) )

# COMMAND ----------

#4° Excluir dados que estados tenham a classificação [Indeterminado, Sem Registro, Exterior]

classificacoes_a_excluir = ["Indeterminado", "Sem Registro", "Exterior"]
df = df.filter(~df['Estado'].isin(classificacoes_a_excluir))
display(df)


# COMMAND ----------

#5°: inserir coluna com nome de atualização para usuario ver quando os dados foram atualizados 
from pyspark.sql.functions import current_timestamp, date_format,from_utc_timestamp
df = df.withColumn("Atualizacao",\
     date_format( from_utc_timestamp(current_timestamp(), "America/Sao_Paulo"), "yyyy-MM-dd HH:mm:ss"))
display(df)

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/Anac/Gold/
# MAGIC

# COMMAND ----------

#6°:Salvar na camada Gold particionada por UF > 'Estado'
df.write\
    .mode("overwrite")\
    .format("parquet")\
    .partitionBy("Estado")\
    .save("dbfs:/mnt/Anac/Gold/anac_gold_particionado")
