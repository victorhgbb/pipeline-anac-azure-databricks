# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/Anac/Bronze

# COMMAND ----------

df = spark.read.json("dbfs:/mnt/Anac/Bronze/V_OCORRENCIA_AMPLA.json")
display(df)

# COMMAND ----------

print(df.columns)

# COMMAND ----------

# substituindo colunas de texto null para 'sem Registro'


# COMMAND ----------

colunas = ['Aerodromo_de_Destino', 'Aerodromo_de_Origem', 'CLS', 'Categoria_da_Aeronave', 'Classificacao_da_Ocorrência', 'Danos_a_Aeronave', 'Data_da_Ocorrencia', 'Descricao_do_Tipo', 'Fase_da_Operacao', 'Historico', 'Hora_da_Ocorrência', 'ICAO', 'Ilesos_Tripulantes', 'Ilesos_Passageiros', 'Latitude', 'Matricula', 'Modelo', 'Municipio', 'Nome_do_Fabricante', 'Numero_da_Ficha', 'Numero_da_Ocorrencia', 'Numero_de_Assentos', 'Operacao', 'Operador', 'Operador_Padronizado', 'PMD', 'PSSO', 'Regiao', 'Tipo_ICAO', 'Tipo_de_Aerodromo', 'Tipo_de_Ocorrencia', 'UF']

# percorrer todas as colunas e fazer a mesma coisa pra todas selecionadas na variavel 
for ajuste in colunas:
    df = df.fillna('Sem Registro', subset=[ajuste])

display(df)

# COMMAND ----------

# colunas de origem estão com dados em str , converter para 'int' e já trocar null por 0 como aprendemos em aulas anteriores 
#Fixando função Loop

coluna_converter = 'Lesoes_Desconhecidas_Passageiros'
df = df\
    .withColumn(coluna_converter, df[coluna_converter].cast("int"))\
    .fillna(0, subset=[coluna_converter])
display(df)

#  Como fazer para passar em todas as colunas ?
# ajuste_int = ['Lesoes_Desconhecidas_Passageiros', 'Lesoes_Desconhecidas_Terceiros', 'Lesoes_Desconhecidas_Tripulantes', 'Lesoes_Fatais_Passageiros', 'Lesoes_Fatais_Terceiros', 'Lesoes_Fatais_Tripulantes', 'Lesoes_Graves_Passageiros', 'Lesoes_Graves_Terceiros', 'Lesoes_Graves_Tripulantes', 'Lesoes_Leves_Passageiros', 'Lesoes_Leves_Terceiros', 'Lesoes_Leves_Tripulantes']


# COMMAND ----------

ajuste_int = ['Lesoes_Desconhecidas_Passageiros', 'Lesoes_Desconhecidas_Terceiros', 'Lesoes_Desconhecidas_Tripulantes', 'Lesoes_Fatais_Passageiros', 'Lesoes_Fatais_Terceiros', 'Lesoes_Fatais_Tripulantes', 'Lesoes_Graves_Passageiros', 'Lesoes_Graves_Terceiros', 'Lesoes_Graves_Tripulantes', 'Lesoes_Leves_Passageiros', 'Lesoes_Leves_Terceiros', 'Lesoes_Leves_Tripulantes']


for Loop in ajuste_int :
    df = df\
        .withColumn(Loop, df[Loop].cast("int"))\
        .fillna(0, subset=[Loop])
display(df)

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/Anac

# COMMAND ----------

# Salvar na pasta Silver em formato parquet (origem era Json mas parquet é melhor , tambem ja vimos em aulas anteriores)

df.write.mode("overwrite").parquet("dbfs:/mnt/Anac/Silver/anac_silver.parquet")

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/Anac/Silver/

# COMMAND ----------

# Ler esse novo arquivo parket 

display(spark.read.parquet("dbfs:/mnt/Anac/Silver/anac_silver.parquet/"))
