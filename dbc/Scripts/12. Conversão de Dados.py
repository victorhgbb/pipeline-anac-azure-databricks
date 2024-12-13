# Databricks notebook source
# MAGIC %md
# MAGIC ######Renomeando Colunas withColumnRenamed e Alias

# COMMAND ----------

# sintaxe
df = df.withColumnRenamed("coluna_antiga", "coluna_nova")


# COMMAND ----------

df = spark.read.json("dbfs:/FileStore/tables/Anac/V_OCORRENCIA_AMPLA.json")
display(df)

# COMMAND ----------

print(df.columns)

# COMMAND ----------

renomeado = df.withColumnRenamed('Aerodromo_de_Destino', 'Destino') \
    .withColumnRenamed('Aerodromo_de_Origem', 'Origem') \
    .withColumnRenamed('Danos_a_Aeronave', 'Danos')\
    .withColumnRenamed('UF', 'Estado')
    

display(renomeado)


# COMMAND ----------

print(renomeado.columns)

# COMMAND ----------

print(df.columns)

# COMMAND ----------

#selecionando apenas algumas colunas e ja renomeando com .alias

Teste = df.select(df.Aerodromo_de_Destino.alias('Destino'),
                  df.Nome_do_Fabricante,
                  df.Aerodromo_de_Origem.alias('Origem'),
                  df.UF.alias('Estado'),
                  df.Numero_da_Ocorrencia,
                  df.Matricula.alias("Registro")


        )
display(Teste)

# COMMAND ----------

# MAGIC %md
# MAGIC ######Conversão de Dados método cast + withColumn 

# COMMAND ----------

#Resumo
#A função cast no PySpark aceita vários tipos de dados para a conversão
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

#Curiosidades
"""
Qual diferença entre esses que trazem numeros decimais no geral
double:            precisão dupla.
float:             precisão simples.
decimal:           decimal.

----- numeros com longas casas decimais 
float (Precisão Simples 32 bits na memória):
Mínimo: 1.40129846432 x 10^-45
Máximo: 3.40282346639 x 10^38

double (Precisão Dupla 64 bits na memória):
Mínimo: 4.94065645841 x 10^-324
Máximo: 1.79769313486 x 10^308

decimal : pode escolher quantas casas decimais aparece no resultado 
O decimal usa uma representação decimal, o que permite maior precisão e evita muitos dos problemas de arredondamento associados ao float

Decimal ou Float são mais ultilizados

"""

# COMMAND ----------

#Carga Df sem .option("inferSchema", "true") sendo assim virá tudo como string, vamor ver na prática 
orders_df       = spark.read.\
    format("csv")\
    .option("header", "true")\
    .load("/FileStore/tables/Bikes/orders.csv")
display(orders_df)

# COMMAND ----------

orders_df.printSchema()

# COMMAND ----------

#Converter Dados com withColumn
orders_df = orders_df.withColumn('order_id',orders_df.order_id.cast('int') ) 
orders_df = orders_df.withColumn('customer_id',orders_df.customer_id.cast('integer') ) 
orders_df = orders_df.withColumn('order_date',orders_df.order_date.cast('date'))
orders_df = orders_df.withColumn('required_date',orders_df.required_date.cast('date'))
display(orders_df)

# COMMAND ----------

#Converter Dados com .Select  
orders_df2       = spark.read.\
    format("csv")\
    .option("header", "true")\
    .load("/FileStore/tables/Bikes/orders.csv")
display(orders_df2)

# COMMAND ----------

#.Select (Converte e ja seleciona as colunas , pode renomear com Alias tambem)
Conversao = orders_df2.select(
    orders_df2.order_id.cast("integer"),
    orders_df2.order_date.cast("date"),
    orders_df2.store_id.cast("int"),
    orders_df2.staff_id.cast("int").alias("IdLoja"),
    orders_df2.order_status.cast("int").alias("Status_Entrega"),
)
display(Conversao)

# COMMAND ----------

# MAGIC %md
# MAGIC ######Conversões númericas Avançadas

# COMMAND ----------


Items = spark.read.\
    format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("/FileStore/tables/Bikes/order_items.csv")
display(Items)

# COMMAND ----------

print(Items.columns)

# COMMAND ----------

#formato double
fracionado = Items.withColumn('Fracao',Items.list_price/2.03)
display(fracionado)

# COMMAND ----------

#Criando novas colunas ja convertidas 
fracionado2 = fracionado\
    .withColumn('double' ,fracionado.Fracao)\
    .withColumn('Int'    ,fracionado.Fracao.cast('integer') )\
    .withColumn('Float'  ,fracionado.Fracao.cast('float') )\
    .withColumn('Decimal',fracionado.Fracao.cast('decimal(10,2)'))
display(fracionado2)


"""
Obs : 'decimal(10,2)'  
o valor decimal terá um máximo de 10 dígitos no total (incluindo os dígitos à esquerda e à direita do ponto decimal) e 2 dígitos à direita do ponto decimal.
"""

# COMMAND ----------

#Substituindo o duble por decimal no Df
display(fracionado)

# COMMAND ----------


SubDouble =  fracionado.withColumn('Fracao',fracionado.Fracao.cast('decimal(10,2)'))
display(SubDouble)

# COMMAND ----------

# MAGIC %md
# MAGIC ######Arredondamento de Numeros

# COMMAND ----------

display(fracionado2) # Rodar ate gerar o DF fracionado2 

# COMMAND ----------

#Aproveitar  script fracionado2 para Treinar 
"""
floor (arredondamento para baixo)
ceil  (arredondamento para cima)
round (arredondamento Automatico)
"""

from pyspark.sql.functions import floor, ceil , round
Arred = fracionado2\
    .withColumn('ArredAcima', ceil(fracionado2.double))\
    .withColumn('ArredAbaixo',floor(fracionado2.double))\
    .withColumn('ArredAutomatico',round(fracionado2.double))
display(Arred)


# COMMAND ----------

# MAGIC %md
# MAGIC ######Trabalhando com Datas
# MAGIC https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
# MAGIC
# MAGIC https://spark.apache.org/docs/latest/sql-ref-datatypes.html

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

#Extraindo uma fração de uma data em novas Colunas
from pyspark.sql.functions import year, month, dayofmonth

orders_df = orders_df\
                     .withColumn("ano_pedido", year("order_date")) \
                     .withColumn("mes_pedido", month("order_date")) \
                     .withColumn("dia_pedido", dayofmonth("order_date")) 
display(orders_df)

# COMMAND ----------

#usando a classe datetime
from pyspark.sql.functions import expr
from datetime import datetime
orders_df2 = orders_df\
                     .withColumn("ano_pedido", expr("year(order_date)")) \
                     .withColumn("mes_pedido", expr("month(order_date)")) \
                     .withColumn("dia_pedido", expr("day(order_date)"))
display(orders_df2)

# COMMAND ----------

#Simulando mater registro de quando os dados foram carregados (Muito comum no dia a dia para rastreio se os dados foram atualizados ou nao )

from pyspark.sql.functions import current_date,current_timestamp ,expr

orders_df = orders_df \
    .withColumn("data_atual", current_date()) \
    .withColumn("data_hora_atual", current_timestamp()) \
    .withColumn("data_hora_br", expr("from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo')"))

display(orders_df)


# COMMAND ----------

#formato brasil -3h
#   .withColumn("data_hora_br", expr("from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo')"))
from pyspark.sql.functions import current_date,current_timestamp ,expr

orders_df = orders_df \
    .withColumn("data_atual", current_date()) \
    .withColumn("data_hora_atual", current_timestamp()) \
    .withColumn("data_hora_br", expr("from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo')"))

display(orders_df)

"""
referencias 
https://docs.databricks.com/en/sql/language-manual/functions/from_utc_timestamp.html
https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.functions.from_utc_timestamp.html
"""

# COMMAND ----------

#Extraindo horas minutos e Segundos 
from pyspark.sql.functions import year, month, dayofmonth, hour, minute, second
orders_df = orders_df\
                     .withColumn("hora_carga", hour("data_hora_br")) \
                     .withColumn("minuto_carga", minute("data_hora_br")) \
                     .withColumn("segundo_carga", second("data_hora_br"))

display(orders_df)



# COMMAND ----------

display(orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Comandos e Formatos de Datas avançado 
# MAGIC https://docs.databricks.com/en/sql/language-manual/functions/cast.html
# MAGIC
# MAGIC https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html

# COMMAND ----------

orders_df       = spark.read.\
    format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("/FileStore/tables/Bikes/orders.csv")
display(orders_df)

# COMMAND ----------

#Formato PT-Br

from pyspark.sql.functions import  date_format
orders_df = orders_df.withColumn("data_formatada", date_format(orders_df.order_date, "dd/MM/yyyy"))
display(orders_df)



# COMMAND ----------

#Exemplos outros formatos
from pyspark.sql.functions import date_format 

orders_df = orders_df\
        .withColumn("Exemplo1", date_format(orders_df.order_date,"yyyy MM dd"))\
        .withColumn("Exemplo2", date_format(orders_df.order_date,"MM/dd/yyyy HH:mm"))\
        .withColumn("Exemplo3", date_format(orders_df.order_date,"MM-yyyy"))\
        .withColumn("Exemplo4", date_format(orders_df.order_date,"yyyy MMM dd"))\
        .withColumn("Exemplo5", date_format(orders_df.order_date,"yyyy MMMM dd E"))\
        .withColumn("Exemplo6", date_format(orders_df.order_date,"MMM"))\
        .withColumn("Exemplo7", date_format(orders_df.order_date,"MMM-yyyy"))   

display(orders_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ###### Traduçoes para portugues
# MAGIC

# COMMAND ----------

#conversao para portugues método when 
from pyspark.sql.functions import when
orders_df2 = orders_df.withColumn("Exemplo6",
                                    when(orders_df.Exemplo6 =="Jan","Jan")
                                   .when(orders_df.Exemplo6 =="Feb","Fev")
                                   .when(orders_df.Exemplo6 =="Mar","Mar")
                                   .when(orders_df.Exemplo6 =="Apr","Abr")
                                   .when(orders_df.Exemplo6 =="May","Mai")
                                   .when(orders_df.Exemplo6 =="Jun","Jun")
                                   .when(orders_df.Exemplo6 =="Jul","Jul")
                                   .when(orders_df.Exemplo6 =="Aug","Ago")
                                   .when(orders_df.Exemplo6 =="Sep","Set")
                                   .when(orders_df.Exemplo6 =="Oct","Out")
                                   .when(orders_df.Exemplo6 =="Nov","Nov")
                                   .when(orders_df.Exemplo6 =="Dec","Dez")
                                   .otherwise(orders_df.Exemplo6))

display(orders_df2)


# COMMAND ----------

#Colinha marota
"""
MMM >mes abrev 
"Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec",
"Jan Fev Mar Abr Mai Jun Jul Ago Set Out Nov Dez"

MMMM > mes completo
"January February March April May June July August September October November December",
"Janeiro Fevereiro Março Abril Maio Junho Julho Agosto Setembro Outubro Novembro Dezembro"


E > dia da semana 
"Sun Mon Tue Wed Thu Fri Sat", 
"Dom Seg Ter Qua Qui Sex Sáb"

EEEE > dia da semana completo
"Sunday Monday Tuesday Wednesday Thursday Friday Saturday", 
"Domingo Segunda-feira Terça-feira Quarta-feira Quinta-feira Sexta-feira Sábado"


"""

# COMMAND ----------

# MAGIC %md
# MAGIC ######Diferença entre Datas Adição e Subtração

# COMMAND ----------

orders_df       = spark.read.\
    format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("/FileStore/tables/Bikes/orders.csv")
display(orders_df)

# COMMAND ----------

from pyspark.sql.functions import datediff, months_between, year


orders_df = orders_df.withColumn("diferenca_dias", datediff("required_date", "order_date")) \
                     .withColumn("diferenca_meses", months_between("required_date", "order_date")) \
                     .withColumn("diferenca_anos", year("required_date") - year("order_date"))

display(orders_df)


# COMMAND ----------

#Simulando diferença de alguma coluna de data com a data Atual 
from pyspark.sql.functions import datediff, months_between, year ,current_date

orders_df = orders_df.withColumn("diferenca_dias", datediff( current_date(),"required_date",)) \
                     .withColumn("diferenca_meses", months_between(current_date(),"required_date")) \
                     .withColumn("diferenca_anos", year(current_date()) -  year("required_date"))

display(orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Adicionar intervalos em Datas
# MAGIC
# MAGIC https://docs.databricks.com/en/sql/language-manual/functions/cast.html
# MAGIC

# COMMAND ----------

orders_df       = spark.read.\
    format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("/FileStore/tables/Bikes/orders.csv")
display(orders_df)

# COMMAND ----------

from pyspark.sql.functions import expr

# Adicionar coluna 'previsao_entrega' com 5 dias adicionados à 'order_date'
# Adicionar coluna 'pagamento' com 1 mês adicionado à 'order_date'
# Adicionar coluna 'devolucao' com 1 Ano adicionado à 'order_date'

orders_df = orders_df\
                     .withColumn("previsao_entrega", expr("order_date + interval 5 days"))\
                     .withColumn("pagamento", expr("order_date + interval 1 month"))\
                     .withColumn("max_devolucao", expr("order_date + interval 1 year"))
display(orders_df)

# COMMAND ----------

#simulação rápida tirando intervalos
orders_df = orders_df\
                     .withColumn("previsao_entrega", expr("order_date - interval 5 days"))\
                     .withColumn("pagamento", expr("order_date - interval 1 month"))\
                     .withColumn("max_devolucao", expr("order_date - interval 1 year"))
display(orders_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ###### Datas em SQL
# MAGIC

# COMMAND ----------

orders_df       = spark.read.\
    format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("/FileStore/tables/Bikes/orders.csv")
display(orders_df)

# COMMAND ----------

orders_df.createOrReplaceTempView("tabela_ordens")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from tabela_ordens

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- extraindo partes de uma data 
# MAGIC SELECT *,
# MAGIC        YEAR(order_date) AS Ano,
# MAGIC        MONTH(order_date) AS Mes,
# MAGIC        DAY(order_date) AS Dia
# MAGIC       FROM tabela_ordens
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- para treinar   
# MAGIC -- traduções: now ou  current_timestamp  = carimbo de data/hora atual
# MAGIC -- muito usado para ter coluna ou colunas de quando os daos foram carregados para base, arquivos banco de dados etc
# MAGIC --  converter fuso horário from_utc_timestamp
# MAGIC
# MAGIC select * ,
# MAGIC now() as agora,
# MAGIC current_timestamp() AS agora2,
# MAGIC from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') AS agora_PTBR,
# MAGIC date_format(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo'), 'yyyy-MM-dd HH:mm:ss') AS agora_formatado,
# MAGIC date_format(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo'), 'dd/MM/yyyy') AS agora_formatado_brasil,
# MAGIC date_format(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo'), 'dd/MM/yyyy HH:mm:ss') AS agora_formatado_brasil_Completo,
# MAGIC year(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo')) as ano,
# MAGIC month(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo')) as mes,
# MAGIC day(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo')) as dia,
# MAGIC hour(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo')) as hora,
# MAGIC minute(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo')) as minuto,
# MAGIC second(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo')) as segundo
# MAGIC from tabela_ordens

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- dados maior que certa data 
# MAGIC  SELECT * FROM tabela_ordens WHERE order_date >= '2016-01-01' -- Obs o padrao é ano-mes-dia
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- quantos anos tenho de registro na base?
# MAGIC  SELECT distinct 
# MAGIC  year(order_date) 
# MAGIC  FROM tabela_ordens 

# COMMAND ----------

# MAGIC %sql 
# MAGIC --Dados entre 2017  e 2018
# MAGIC SELECT * FROM tabela_ordens
# MAGIC  WHERE year(order_date) BETWEEN 2017 and 2018

# COMMAND ----------

# MAGIC %sql 
# MAGIC --Dados entre datas espesificas 
# MAGIC SELECT * FROM tabela_ordens
# MAGIC WHERE order_date BETWEEN '2017-01-04' and '2018-01-01' 
# MAGIC  -- Obs o padrao é ano-mes-dia 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * ,
# MAGIC date_format(order_date, 'yyyy-MM-dd HH:mm:ss') AS ptbr,
# MAGIC date_format(order_date, 'dd/MM/yyyy') AS dataformatada,
# MAGIC date_format(order_date, 'dd/MM/yyyy HH:mm:ss') AS datahora
# MAGIC
# MAGIC from tabela_ordens

# COMMAND ----------

#Salvar consulta em um Df para trabalhar com Python ou salvar como qualqer tipo de arquivo 

resultado = spark.sql(
    
"""
select * ,
date_format(order_date, 'yyyy-MM-dd HH:mm:ss') AS ptbr,
date_format(order_date, 'dd/MM/yyyy') AS dataformatada,
date_format(order_date, 'dd/MM/yyyy HH:mm:ss') AS datahora

from tabela_ordens                



""")


display(resultado)

