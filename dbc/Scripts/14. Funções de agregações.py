# Databricks notebook source

Items = spark.read.\
    format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("/FileStore/tables/Bikes/order_items.csv")
display(Items)

# COMMAND ----------

display(Items.groupBy("list_price").count())


# COMMAND ----------


#Soma de vendas por id de produtos
display(Items.groupBy("product_id").sum("list_price"))


# COMMAND ----------

#Salvando resultado em um df 
resultado = Items.groupBy("product_id").sum("list_price")
display(resultado)

# COMMAND ----------

#Média de Vendas por produto
display(Items.groupBy("product_id").avg("list_price"))

# COMMAND ----------

#Valores mínimos e máximos
display(Items.groupBy("product_id").min("list_price"))
display(Items.groupBy("product_id").max("list_price"))

# COMMAND ----------

#mostra detalhes estatísticos de  todo df 
display(Items.summary())

"""
count: Número de registros não nulos.
mean (média): Valor médio da coluna.
stddev (desvio padrão): Desvio padrão da coluna.
min (mínimo): Valor mínimo na coluna.
25%: Primeiro quartil (25% dos dados estão abaixo desse valor).
50% (mediana): Valor que separa a metade inferior e superior dos dados.
75%: Terceiro quartil (75% dos dados estão abaixo desse valor).
max (máximo): Valor máximo na coluna.

"""

# COMMAND ----------

# MAGIC %md
# MAGIC ######Agregações em SQL

# COMMAND ----------


Items = spark.read.\
    format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("/FileStore/tables/Bikes/order_items.csv")
Items.createOrReplaceTempView("Items")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from Items

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- agregações 
# MAGIC select 
# MAGIC min(list_price)   as minimo,
# MAGIC max(list_price)   as maximo,
# MAGIC avg(list_price)   as media,
# MAGIC sum(list_price)   as soma ,
# MAGIC count(list_price) as contagem
# MAGIC  from Items

# COMMAND ----------

# MAGIC %sql 
# MAGIC --- e se eu quisese fazer todas agregações por alguma coluna vamos fazer por product_id?
# MAGIC
# MAGIC select 
# MAGIC product_id,
# MAGIC sum(list_price) as TotalVendas,
# MAGIC avg(list_price) as MediaVendas,
# MAGIC count(list_price) as Qnt
# MAGIC from Items
# MAGIC group by product_id order by TotalVendas desc
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC --- avalizando por outra coluna item_id por exemplo 
# MAGIC select 
# MAGIC item_id,
# MAGIC sum(list_price) as TotalVendas,
# MAGIC avg(list_price) as MediaVendas,
# MAGIC count(list_price) as Qnt
# MAGIC from Items
# MAGIC group by item_id order by TotalVendas desc
# MAGIC

# COMMAND ----------

#como salvar resultado em um Df ?

resultado = spark.sql (
"""
select 
item_id,
sum(list_price) as TotalVendas,
avg(list_price) as MediaVendas,
count(list_price) as Qnt
from Items
group by item_id order by TotalVendas desc

"""
)

display(resultado)

# COMMAND ----------

#Vamos treinar mais com datas?
ordens = spark.read.\
    format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("/FileStore/tables/Bikes/orders.csv")
ordens.createOrReplaceTempView("ordens")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from ordens

# COMMAND ----------

# MAGIC %sql
# MAGIC -- quantas vendas por ano  e depois vamos fazer por ano e mes 
# MAGIC select 
# MAGIC year(order_date) as Ano,
# MAGIC count(order_id) as qnt
# MAGIC from ordens
# MAGIC group by Ano order by Ano
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC year(order_date) as Ano,
# MAGIC month(order_date) as mes,
# MAGIC count(order_id) as qnt
# MAGIC from ordens
# MAGIC group by Ano,mes  order by Ano,mes
