# Databricks notebook source
# MAGIC %md
# MAGIC ######Documentação
# MAGIC https://docs.databricks.com/pt/partners/bi/power-bi.html

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Localizar Conexão

# COMMAND ----------

# Cluster > JDBC/ODBC 
# usar Server Hostname e HTTP Path para conexão 

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Criando banco de Dados
# MAGIC

# COMMAND ----------

# script da Aula (2. Criando Tabelas Via Comando SQL)
# Rodar para criar Banco de dados a partir de arquivos SCV'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS BikesCSV;
# MAGIC use BikesCSV;
# MAGIC CREATE TABLE IF NOT EXISTS Produtos    USING csv OPTIONS ('path' '/FileStore/tables/Bikes/products.csv'    ,'header' 'true','inferSchema' 'true');
# MAGIC CREATE TABLE IF NOT EXISTS Clientes    USING csv OPTIONS ('path' '/FileStore/tables/Bikes/customers.csv'   ,'header' 'true','inferSchema' 'true');
# MAGIC CREATE TABLE IF NOT EXISTS brands      USING csv OPTIONS ('path' '/FileStore/tables/Bikes/brands.csv'      ,'header' 'true','inferSchema' 'true');
# MAGIC CREATE TABLE IF NOT EXISTS categories  USING csv OPTIONS ('path' '/FileStore/tables/Bikes/categories.csv'  ,'header' 'true','inferSchema' 'true');
# MAGIC CREATE TABLE IF NOT EXISTS order_items USING csv OPTIONS ('path' '/FileStore/tables/Bikes/order_items.csv' ,'header' 'true','inferSchema' 'true');
# MAGIC CREATE TABLE IF NOT EXISTS orders      USING csv OPTIONS ('path' '/FileStore/tables/Bikes/orders.csv'      ,'header' 'true','inferSchema' 'true');
# MAGIC CREATE TABLE IF NOT EXISTS staffs      USING csv OPTIONS ('path' '/FileStore/tables/Bikes/staffs.csv'      ,'header' 'true','inferSchema' 'true');
# MAGIC CREATE TABLE IF NOT EXISTS stocks      USING csv OPTIONS ('path' '/FileStore/tables/Bikes/stocks.csv'      ,'header' 'true','inferSchema' 'true');
# MAGIC CREATE TABLE IF NOT EXISTS stores      USING csv OPTIONS ('path' '/FileStore/tables/Bikes/stores.csv'      ,'header' 'true','inferSchema' 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select 
# MAGIC o.order_id
# MAGIC ,i.quantity              as Quantidade
# MAGIC ,p.product_name          as Produto
# MAGIC ,b.brand_name            as Marca
# MAGIC ,p.model_year            as Modelo
# MAGIC ,ct.category_name        as Categoria
# MAGIC ,i.list_price            as Valor
# MAGIC ,o.order_date            as DtCompra
# MAGIC ,c.first_name            as Cliente
# MAGIC ,c.email                 as EmailCliente
# MAGIC ,c.city                  as Cidade
# MAGIC ,c.state                 as Estado
# MAGIC ,s.store_name            as LojaCompra
# MAGIC ,s.email                 as EmailLoja
# MAGIC from      order_items I
# MAGIC left join orders      O  on i.order_id = o.order_id
# MAGIC left join clientes    C  on o.customer_id = c.customer_id
# MAGIC left join produtos    P  on i.product_id = p.product_id
# MAGIC left join stores      S  on o.store_id = s.store_id
# MAGIC left join brands      B  on p.brand_id = b.brand_id
# MAGIC left join categories  CT on p.category_id = ct.category_id

# COMMAND ----------

# MAGIC %md
# MAGIC ######Transformando resultado da consulta em outra tabela no Banco de Dados

# COMMAND ----------

# inserir consulta em uma variavel fica mais facil
query = """

select 
o.order_id
,i.quantity              as Quantidade
,p.product_name          as Produto
,b.brand_name            as Marca
,p.model_year            as Modelo
,ct.category_name        as Categoria
,i.list_price            as Valor
,o.order_date            as DtCompra
,c.first_name            as Cliente
,c.email                 as EmailCliente
,c.city                  as Cidade
,c.state                 as Estado
,s.store_name            as LojaCompra
,s.email                 as EmailLoja
from      order_items I
left join orders      O  on i.order_id = o.order_id
left join clientes    C  on o.customer_id = c.customer_id
left join produtos    P  on i.product_id = p.product_id
left join stores      S  on o.store_id = s.store_id
left join brands      B  on p.brand_id = b.brand_id
left join categories  CT on p.category_id = ct.category_id


"""
resultado = spark.sql(query)

# Salvando em formato delta em banco de dados ("nao sabe o que é Delta? nos proximos módulos vou dar exemplos e vamos aprender na prática , não se preocupe")
resultado.write.\
    format("delta")\
    .mode("overwrite")\
    .saveAsTable("bikescsv.ConsultaCompleta")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from consultacompleta
# MAGIC -- ver no power bi em forma de tabela

# COMMAND ----------

# MAGIC %md
# MAGIC Curiosidades para ir pesquisando 

# COMMAND ----------

#usar catalogo hive_metastore

# COMMAND ----------

"""
Vamos ver tudo isso em aulas posteriores!!!

Delta é projetado para manter e gerenciar dados de maneira transacional e eficiente. Essa pasta no DBFS contém os arquivos e metadados necessários para implementar as características do Delta, como transações ACID, controle de versão e otimizações de consulta.

A pasta Hive  é uma parte do sistema de gerenciamento de metadados do Databricks, e é usada para manter o registro das tabelas e metadados relacionados. Quando você cria uma tabela no Databricks usando o formato Delta, um registro da tabela é armazenado no Hive Metastore, que é uma parte integrante do ambiente Databricks.





"""

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Salvando todos Arquivos no formato Delta em Banco de Dados

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Criando novo banco de Dados
# MAGIC CREATE DATABASE IF NOT EXISTS teste;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --Deletar  tabelas obs(passar banco de dados primeiro.depois nome da tabela)
# MAGIC DROP table IF EXISTS bikescsv.brands ;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Deletar todos bancos de dados
# MAGIC DROP DATABASE IF EXISTS teste CASCADE;
# MAGIC

# COMMAND ----------

"""
Importanto todos os dados no formato Delta para 

Fluxo vai ser 

1° Criar novo banco de Dados
2° Transformar todos os dados de origem em um DF
3° Salvar os DataFrames no formato Delta para o banco de Dados 
Vamos Fazer na prática?

"""

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1° Criar novo banco de Dados  para receber as tabelas Delta
# MAGIC CREATE DATABASE IF NOT EXISTS BikesDelta;

# COMMAND ----------


#2° Transformar todos os dados de origem em um DF
produtos_df     = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/FileStore/tables/Bikes/products.csv")
clientes_df     = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/FileStore/tables/Bikes/customers.csv")
brands_df       = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/FileStore/tables/Bikes/brands.csv")
categories_df   = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/FileStore/tables/Bikes/categories.csv")
order_items_df  = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/FileStore/tables/Bikes/order_items.csv")
orders_df       = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/FileStore/tables/Bikes/orders.csv")
staffs_df       = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/FileStore/tables/Bikes/staffs.csv")
stocks_df       = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/FileStore/tables/Bikes/stocks.csv")
stores_df       = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/FileStore/tables/Bikes/stores.csv")

# COMMAND ----------

stocks_df.show(5)

# COMMAND ----------

# 3° Salvar os DataFrames no formato Delta para o banco de Dados
produtos_df.write.format("delta").mode("overwrite").saveAsTable("BikesDelta.Produtos")
clientes_df.write.format("delta").mode("overwrite").saveAsTable("BikesDelta.Clientes")
brands_df.write.format("delta").mode("overwrite").saveAsTable("BikesDelta.Brands")
categories_df.write.format("delta").mode("overwrite").saveAsTable("BikesDelta.Categories")
order_items_df.write.format("delta").mode("overwrite").saveAsTable("BikesDelta.order_items")
orders_df.write.format("delta").mode("overwrite").saveAsTable("BikesDelta.Orders")
staffs_df.write.format("delta").mode("overwrite").saveAsTable("BikesDelta.Staffs")
stocks_df.write.format("delta").mode("overwrite").saveAsTable("BikesDelta.Stocks")
stores_df.write.format("delta").mode("overwrite").saveAsTable("BikesDelta.Stores")

# COMMAND ----------

# MAGIC %sql
# MAGIC use BikesDelta;
# MAGIC select * from Produtos

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC o.order_id
# MAGIC ,i.quantity              as Quantidade
# MAGIC ,p.product_name          as Produto
# MAGIC ,b.brand_name            as Marca
# MAGIC ,p.model_year            as Modelo
# MAGIC ,ct.category_name        as Categoria
# MAGIC ,i.list_price            as Valor
# MAGIC ,o.order_date            as DtCompra
# MAGIC ,c.first_name            as Cliente
# MAGIC ,c.email                 as EmailCliente
# MAGIC ,c.city                  as Cidade
# MAGIC ,c.state                 as Estado
# MAGIC ,s.store_name            as LojaCompra
# MAGIC ,s.email                 as EmailLoja
# MAGIC from      BikesDelta.order_items I
# MAGIC left join BikesDelta.orders      O  on i.order_id = o.order_id
# MAGIC left join BikesDelta.clientes    C  on o.customer_id = c.customer_id
# MAGIC left join BikesDelta.produtos    P  on i.product_id = p.product_id
# MAGIC left join BikesDelta.stores      S  on o.store_id = s.store_id
# MAGIC left join BikesDelta.brands      B  on p.brand_id = b.brand_id
# MAGIC left join BikesDelta.categories  CT on p.category_id = ct.category_id
# MAGIC

# COMMAND ----------

#usar catalogo hive_metastore
