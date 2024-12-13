# Databricks notebook source
#Obs quando seu Cluster Desligar vai perder os dados do Banco de dados e tabelas criadas (Database Tables)
# Dados do DBFS não sao perdidos

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Criando Banco de dados 

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/tables/Bikes/'))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS Teste;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Criando tabela de Produtos

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use teste;
# MAGIC CREATE TABLE IF NOT EXISTS Produtos
# MAGIC USING csv
# MAGIC OPTIONS (
# MAGIC   'path' '/FileStore/tables/Bikes/products.csv',  -- Caminho para o arquivo no DBFS
# MAGIC   'header' 'true',                               -- Se a primeira linha do arquivo contém cabeçalho
# MAGIC   'inferSchema' 'true'                           -- Inferir automaticamente os tipos de dados das colunas
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ver todos os dados 
# MAGIC use Teste;
# MAGIC SELECT * FROM Produtos
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC use Teste;
# MAGIC
# MAGIC SELECT count(*) FROM Produtos

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quantidade de Bikes por ano do modelo produzido 
# MAGIC select 
# MAGIC model_year as Ano
# MAGIC ,count(product_id) QNT
# MAGIC from produtos
# MAGIC group by model_year
# MAGIC order by  QNT desc
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Criando tabela de Clientes

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use Teste;
# MAGIC CREATE TABLE IF NOT EXISTS Clientes
# MAGIC USING csv
# MAGIC OPTIONS (
# MAGIC   'path' '/FileStore/tables/Bikes/customers.csv',  -- Caminho para o arquivo no DBFS
# MAGIC   'header' 'true',                               -- Se a primeira linha do arquivo contém cabeçalho
# MAGIC   'inferSchema' 'true'                           -- Inferir automaticamente os tipos de dados das colunas
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC use Teste;
# MAGIC select * from Clientes

# COMMAND ----------

# MAGIC %sql
# MAGIC use Teste;
# MAGIC --  quantos Clientes Por Estado
# MAGIC select count(*) from clientes
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC use Teste;
# MAGIC --  quantos Clientes Por Estado
# MAGIC select  
# MAGIC count(customer_id) Quantidade
# MAGIC ,city as Cidade
# MAGIC from clientes
# MAGIC group by city
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC use Teste;
# MAGIC --  quantos Clientes Por Estado
# MAGIC select  
# MAGIC state
# MAGIC ,count(customer_id) Quantidade
# MAGIC from clientes
# MAGIC group by state
# MAGIC order by 2 desc

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Criando outras tabelas

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/tables/Bikes/'))

# COMMAND ----------

# MAGIC %sql
# MAGIC use Teste;
# MAGIC CREATE TABLE IF NOT EXISTS brands
# MAGIC USING csv OPTIONS ('path' '/FileStore/tables/Bikes/brands.csv','header' 'true','inferSchema' 'true');
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS categories
# MAGIC USING csv OPTIONS ('path' '/FileStore/tables/Bikes/categories.csv','header' 'true','inferSchema' 'true');
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS order_items
# MAGIC USING csv OPTIONS ('path' '/FileStore/tables/Bikes/order_items.csv','header' 'true','inferSchema' 'true');
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS orders
# MAGIC USING csv OPTIONS ('path' '/FileStore/tables/Bikes/orders.csv','header' 'true','inferSchema' 'true');
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS staffs
# MAGIC USING csv OPTIONS ('path' '/FileStore/tables/Bikes/staffs.csv','header' 'true','inferSchema' 'true');
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS stocks
# MAGIC USING csv OPTIONS ('path' '/FileStore/tables/Bikes/stocks.csv','header' 'true','inferSchema' 'true');
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS stores
# MAGIC USING csv OPTIONS ('path' '/FileStore/tables/Bikes/stores.csv','header' 'true','inferSchema' 'true');
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC use teste;
# MAGIC select * from stores
