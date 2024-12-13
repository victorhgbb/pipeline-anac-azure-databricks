# Databricks notebook source
# MAGIC %md
# MAGIC ######O que é Delta Lake?
# MAGIC
# MAGIC https://learn.microsoft.com/pt-br/azure/databricks/delta/
# MAGIC
# MAGIC Referencia ACID
# MAGIC
# MAGIC https://learn.microsoft.com/pt-br/azure/databricks/lakehouse/acid 
# MAGIC
# MAGIC Referencia SQL
# MAGIC
# MAGIC https://learn.microsoft.com/pt-br/azure/databricks/sql/language-manual/
# MAGIC
# MAGIC https://learn.microsoft.com/pt-br/azure/databricks/delta/tutorial
# MAGIC
# MAGIC Referencia Dataflame
# MAGIC
# MAGIC https://docs.delta.io/latest/api/python/index.html
# MAGIC
# MAGIC Lakehouse
# MAGIC
# MAGIC https://www.databricks.com/br/blog/2020/01/30/what-is-a-data-lakehouse.html
# MAGIC

# COMMAND ----------

# Resumo de benefícios do Delta Lake 
"""
Delta Lake : é a camada de armazenamento otimizada que fornece a base para armazenar dados e tabelas na Plataforma Databricks Lakehouse

Lakehouse  : é o ambiente onde fica armazenado arquivos\tabelas  no formato Delta

Diferença básica de Delta lake e D'ata lake :  o que acontece quando apaga dados dos 2 ambientes ? voce sabe ?

Transações ACID:significa atomicidade, consistência, isolamento e durabilidade.
Atomicidade: significa que todas as transações têm êxito ou falham completamente.
Consistência: estão relacionadas a como um determinado estado dos dados é observado por operações simultâneas.
Isolamento: se refere a como as operações simultâneas podem entrar em conflito entre si.
Durabilidade: significa que as alterações confirmadas são permanentes.


Benefícios:
Armazenamento otimizado: O Delta Lake é o software de código aberto que estende arquivos de dados Parquet com log e extramamente eficiente e compactado

Segurança e controle do fluxo de dados e reculperação se nescessário 

Versionamento dos dados: 
Cada gravação em uma tabela Delta cria uma versão da tabela. Você pode usar o log de transações para examinar modificações em sua tabela e consultar versões anteriores Você pode recuperar informações de operações, usuário, carimbo de data/hora e assim por diante relativas a cada gravação em uma tabela do Delta por meio do comando history. As operações são retornadas em ordem cronológica inversa.
A retenção do histórico de tabelas é determinada pela configuração da tabela delta.logRetentionDuration, que é de 30 dias por padrão.

(Fique Tranquilo(a) vamos fazer tudo isso na prática e tenho certeza que vai ficar fácil pra voce entender)

Exemplo de data lake
https://www.databricks.com/br/glossary/data-lakehouse



"""

# COMMAND ----------

#Salvando em tabela Delta simples para verificarmos os logs de transações 
spark.read.json("dbfs:/FileStore/tables/Anac/V_OCORRENCIA_AMPLA.json") \
    .write.format("delta")\
    .mode("overwrite")\
    .saveAsTable("anac")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from anac
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC delete from anac
# MAGIC where Aerodromo_de_Origem is null

# COMMAND ----------

# MAGIC %sql 
# MAGIC delete from anac
# MAGIC where UF = "MG"

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- testando os delets
# MAGIC select * from anac
# MAGIC where UF = "SP"
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ######Versionamento, verificando e recuperando dados 
# MAGIC
# MAGIC Obs : A retenção do histórico de tabelas é determinada pela configuração da tabela delta.logRetentionDuration, que é de 30 dias por padrão
# MAGIC https://learn.microsoft.com/pt-br/azure/databricks/delta/history

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ver logs transacionais
# MAGIC DESCRIBE HISTORY  anac 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ver logs transacionais  pelo caminho hive
# MAGIC DESCRIBE HISTORY  '/user/hive/warehouse/anac'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ver detalhes da estrutura Delta
# MAGIC DESCRIBE DETAIL  '/user/hive/warehouse/anac'

# COMMAND ----------

# MAGIC %sql 
# MAGIC --Vendo versao de log na prática 
# MAGIC select * from anac VERSION AS OF 1
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --pode passar tambem o caminho Hive o que preferir  obs from dever ser delta.`Caminhohive`
# MAGIC select * from delta.`/user/hive/warehouse/anac`  VERSION AS OF 1

# COMMAND ----------

# MAGIC %sql
# MAGIC --- restaurando uma versao (fazer consulta antes de restaurar para ver funcionando na prática )
# MAGIC select * from anac where UF = "MG"
# MAGIC --RESTORE TABLE anac TO VERSION AS OF 1
# MAGIC  

# COMMAND ----------

#Salvando em modo particionado 
#Obs: particionar o que realmente for nescessárrio para otimizar a consulta (lembra das aulas de particionamento)
spark.read.json("dbfs:/FileStore/tables/Anac/V_OCORRENCIA_AMPLA.json") \
    .write.format("delta")\
    .partitionBy("UF") \
    .mode("overwrite")\
    .saveAsTable("anac_particionado")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from anac_particionado

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from anac_particionado
# MAGIC where UF = "MG"

# COMMAND ----------

# MAGIC %sql 
# MAGIC delete from  anac_particionado
# MAGIC where Aerodromo_de_Destino = 'SBMK' 

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe history anac_particionado

# COMMAND ----------

# MAGIC %md
# MAGIC ######Otimizando Consulta 
# MAGIC https://learn.microsoft.com/pt-br/azure/databricks/delta/tutorial
# MAGIC
# MAGIC https://learn.microsoft.com/pt-br/azure/databricks/delta/optimize
# MAGIC
# MAGIC https://learn.microsoft.com/pt-br/azure/databricks/delta/vacuum

# COMMAND ----------

spark.read.json("dbfs:/FileStore/tables/Anac/V_OCORRENCIA_AMPLA.json") \
    .write.mode("overwrite")\
    .saveAsTable("anac_normal")

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC replace(Aerodromo_de_Origem,"Sem Origem") as Origem,
# MAGIC Fase_da_Operacao,
# MAGIC Danos_a_Aeronave,
# MAGIC Municipio,
# MAGIC UF as Estado,
# MAGIC Operacao,
# MAGIC Regiao,
# MAGIC count(Matricula)  as quantidade
# MAGIC  from anac_normal
# MAGIC  group by 1,2,3,4,5,6,7
# MAGIC  having Origem is not null
# MAGIC  and Danos_a_Aeronave = 'Nenhum'
# MAGIC
# MAGIC  order by 1,2

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Formato Otimizado

# COMMAND ----------

#salvando no formato delta 
spark.read.json("dbfs:/FileStore/tables/Anac/V_OCORRENCIA_AMPLA.json") \
    .write.format("delta") \
    .partitionBy("Danos_a_Aeronave") \
    .mode("overwrite") \
    .saveAsTable("anac_delta_novo")


# COMMAND ----------

"""
OPTIMIZE nometbela  
ZORDER BY (nomecolunaordernar) obs : cria tipo um indice  nas colunas   pode ser mais de 1 (tudo tem que testar)
"""'

# COMMAND ----------

"""

se der erro (IllegalArgumentException: Danos_a_Aeronave is a partition column. Z-Ordering can only be performed on data columns) quer dizer que a otimização Z-Ordering que você está tentando aplicar não será eficaz porque as estatísticas não estão sendo coletadas para as colunas especificadas ou então esta tentando fazer pela partição (no nosso exemplo foi criado a partição Danos_a_Aeronave nao posso fazer ZORDER BY por ela  )
%sql set spark.databricks.delta.optimize.zorder.checkStatsCollection.enabled = False;

"""

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC replace(Aerodromo_de_Origem,"Sem Origem") as Origem,
# MAGIC Fase_da_Operacao,
# MAGIC Danos_a_Aeronave,
# MAGIC Municipio,
# MAGIC UF as Estado,
# MAGIC Operacao,
# MAGIC Regiao,
# MAGIC count(Matricula)  as quantidade
# MAGIC  from anac_delta_novo
# MAGIC  group by 1,2,3,4,5,6,7
# MAGIC  having Origem is not null
# MAGIC  and Danos_a_Aeronave = 'Nenhum'
# MAGIC
# MAGIC  order by 1,2
