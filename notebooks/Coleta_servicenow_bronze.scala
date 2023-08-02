// Databricks notebook source
// MAGIC %md
// MAGIC <b>Importando bibliotecas</b>

// COMMAND ----------

// MAGIC %python
// MAGIC from datetime import date, timedelta
// MAGIC from pyspark.sql import SparkSession
// MAGIC from pyspark.sql.functions import col
// MAGIC
// MAGIC data_df_inicio = date.today() - timedelta(days=10)
// MAGIC data_df_fim = date.today() - timedelta(days=1)
// MAGIC
// MAGIC # Inicialize a sessão do Spark
// MAGIC spark = SparkSession.builder.appName("Exemplo_PySpark").getOrCreate()

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Definindo variaveis</b>

// COMMAND ----------

// MAGIC %python
// MAGIC # Configurações da API
// MAGIC sysparm_table = 'sn_customerservice_case_dga'
// MAGIC sysparm_fields = 'number, u_krotonpad_treeview'
// MAGIC sysparm_limit = 10000
// MAGIC sysparm_query = f"""sys_created_byLIKE-ate^sys_created_onBETWEENjavascript:gs.dateGenerate('{data_df_inicio}','00:00:00')@javascript:gs.dateGenerate('{data_df_fim}','00:59:59')^u_canal_entradaLIKETelefone"""
// MAGIC
// MAGIC username = 'lucas.mg'
// MAGIC password = '123'
// MAGIC url = f"https://kroton.service-now.com/api/now/table/{sysparm_table}"
// MAGIC headers = {"Content-Type": "application/json", "Accept": "application/json"}
// MAGIC query_params = {
// MAGIC     "sysparm_fields": sysparm_fields,
// MAGIC     "sysparm_query": sysparm_query,
// MAGIC     "sysparm_display_value": "true",
// MAGIC     "sysparm_limit": sysparm_limit,
// MAGIC     "sysparm_exclude_reference_link": "true",
// MAGIC     "sysparm_suppress_pagination_header": "true"
// MAGIC }
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Chamando a API</b>

// COMMAND ----------

// MAGIC %python
// MAGIC # Chame a API usando o módulo requests para obter os dados
// MAGIC import requests
// MAGIC response = requests.get(url, auth=(username, password), headers=headers, params=query_params)
// MAGIC response.raise_for_status()
// MAGIC data = response.json()
// MAGIC
// MAGIC # Converta os dados para um DataFrame do PySpark
// MAGIC from pyspark.sql import Row
// MAGIC data_rows = [Row(**item) for item in data["result"]]
// MAGIC df = spark.createDataFrame(data_rows)

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Salvando dados no DataLake</b>

// COMMAND ----------

val path = "dbfs:/mnt/dados/bronze/service_now"
df.write.format("delta").mode(SaveMode.Overwrite).save(path)
