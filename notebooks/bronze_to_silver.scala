// Databricks notebook source
val path = "dbfs:/mnt/dados/bronze/dataset/"
val df = spark.read.format("delta").load(path)

// COMMAND ----------

val dados_detalhados = df.select("anuncio.*", "anuncio.endereco.*")

// COMMAND ----------

val df_silver = dados_detalhados.drop("caracteristicas", "endereco")

// COMMAND ----------

val path = "dbfs:/mnt/dados/silver/dataset"
df_silver.write.format("delta").mode("overwrite").save(path)
