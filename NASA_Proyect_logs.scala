// Databricks notebook source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

val NasaLog_CSV = spark.read.text("/FileStore/tables/access_log_Aug95")

// COMMAND ----------

NasaLog_CSV.show()

// COMMAND ----------

val host="""(\S+)"""
val userIdentifier="""\s(-|\S+)"""
val userId="""\s(-|\S+)"""
val date="""\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2})"""
val timezone="""(-\d{4})]"""
val method="""\"(GET|POST|PUT|TRACE|HEAD)?\s(-|\S+)\s(\S+)\""""
val status="""\s(\d{3})\s"""
val size="""(\d+)$"""

// COMMAND ----------

val nasa_df2 = NasaLog_CSV
  .select(regexp_extract($"value", host,1).alias("host"))
nasa_df2.show(false)

// COMMAND ----------

val nasa_df = NasaLog_CSV
  .select(regexp_extract($"value", host,1).alias("host"),
          regexp_extract($"value", userIdentifier, 1).alias("user"),
          regexp_extract($"value", userId, 1).alias("userid"),
          regexp_extract($"value", date, 1).alias("date"),
          regexp_extract($"value", timezone, 1).alias("timeZone"),
          regexp_extract($"value", method, 1).alias("method"),
          regexp_extract($"value", method, 2).alias("resource"),
          regexp_extract($"value", method, 3).alias("protocol"),
          regexp_extract($"value", status, 1).alias("status"),
          regexp_extract($"value", size, 1).alias("size"))

// COMMAND ----------

val dfSave = nasa_df.select("host", "date", "method", "resource", "protocol", "status", "size")
dfSave.show()

// COMMAND ----------

val df_nasa_sinBlancos = dfSave
.withColumn("date", to_timestamp(col("date"), "dd/MMM/yyyy:HH:mm:ss"))
.withColumn("method", when(length(col("method")) === 0,"OTHER").otherwise(col("method")))
.withColumn("size", when(length(col("size")) === 0,"0").otherwise(col("size")))

df_nasa_sinBlancos.select("*").orderBy("size").show()

// COMMAND ----------

df_nasa_sinBlancos.write.format("parquet").mode("overwrite").save("/NasaLogTratado")

// COMMAND ----------

val df_nasa = spark.read.parquet("/NasaLogTratado")
df_nasa.printSchema()

// COMMAND ----------

df_nasa.createOrReplaceTempView("logs")
val df_Cast = spark.sql("SELECT host, date, method, resource, protocol, CAST(status AS INT), CAST(size AS INT) FROM logs")

// COMMAND ----------

df_Cast.select("protocol").distinct.show()

// COMMAND ----------

df_Cast.groupBy("status").agg(count("status").alias("cd_estado_mas_comunes")).sort(desc("cd_estado_mas_comunes")).show()

// COMMAND ----------

df_Cast.groupBy("method").agg(count("method").alias("metodo_mas_usado")).sort(desc("metodo_mas_usado")).show()

// COMMAND ----------

df_Cast.groupBy("resource").agg(max("size").alias("mas_bytes")).sort(desc("mas_bytes")).show()

// COMMAND ----------

df_Cast.groupBy("resource").agg(count("resource").alias("mas_trafico")).sort(desc("mas_trafico")).show()

// COMMAND ----------

df_Cast.withColumn("dia", dayofmonth(col("date"))).groupBy("dia").agg(max("size").alias("Maximo")).sort(desc("Maximo")).show()

// COMMAND ----------

df_Cast.groupBy("host").agg(count("host").alias("host_mas_frecuente")).sort(desc("host_mas_frecuente")).show()

// COMMAND ----------

df_Cast.withColumn("horas", hour(col("date"))).groupBy("horas").agg(count("horas").alias("mas_trafico")).sort(desc("mas_trafico")).show(29)

// COMMAND ----------

df_Cast.withColumn("dias", dayofmonth(col("date")))
       .filter(col("status")==="404")
       .groupBy("dias")
       .agg(count("dias").alias("dias_con_errores_404"))
       .sort(desc("dias_con_errores_404"))
       .show(false)

// COMMAND ----------


