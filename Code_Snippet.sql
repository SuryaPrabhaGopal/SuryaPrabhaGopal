-- Databricks notebook source
-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col
-- MAGIC from pyspark.sql.types import StructType, StructField, IntegerType, DateType, DecimalType
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC rates_schema = StructType(fields=[StructField("Date", StringType(), False),
-- MAGIC                                   StructField("Australiandollar", StringType(), True),
-- MAGIC                                   StructField("NewZealanddollar", StringType(), True)
-- MAGIC                                   ])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set(
-- MAGIC     "fs.azure.account.key.suryastoragedb.dfs.core.windows.net",
-- MAGIC     "AccessKey"
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("abfs://raw@suryastoragedb.dfs.core.windows.net")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("abfs://raw@suryastoragedb.dfs.core.windows.net"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read \
-- MAGIC .json("abfss://raw@suryastoragedb.dfs.core.windows.net/Test.json")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC rates_df = spark.read.option("header", True)\
-- MAGIC     .json("abfss://raw@suryastoragedb.dfs.core.windows.net/Test.json")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC rates_df.show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC display(rates_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col

-- COMMAND ----------

-- MAGIC %python
-- MAGIC rates_selected_df = rates_df.select(col("Date").alias("Exchange_Date"),col("Australiandollar").alias("AUD"),col("NewZealanddollar").alias("NZD"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(rates_selected_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC rates_df_dup = rates_selected_df.dropDuplicates(["NZD"])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(rates_df_dup)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC rates_rencol_df = rates_df.withColumnRenamed("Date","Exchange_Date")\
-- MAGIC                             .withColumnRenamed("Australiandollar","AUD")\
-- MAGIC                             .withColumnRenamed("NewZealanddollar","NZD")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(rates_rencol_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(rates_selected_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.window import Window
-- MAGIC from pyspark.sql.functions import desc, rank

-- COMMAND ----------

-- MAGIC %python
-- MAGIC rates_ptn = Window.partitionBy("NZD").orderBy(desc("Exchange_Date"))
-- MAGIC rates_final = rates_selected_df.withColumn("rank", rank().over(rates_ptn))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(rates_final)

-- COMMAND ----------

CREATE DATABASE TEST;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC rates_df_dup.write.format("delta").mode("overwrite").saveAsTable("Test.ExchangeRates")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC rates_df_dup.count()

-- COMMAND ----------

Show databases;

-- COMMAND ----------

USE test;

-- COMMAND ----------

SELECT CURRENT_DATABASE();

-- COMMAND ----------

SELECT * FROM Test.ExchangeRates;

-- COMMAND ----------

SELECT COUNT(*) FROM Test.ExchangeRates;

-- COMMAND ----------

SELECT MEAN(NZD) AS AVG_EXCHANGE_RATE FROM Test.ExchangeRates;

-- COMMAND ----------

SELECT MIN(NZD) AS WORST_EXCHANGE_RATE,MAX(NZD) AS BEST_EXCHANGE_RATE,MEAN(NZD) AS AVG_EXCHANGE_RATE FROM Test.ExchangeRates;
