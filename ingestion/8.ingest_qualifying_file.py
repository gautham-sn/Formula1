# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(fields = [StructField("qualifyId", IntegerType(), False),
                                         StructField("raceId", IntegerType(), True),
                                         StructField("driverId", IntegerType(), True),
                                         StructField("constructorId", IntegerType(), True),
                                         StructField("number", IntegerType(), True),
                                         StructField("position", IntegerType(), True),
                                         StructField("q1", StringType(), True),
                                         StructField("q2", StringType(), True),
                                         StructField("q3", StringType(), True),
                                         ])

# COMMAND ----------

qualifying_df = spark.read\
.schema(qualifying_schema)\
.option("multiLine",True)\
.json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

qualifying_final_df = qualifying_df.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("driverId","driver_id")\
.withColumnRenamed("qualifyId","qualify_id")\
.withColumnRenamed("constructorId","constructor_id")\
.withColumn('data_source', lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

qualifying_ingestion_df = add_ingestion_date(qualifying_final_df)

# COMMAND ----------

# qualifying_ingestion_df.write.mode("overwrite").format('parquet').saveAsTable('f1_processed.qualifying')
# overwrite_partition(qualifying_ingestion_df, 'f1_processed', 'qualifying', 'race_id')
merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_data(qualifying_ingestion_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

display(spark.read.format('delta').load("/mnt/formula1dl15/processed/qualifying"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(*) from f1_processed.qualifying
# MAGIC group by race_id order by race_id desc;

# COMMAND ----------

dbutils.notebook.exit("Success")
