# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

drivers_df = spark.read.format('delta').load(f"{processed_folder_path}/drivers") \
.withColumnRenamed("number", "driver_number") \
.withColumnRenamed("name", "driver_name") \
.withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

constructors_df = spark.read.format('delta').load(f"{processed_folder_path}/constructors") \
.withColumnRenamed("name", "team") 

# COMMAND ----------

circuits_df = spark.read.format('delta').load(f"{processed_folder_path}/circuits") \
.withColumnRenamed("location", "circuit_location") 

# COMMAND ----------

races_df = spark.read.format('delta').load(f"{processed_folder_path}/races") \
.withColumnRenamed("name", "race_name") \
.withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

results_df = spark.read.format('delta').load(f"{processed_folder_path}/results") \
.filter(f"file_date = '{v_file_date}'") \
.withColumnRenamed("time", "race_time") \
.withColumnRenamed("race_id", "results_race_id") \
.withColumnRenamed("file_date", "results_file_date") 

# COMMAND ----------

races_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") 

# COMMAND ----------

race_results_df = results_df.join(races_circuits_df, results_df.results_race_id == races_circuits_df.race_id, "inner") \
                            .join(drivers_df, results_df.driver_id == drivers_df.driver_id, "inner") \
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id, "inner")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_results_df.select("race_id", "race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", 
                                  "driver_nationality", "team", "grid", "fastest_lap", "race_time", "points", "position","results_file_date") \
                        .withColumn("created_date", current_timestamp()) \
                        .withColumnRenamed("results_file_date", "file_date")

# COMMAND ----------

# final_df.write.mode("overwrite").format('parquet').saveAsTable('f1_presentation.race_results')
# overwrite_partition(final_df, 'f1_presentation', 'race_results', 'race_id')
merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_presentation', 'race_results', presentation_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.race_results where race_year = 2021;

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(*) from f1_presentation.race_results
# MAGIC group by race_id order by race_id desc;

# COMMAND ----------


