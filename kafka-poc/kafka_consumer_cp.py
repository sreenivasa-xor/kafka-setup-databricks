# Databricks notebook source
from pyspark.sql.types import StructType
from pyspark.sql.types import StructType, IntegerType, StringType
from pyspark.sql.functions import col, from_json

# COMMAND ----------

topic = dbutils.widgets.get("topic")

# COMMAND ----------

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", f"{topic}") \
  .option("startingOffsets", "earliest") \
  .option("failOnDataLoss", "false") \
  .load()

# COMMAND ----------

#{'name': 'Christian Moore', 'address': '3249 Joshua Bypass\nTuckerview, NH 46117', 'created_at': '2007'}
schema = StructType().add("name", StringType()).add("address", StringType()).add("created_at", StringType())
parsed_df = df.select( \
  col("key").cast("string"),
  from_json(col("value").cast("string"), schema).alias("parsed_value"))

# COMMAND ----------

final_df = parsed_df.select("parsed_value.*")

# COMMAND ----------

#final_df.writeStream.format("console").start().awaitTermination()
# final_df.writeStream \
#  .format("memory") \
#   .trigger(processingTime = "10 seconds") \
#   .queryName("input_console") \
#   .outputMode("append") \
#   .start()  

final_df.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/delta/events/_checkpoints/etl-from-json2") \
  .start("/delta/eventscp")

# display(parsed_df.select("parsed_value.*"))




# COMMAND ----------


