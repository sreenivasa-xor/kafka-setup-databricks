# Databricks notebook source
# MAGIC %sh 
# MAGIC cd kafka_2.12-3.9.0
# MAGIC bin/kafka-topics.sh --create --topic azdbadftopic --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 

# COMMAND ----------

# MAGIC %sh 
# MAGIC cd kafka_2.12-3.9.0
# MAGIC bin/kafka-topics.sh --create --topic azdbadftopic1 --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 

# COMMAND ----------

# MAGIC %sh cd kafka_2.12-3.9.0
# MAGIC bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# COMMAND ----------


