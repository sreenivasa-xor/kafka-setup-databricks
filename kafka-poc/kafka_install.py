# Databricks notebook source
# MAGIC %sh
# MAGIC lsb_release -a

# COMMAND ----------

# MAGIC %sh wget https://dlcdn.apache.org/kafka/3.1.0/kafka_2.12-3.1.0.tgz

# COMMAND ----------

# MAGIC %sh tar -xzf kafka_2.12-3.1.0.tgz

# COMMAND ----------

# MAGIC %sh cd kafka_2.12-3.1.0/
# MAGIC ls -lrt ./
# MAGIC bin/zookeeper-server-start.sh config/zookeeper.properties
