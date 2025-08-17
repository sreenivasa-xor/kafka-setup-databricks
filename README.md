# kafka-setup-databricks
Kafka with Databricks

Kafka Installation and Databricks Integration Guide
**1. Kafka Installation**
Execute the following steps to install and start Kafka in your environment:

**1. Check Linux version:**
   %sh
   lsb_release -a

**2. Download Kafka binaries:**
   %sh wget https://dlcdn.apache.org/kafka/3.9.0/kafka_2.12-3.9.0.tgz

**3. Extract tar file:**
   %sh tar -xzf kafka_2.12-3.9.0.tgz

**4. Start Zookeeper service:**
   %sh cd kafka_2.12-3.9.0/
   bin/zookeeper-server-start.sh config/zookeeper.properties

**5. Start Kafka broker service:**
   %sh cd kafka_2.12-3.9.0
   bin/kafka-server-start.sh config/server.properties


**2. Kafka Topic Creation**

To create topics in Kafka, execute the following commands:
   
**1. Create topic 'azdbadftopic':**
   %sh
   cd kafka_2.12-3.9.0
   bin/kafka-topics.sh --create --topic azdbadftopic --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1

**2. Create topic 'azdbadftopic1':**
   %sh
   cd kafka_2.12-3.9.0
   bin/kafka-topics.sh --create --topic azdbadftopic1 --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1

**3. List available topics:**
   %sh cd kafka_2.12-3.9.0
   bin/kafka-topics.sh --list --bootstrap-server localhost:9092

**3. Kafka Producer API**

**1. Install required libraries:**
   pip install Faker
   pip install kafka-python

**2. Generate fake data and publish to Kafka topic:**

   from faker import Faker
   from kafka import KafkaProducer
   import json, time

   fake = Faker()

   def get_registered_user():
       return {
           "name": fake.name(),
           "address": fake.address(),
           "created_at": fake.year()
       }

   def json_serializer(data):
       return json.dumps(data).encode("utf-8")

   producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                            value_serializer=json_serializer)

   if __name__ == "__main__":
       while True:
           registered_user = get_registered_user()
           print(registered_user)
           producer.send("azdbadftopic", registered_user)
           time.sleep(1)

**4. Kafka Consumer in Databricks**

Consume and process messages from Kafka using Databricks Structured Streaming:

   from pyspark.sql.types import StructType, StringType
   from pyspark.sql.functions import col, from_json

   topic = dbutils.widgets.get("topic")

   df = spark.readStream.format("kafka") \ 
        .option("kafka.bootstrap.servers", "localhost:9092") \ 
        .option("subscribe", f"{topic}") \ 
        .option("startingOffsets", "earliest") \ 
        .option("failOnDataLoss", "false") \ 
        .load()

   schema = StructType().add("name", StringType()).add("address", StringType()).add("created_at", StringType())

   parsed_df = df.select(col("key").cast("string"),
                         from_json(col("value").cast("string"), schema).alias("parsed_value"))

   final_df = parsed_df.select("parsed_value.*")

   final_df.writeStream \ 
           .format("delta") \ 
           .outputMode("append") \ 
           .option("checkpointLocation", "/delta/events/_checkpoints/etl-from-json2") \ 
           .start("/delta/eventscp")

