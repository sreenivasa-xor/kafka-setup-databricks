# Databricks notebook source
pip install Faker

# COMMAND ----------

pip install kafka-python

# COMMAND ----------

from faker import Faker
fake = Faker()

def get_registered_user():
    return {
        "name": fake.name(),
        "address": fake.address(),
        "created_at": fake.year()
    }


if __name__ == "__main__":
    print(get_registered_user())

# COMMAND ----------

from kafka import KafkaProducer
import json
import time


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=json_serializer)

if __name__ == "__main__":
    while 1 == 1:
        registered_user = get_registered_user()
        print(registered_user)
        producer.send("azdbadftopic1", registered_user)
        time.sleep(1)

# COMMAND ----------


